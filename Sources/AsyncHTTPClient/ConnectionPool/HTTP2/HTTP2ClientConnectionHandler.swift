//===----------------------------------------------------------------------===//
//
// This source file is part of the AsyncHTTPClient open source project
//
// Copyright (c) 2026 Apple Inc. and the AsyncHTTPClient project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of AsyncHTTPClient project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOCore
import NIOHPACK
import NIOHTTP1
import NIOHTTP2

/// Runs every request of one HTTP/2 client connection as a stream on the connection channel, with
/// no child channel per stream.
///
/// Sits last in the pipeline, after `HTTP2IdleHandler`, behind a `NIOHTTP2Handler` that has no
/// multiplexer. It owns stream IDs, all inbound WINDOW_UPDATEs and per-stream write backpressure.
/// Event loop only.
///
/// Re-entrancy rule: a call out (to the request, or `context.write`, which can fire nio-http2 events
/// synchronously) may remove the stream, so code after a call out checks `stream.isActive`.
final class HTTP2ClientConnectionHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP2Frame
    typealias OutboundOut = HTTP2Frame

    static let targetWindowSize: Int32 = 8 * 1024 * 1024
    /// Client stream IDs are odd, from 1 to `Int32.max`.
    static let maximumStreamCount = Int(Int32.max / 2)

    let eventLoop: any EventLoop

    private var context: ChannelHandlerContext?
    private var streams: [HTTP2StreamID: HTTP2ClientStream] = [:]
    private var nextStreamID: Int32 = 1
    private var connectionWindow = HTTP2InboundWindow(targetSize: HTTP2ClientConnectionHandler.targetWindowSize)

    private var streamsWithPendingReads: [HTTP2ClientStream] = []
    private var readScratch: [HTTP2ClientStream] = []
    private var streamsWithHeldWrites: [HTTP2ClientStream] = []
    private var scratch: [HTTP2ClientStream] = []
    /// Set by a GOAWAY frame; streams above it that nio-http2 didn't close are failed at read complete.
    private var goAwayLastStreamID: HTTP2StreamID?

    private var batchDepth = 0
    private var needsFlush = false
    /// Inside the parent's read burst; `NIOHTTP2Handler.channelReadComplete` flushes those writes.
    private var isReading = false
    private var parentIsWritable = true
    private var isInactive = false

    init(eventLoop: any EventLoop) {
        self.eventLoop = eventLoop
    }

    var openStreamCount: Int {
        self.streams.count
    }

    func executeRequest(_ request: any HTTPExecutableRequest) {
        self.eventLoop.assertInEventLoop()
        guard self.context != nil, !self.isInactive, self.nextStreamID > 0 else {
            request.fail(HTTPClientError.cancelled)
            return
        }

        let id = HTTP2StreamID(Int(self.nextStreamID))
        // Wraps to negative after Int32.max, which the guard above catches.
        self.nextStreamID &+= 2

        let stream = HTTP2ClientStream(
            id: id,
            request: request,
            handler: self,
            parentIsWritable: self.parentIsWritable,
            targetWindowSize: Self.targetWindowSize
        )
        self.streams[id] = stream

        self.withBatch {
            request.willExecuteRequest(StreamExecutor(stream))
            guard stream.isActive else { return }
            let action = stream.machine.startRequest(
                head: request.requestHead,
                metadata: request.requestFramingMetadata
            )
            self.run(action, stream)
            // The child channel's auto-read on activation.
            guard stream.isActive else { return }
            self.run(stream.machine.read(), stream)
        }
    }

    func cancelAllStreams() {
        self.eventLoop.assertInEventLoop()
        self.withBatch {
            self.forEachStream { stream in
                self.run(stream.machine.requestCancelled(), stream)
            }
        }
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
        self.parentIsWritable = context.channel.isWritable
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.context = nil
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = self.unwrapInboundIn(data)
        self.isReading = true

        if case .goAway(let lastStreamID, _, _) = frame.payload {
            self.goAwayLastStreamID = lastStreamID
            return
        }
        guard frame.streamID != .rootStream, let stream = self.streams[frame.streamID] else {
            return
        }

        switch frame.payload {
        case .headers(let headers):
            if headers.endStream {
                stream.inboundWindow.closed = true
                stream.remoteEnded = true
            }
        case .data(let data):
            stream.inboundWindow.bufferedFrameReceived(size: data.data.readableBytes)
            if data.endStream {
                stream.inboundWindow.closed = true
                stream.remoteEnded = true
            }
        case .alternativeService, .rstStream, .priority, .windowUpdate, .settings, .pushPromise, .ping, .goAway,
            .origin:
            // RST_STREAM arrives as a StreamClosedEvent with a reason.
            return
        }

        stream.pendingReads.append(frame.payload)
        if !stream.isListedForReads {
            stream.isListedForReads = true
            self.streamsWithPendingReads.append(stream)
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.isReading = false
        // NIOHTTP2Handler.channelReadComplete already flushed everything written during the burst.
        self.needsFlush = false
        self.withBatch {
            swap(&self.readScratch, &self.streamsWithPendingReads)
            var index = 0
            while index < self.readScratch.count {
                let stream = self.readScratch[index]
                stream.isListedForReads = false
                self.tryToRead(stream)
                index += 1
            }
            self.readScratch.removeAll(keepingCapacity: true)

            if let lastStreamID = self.goAwayLastStreamID {
                self.goAwayLastStreamID = nil
                self.failStreams(above: lastStreamID)
            }
        }
        context.fireChannelReadComplete()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case let event as StreamClosedEvent:
            if let stream = self.streams[event.streamID] {
                self.streamClosed(stream, reason: event.reason)
            }

        case let event as NIOHTTP2WindowUpdatedEvent:
            guard let size = event.inboundWindowSize else { break }
            if event.streamID == .rootStream {
                if let increment = self.connectionWindow.newWindowSize(size) {
                    self.withBatch { self.writeWindowUpdate(.rootStream, increment: increment) }
                }
            } else if let stream = self.streams[event.streamID],
                let increment = stream.inboundWindow.newWindowSize(size)
            {
                self.withBatch { self.writeWindowUpdate(stream.id, increment: increment) }
            }

        case let event as NIOHTTP2BulkStreamWindowChangeEvent:
            self.withBatch {
                self.forEachStream { stream in
                    if let increment = stream.inboundWindow.initialWindowSizeChanged(delta: event.delta) {
                        self.writeWindowUpdate(stream.id, increment: increment)
                    }
                }
            }

        default:
            break
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        let isWritable = context.channel.isWritable
        self.parentIsWritable = isWritable
        self.withBatch {
            // Ascending IDs, so streams waiting to send HEADERS send them in ID order.
            self.forEachStream(sorted: true) { stream in
                if let newValue = stream.writability.parentWritabilityChanged(isWritable) {
                    self.writabilityChanged(stream, to: newValue)
                }
            }
        }
        context.fireChannelWritabilityChanged()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        if let streamError = error as? NIOHTTP2Errors.StreamError {
            if let stream = self.streams[streamError.streamID] {
                self.withBatch {
                    self.run(stream.machine.errorHappened(streamError.baseError), stream)
                }
            }
            return
        }
        context.fireErrorCaught(error)
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.isInactive = true
        self.isReading = false
        self.withBatch {
            self.forEachStream(sorted: true) { stream in
                stream.closedByNIO = true
                self.forceDeliverPendingReads(stream)
                if stream.isActive {
                    self.run(stream.machine.channelInactive(), stream)
                }
                self.removeStream(stream)
            }
        }
        context.fireChannelInactive()
    }

    // Executor entry points (event loop).

    fileprivate func writeRequestBodyPart0(
        _ data: IOData,
        stream: HTTP2ClientStream,
        request: any HTTPExecutableRequest,
        promise: EventLoopPromise<Void>?
    ) {
        guard stream.isActive, stream.request === request else {
            promise?.fail(HTTPClientError.requestStreamCancelled)
            return
        }
        self.withBatch {
            self.run(stream.machine.requestStreamPartReceived(data, promise: promise), stream)
        }
    }

    fileprivate func finishRequestBodyStream0(
        trailers: HTTPHeaders?,
        stream: HTTP2ClientStream,
        request: any HTTPExecutableRequest,
        promise: EventLoopPromise<Void>?
    ) {
        guard stream.isActive, stream.request === request else { return }
        self.withBatch {
            self.run(stream.machine.requestStreamFinished(trailers: trailers, promise: promise), stream)
        }
    }

    fileprivate func demandResponseBodyStream0(stream: HTTP2ClientStream, request: any HTTPExecutableRequest) {
        guard stream.isActive, stream.request === request else { return }
        self.withBatch {
            self.run(stream.machine.demandMoreResponseBodyParts(), stream)
        }
    }

    fileprivate func cancelRequest0(stream: HTTP2ClientStream, request: any HTTPExecutableRequest) {
        guard stream.isActive, stream.request === request else { return }
        self.withBatch {
            self.run(stream.machine.requestCancelled(), stream)
        }
    }

    // Running state machine actions.

    private func run(_ action: HTTPRequestStateMachine.Action, _ stream: HTTP2ClientStream) {
        guard stream.isActive else {
            // Removed by a nested call out; only promises still need an answer.
            switch action {
            case .sendBodyPart(_, let promise), .sendRequestEnd(_, let promise, _):
                promise?.fail(HTTPClientError.requestStreamCancelled)
            case .failSendBodyPart(let error, let promise), .failSendStreamFinished(let error, let promise):
                promise?.fail(error)
            case .failRequest(let error, .close(let promise)):
                promise?.fail(error)
            default:
                break
            }
            return
        }
        let request = stream.request

        switch action {
        case .sendRequestHead(let head, let sendEnd):
            self.sendRequestHead(head, sendEnd: sendEnd, stream)

        case .notifyRequestHeadSendSuccessfully(let resumeRequestBodyStream, _):
            request.requestHeadSent()
            if resumeRequestBodyStream, stream.isActive {
                request.resumeRequestBodyStream()
            }

        case .pauseRequestBodyStream:
            request.pauseRequestBodyStream()

        case .resumeRequestBodyStream:
            request.resumeRequestBodyStream()

        case .sendBodyPart(let data, let promise):
            self.sendBodyPart(data, promise: promise, stream)

        case .sendRequestEnd(let trailers, let writePromise, _):
            // The final action is HTTP/1 semantics; an h2 stream ends after every request anyway.
            // A caller's promise may belong to another event loop, so the callback is Sendable.
            let promise = writePromise ?? self.eventLoop.makePromise(of: Void.self)
            promise.futureResult.whenSuccess {
                request.requestBodyStreamSent()
            }
            self.sendRequestEnd(trailers, promise: promise, stream)

        case .read:
            self.read0(stream)

        case .wait:
            break

        case .forwardResponseHead(let head, let pauseRequestBodyStream):
            request.receiveResponseHead(head)
            if pauseRequestBodyStream, stream.isActive {
                request.pauseRequestBodyStream()
            }

        case .forwardResponseBodyParts(let parts):
            request.receiveResponseBodyParts(parts)

        case .failRequest(let error, let finalAction):
            // Drop the stream first so nothing re-entered from `fail` sees it.
            self.dropStream(stream)
            request.fail(error)
            if case .close(let promise) = finalAction {
                promise?.fail(error)
            }

        case .forwardResponseEnd(let finalAction, let finalParts, let trailers):
            if case .close = finalAction {
                // The response ended while the upload hasn't (an early >= 300 response): abandon it.
                self.dropStream(stream)
            }
            request.receiveResponseEnd(finalParts, trailers: trailers)

        case .failSendBodyPart(let error, let promise), .failSendStreamFinished(let error, let promise):
            promise?.fail(error)
        }
    }

    private func writabilityChanged(_ stream: HTTP2ClientStream, to isWritable: Bool) {
        guard stream.isActive else { return }
        self.run(stream.machine.writabilityChanged(writable: isWritable), stream)
    }

    // Writing.

    private func sendRequestHead(_ head: HTTPRequestHead, sendEnd: Bool, _ stream: HTTP2ClientStream) {
        let headers: HPACKHeaders
        do {
            headers = try HPACKHeaders(http2ClientRequestHead: head)
        } catch {
            self.run(stream.machine.errorHappened(error), stream)
            return
        }

        let payload = HTTP2Frame.FramePayload.headers(.init(headers: headers, endStream: sendEnd))
        if sendEnd {
            stream.headersWritten = true
            stream.localEnded = true
            self.write(HTTP2Frame(streamID: stream.id, payload: payload), promise: nil)
        } else {
            self.hold(payload, promise: nil, stream)
        }

        guard stream.isActive else { return }
        self.run(stream.machine.headSent(), stream)
    }

    private func sendBodyPart(_ data: IOData, promise: EventLoopPromise<Void>?, _ stream: HTTP2ClientStream) {
        // The previous part goes out now; this one is held in case the end follows.
        self.releaseHeldWrite(stream)
        guard stream.isActive else {
            promise?.fail(HTTPClientError.requestStreamCancelled)
            return
        }
        let size = data.readableBytes
        self.hold(.data(.init(data: data)), promise: promise, stream)
        if let newValue = stream.writability.bytesQueued(size) {
            self.writabilityChanged(stream, to: newValue)
        }
    }

    private func sendRequestEnd(_ trailers: HTTPHeaders?, promise: EventLoopPromise<Void>, _ stream: HTTP2ClientStream)
    {
        let held = stream.heldWrite.take()
        stream.localEnded = true
        stream.headersWritten = true
        let id = stream.id

        func trailersPayload(_ trailers: HTTPHeaders) -> HTTP2Frame.FramePayload {
            .headers(.init(headers: HPACKHeaders(http2ClientTrailers: trailers), endStream: true))
        }

        switch (held?.payload, trailers) {
        case (.none, .none):
            self.write(
                HTTP2Frame(streamID: id, payload: .data(.init(data: .byteBuffer(ByteBuffer()), endStream: true))),
                promise: promise
            )

        case (.none, .some(let trailers)):
            self.write(HTTP2Frame(streamID: id, payload: trailersPayload(trailers)), promise: promise)

        case (.data(var data), .none):
            data.endStream = true
            var heldPromise = held!.promise
            heldPromise.setOrCascade(to: promise)
            self.write(HTTP2Frame(streamID: id, payload: .data(data)), promise: heldPromise)

        case (.headers(var headers), .none):
            headers.endStream = true
            var heldPromise = held!.promise
            heldPromise.setOrCascade(to: promise)
            self.write(HTTP2Frame(streamID: id, payload: .headers(headers)), promise: heldPromise)

        case (.some(let payload), .some(let trailers)):
            self.write(HTTP2Frame(streamID: id, payload: payload), promise: held!.promise)
            self.write(HTTP2Frame(streamID: id, payload: trailersPayload(trailers)), promise: promise)

        case (.some, .none):
            preconditionFailure("Only HEADERS and DATA frames are held")
        }
    }

    private func hold(_ payload: HTTP2Frame.FramePayload, promise: EventLoopPromise<Void>?, _ stream: HTTP2ClientStream)
    {
        stream.heldWrite = .init(payload: payload, promise: promise)
        if !stream.isListedForHeldWrites {
            stream.isListedForHeldWrites = true
            self.streamsWithHeldWrites.append(stream)
        }
    }

    /// Writes the held frame of a stream that isn't ending yet. DATA gets a promise so its bytes
    /// count against the watermarks until they're written, like the legacy child channel.
    private func releaseHeldWrite(_ stream: HTTP2ClientStream) {
        guard let held = stream.heldWrite.take() else { return }
        switch held.payload {
        case .data(let data):
            let size = data.data.readableBytes
            // Our own promise, on our loop: a caller's may belong to another event loop.
            let promise = self.eventLoop.makePromise(of: Void.self)
            if let callerPromise = held.promise {
                promise.futureResult.cascade(to: callerPromise)
            }
            promise.futureResult.assumeIsolated().whenComplete { _ in
                guard stream.isActive, let newValue = stream.writability.bytesWritten(size) else { return }
                self.withBatch { self.writabilityChanged(stream, to: newValue) }
            }
            self.write(HTTP2Frame(streamID: stream.id, payload: held.payload), promise: promise)
        default:
            stream.headersWritten = true
            self.write(HTTP2Frame(streamID: stream.id, payload: held.payload), promise: held.promise)
        }
    }

    private func writeWindowUpdate(_ id: HTTP2StreamID, increment: Int) {
        self.write(HTTP2Frame(streamID: id, payload: .windowUpdate(windowSizeIncrement: increment)), promise: nil)
    }

    private func write(_ frame: HTTP2Frame, promise: EventLoopPromise<Void>?) {
        guard let context = self.context else {
            promise?.fail(HTTPClientError.cancelled)
            return
        }
        self.needsFlush = true
        context.write(self.wrapOutboundOut(frame), promise: promise)
    }

    // Batching and flushing.

    /// Runs `body`, then (outermost call only) releases held frames in stream order and flushes
    /// once. Inside a parent read burst the flush is left to `NIOHTTP2Handler.channelReadComplete`.
    private func withBatch(_ body: () -> Void) {
        self.batchDepth += 1
        body()
        if self.batchDepth == 1 {
            // A flush can complete write promises synchronously, and their callbacks can write
            // again, so loop until nothing is held or unflushed.
            while true {
                var index = 0
                while index < self.streamsWithHeldWrites.count {
                    let stream = self.streamsWithHeldWrites[index]
                    stream.isListedForHeldWrites = false
                    if stream.isActive {
                        self.releaseHeldWrite(stream)
                    }
                    index += 1
                }
                self.streamsWithHeldWrites.removeAll(keepingCapacity: true)

                guard self.needsFlush, !self.isReading, let context = self.context else { break }
                self.needsFlush = false
                context.flush()
                if self.streamsWithHeldWrites.isEmpty && !self.needsFlush {
                    break
                }
            }
        }
        self.batchDepth -= 1
    }

    // Reading.

    private func read0(_ stream: HTTP2ClientStream) {
        guard !stream.unsatisfiedRead else { return }
        stream.unsatisfiedRead = true
        if !stream.pendingReads.isEmpty {
            self.tryToRead(stream)
        }
    }

    private func tryToRead(_ stream: HTTP2ClientStream) {
        guard stream.isActive, stream.unsatisfiedRead, !stream.pendingReads.isEmpty else {
            return
        }
        stream.unsatisfiedRead = false
        self.deliverPendingReads(stream)
        // Auto-read, like the child channel after delivering.
        guard stream.isActive else { return }
        self.run(stream.machine.read(), stream)
    }

    private func forceDeliverPendingReads(_ stream: HTTP2ClientStream) {
        guard stream.isActive, !stream.pendingReads.isEmpty else { return }
        stream.unsatisfiedRead = false
        self.deliverPendingReads(stream)
    }

    /// Delivers every queued frame, then the stream's read complete, like
    /// `HTTP2StreamChannel.deliverPendingReads`.
    private func deliverPendingReads(_ stream: HTTP2ClientStream) {
        while stream.isActive, let payload = stream.pendingReads.popFirst() {
            let parts: (first: HTTPClientResponsePart?, second: HTTPClientResponsePart?)
            do {
                parts = try stream.decoder.decode(payload)
            } catch {
                self.run(stream.machine.errorHappened(error), stream)
                return
            }

            if let first = parts.first {
                self.run(stream.machine.channelRead(first), stream)
            }
            if let second = parts.second, stream.isActive {
                self.run(stream.machine.channelRead(second), stream)
            }

            if case .data(let data) = payload, stream.isActive,
                let increment = stream.inboundWindow.bufferedFrameEmitted(size: data.data.readableBytes)
            {
                self.writeWindowUpdate(stream.id, increment: increment)
            }
        }
        guard stream.isActive else { return }
        self.run(stream.machine.channelReadComplete(), stream)
    }

    // Closing streams.

    private func streamClosed(_ stream: HTTP2ClientStream, reason: HTTP2ErrorCode?) {
        self.withBatch {
            stream.closedByNIO = true
            stream.inboundWindow.closed = true
            self.forceDeliverPendingReads(stream)
            guard stream.isActive else { return }

            if let reason {
                let error = NIOHTTP2Errors.streamClosed(streamID: stream.id, errorCode: reason)
                self.run(stream.machine.errorHappened(error), stream)
            } else {
                self.run(stream.machine.channelInactive(), stream)
            }
            self.removeStream(stream)
        }
    }

    /// Streams above a GOAWAY's last stream ID that nio-http2 didn't close itself: their HEADERS
    /// were still buffered (over MAX_CONCURRENT_STREAMS) and will never be sent.
    private func failStreams(above lastStreamID: HTTP2StreamID) {
        self.forEachStream(sorted: true) { stream in
            guard stream.id > lastStreamID else { return }
            // Not marked closed: the RST that `failRequest` sends drops the buffered HEADERS.
            let error = NIOHTTP2Errors.streamClosed(streamID: stream.id, errorCode: .refusedStream)
            self.run(stream.machine.errorHappened(error), stream)
            self.removeStream(stream)
        }
    }

    /// Removes a stream that is being given up, resetting it if it's still open on the wire.
    private func dropStream(_ stream: HTTP2ClientStream) {
        guard stream.isActive else { return }
        self.removeStream(stream)
        if stream.needsReset {
            self.write(HTTP2Frame(streamID: stream.id, payload: .rstStream(.cancel)), promise: nil)
        }
    }

    private func removeStream(_ stream: HTTP2ClientStream) {
        guard stream.isActive else { return }
        stream.isActive = false
        self.streams.removeValue(forKey: stream.id)
        stream.heldWrite.take()?.promise?.fail(HTTPClientError.requestStreamCancelled)
    }

    private func forEachStream(sorted: Bool = false, _ body: (HTTP2ClientStream) -> Void) {
        var streams = self.scratch
        self.scratch = []
        streams.removeAll(keepingCapacity: true)
        streams.append(contentsOf: self.streams.values)
        if sorted {
            streams.sort { $0.id < $1.id }
        }
        for stream in streams where stream.isActive {
            body(stream)
        }
        streams.removeAll(keepingCapacity: true)
        self.scratch = streams
    }
}

@available(*, unavailable)
extension HTTP2ClientConnectionHandler: Sendable {}

extension HTTP2ClientConnectionHandler {
    /// A request's handle on its stream: 3 words (the loop and the stream), so it fits an
    /// existential's inline buffer. Any thread: calls straight through on the event loop, hops
    /// otherwise. A call for a finished stream sees `isActive == false` and does nothing.
    struct StreamExecutor: HTTPRequestExecutor {
        private let stream: NIOLoopBound<HTTP2ClientStream>

        init(_ stream: HTTP2ClientStream) {
            self.stream = NIOLoopBound(stream, eventLoop: stream.handler.eventLoop)
        }

        func writeRequestBodyPart(_ data: IOData, request: any HTTPExecutableRequest, promise: EventLoopPromise<Void>?)
        {
            if self.stream.eventLoop.inEventLoop {
                let stream = self.stream.value
                stream.handler.writeRequestBodyPart0(data, stream: stream, request: request, promise: promise)
            } else {
                let stream = self.stream
                stream.eventLoop.execute {
                    stream.value.handler.writeRequestBodyPart0(
                        data,
                        stream: stream.value,
                        request: request,
                        promise: promise
                    )
                }
            }
        }

        func finishRequestBodyStream(
            trailers: HTTPHeaders?,
            request: any HTTPExecutableRequest,
            promise: EventLoopPromise<Void>?
        ) {
            if self.stream.eventLoop.inEventLoop {
                let stream = self.stream.value
                stream.handler.finishRequestBodyStream0(
                    trailers: trailers,
                    stream: stream,
                    request: request,
                    promise: promise
                )
            } else {
                let stream = self.stream
                stream.eventLoop.execute {
                    stream.value.handler.finishRequestBodyStream0(
                        trailers: trailers,
                        stream: stream.value,
                        request: request,
                        promise: promise
                    )
                }
            }
        }

        func demandResponseBodyStream(_ request: any HTTPExecutableRequest) {
            if self.stream.eventLoop.inEventLoop {
                let stream = self.stream.value
                stream.handler.demandResponseBodyStream0(stream: stream, request: request)
            } else {
                let stream = self.stream
                stream.eventLoop.execute {
                    stream.value.handler.demandResponseBodyStream0(stream: stream.value, request: request)
                }
            }
        }

        func cancelRequest(_ request: any HTTPExecutableRequest) {
            if self.stream.eventLoop.inEventLoop {
                let stream = self.stream.value
                stream.handler.cancelRequest0(stream: stream, request: request)
            } else {
                let stream = self.stream
                stream.eventLoop.execute {
                    stream.value.handler.cancelRequest0(stream: stream.value, request: request)
                }
            }
        }
    }
}
