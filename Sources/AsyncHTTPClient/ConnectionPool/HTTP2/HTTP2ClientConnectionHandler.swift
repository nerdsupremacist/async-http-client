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
/// Re-entrancy rule: a stream is only mutated in place through `self.streams[id]?…`, and every call
/// out (to the request, or `context.write`, which can fire nio-http2 events synchronously) happens
/// after that access ended. After a call out, the stream is looked up again.
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

    private var streamsWithPendingReads: [HTTP2StreamID] = []
    private var readScratchIDs: [HTTP2StreamID] = []
    /// Set by a GOAWAY frame; streams above it that nio-http2 didn't close are failed at read complete.
    private var goAwayLastStreamID: HTTP2StreamID?
    private var streamsWithHeldWrites: [HTTP2StreamID] = []
    private var scratchIDs: [HTTP2StreamID] = []

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

        self.withBatch {
            self.streams[id] = HTTP2ClientStream(
                id: id,
                request: request,
                parentIsWritable: self.parentIsWritable,
                targetWindowSize: Self.targetWindowSize
            )
            request.willExecuteRequest(StreamExecutor(handler: self, streamID: id))

            if let action = self.streams[id]?.machine.startRequest(
                head: request.requestHead,
                metadata: request.requestFramingMetadata
            ) {
                self.run(action, streamID: id)
            }
            // The child channel's auto-read on activation.
            if let action = self.streams[id]?.machine.read() {
                self.run(action, streamID: id)
            }
        }
    }

    func cancelAllStreams() {
        self.eventLoop.assertInEventLoop()
        self.withBatch {
            self.forEachStream { id in
                if let action = self.streams[id]?.machine.requestCancelled() {
                    self.run(action, streamID: id)
                }
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

        let id = frame.streamID
        if case .goAway(let lastStreamID, _, _) = frame.payload {
            self.goAwayLastStreamID = lastStreamID
            return
        }
        guard id != .rootStream, self.streams[id] != nil else {
            return
        }

        switch frame.payload {
        case .headers(let headers):
            if headers.endStream {
                self.streams[id]!.inboundWindow.closed = true
                self.streams[id]!.remoteEnded = true
            }
        case .data(let data):
            self.streams[id]!.inboundWindow.bufferedFrameReceived(size: data.data.readableBytes)
            if data.endStream {
                self.streams[id]!.inboundWindow.closed = true
                self.streams[id]!.remoteEnded = true
            }
        case .alternativeService, .rstStream, .priority, .windowUpdate, .settings, .pushPromise, .ping, .goAway,
            .origin:
            // RST_STREAM arrives as a StreamClosedEvent with a reason.
            return
        }

        self.streams[id]!.pendingReads.append(frame.payload)
        if !self.streams[id]!.isListedForReads {
            self.streams[id]!.isListedForReads = true
            self.streamsWithPendingReads.append(id)
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.isReading = false
        // NIOHTTP2Handler.channelReadComplete already flushed everything written during the burst.
        self.needsFlush = false
        self.withBatch {
            swap(&self.readScratchIDs, &self.streamsWithPendingReads)
            var index = 0
            while index < self.readScratchIDs.count {
                let id = self.readScratchIDs[index]
                self.streams[id]?.isListedForReads = false
                self.tryToRead(id)
                index += 1
            }
            self.readScratchIDs.removeAll(keepingCapacity: true)

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
            self.streamClosed(event.streamID, reason: event.reason)

        case let event as NIOHTTP2WindowUpdatedEvent:
            guard let size = event.inboundWindowSize else { break }
            if event.streamID == .rootStream {
                if let increment = self.connectionWindow.newWindowSize(size) {
                    self.withBatch { self.writeWindowUpdate(.rootStream, increment: increment) }
                }
            } else if let increment = self.streams[event.streamID]?.inboundWindow.newWindowSize(size) {
                self.withBatch { self.writeWindowUpdate(event.streamID, increment: increment) }
            }

        case let event as NIOHTTP2BulkStreamWindowChangeEvent:
            self.withBatch {
                self.forEachStream { id in
                    if let increment = self.streams[id]?.inboundWindow.initialWindowSizeChanged(delta: event.delta) {
                        self.writeWindowUpdate(id, increment: increment)
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
            self.forEachStream(sorted: true) { id in
                if let newValue = self.streams[id]?.writability.parentWritabilityChanged(isWritable) {
                    self.writabilityChanged(id, to: newValue)
                }
            }
        }
        context.fireChannelWritabilityChanged()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        if let streamError = error as? NIOHTTP2Errors.StreamError {
            let id = streamError.streamID
            self.withBatch {
                if let action = self.streams[id]?.machine.errorHappened(streamError.baseError) {
                    self.run(action, streamID: id)
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
            self.forEachStream(sorted: true) { id in
                self.streams[id]?.closedByNIO = true
                self.forceDeliverPendingReads(id)
                if let action = self.streams[id]?.machine.channelInactive() {
                    self.run(action, streamID: id)
                }
                self.removeStream(id)
            }
        }
        context.fireChannelInactive()
    }

    fileprivate func writeRequestBodyPart0(
        _ data: IOData,
        streamID id: HTTP2StreamID,
        request: any HTTPExecutableRequest,
        promise: EventLoopPromise<Void>?
    ) {
        guard self.streams[id]?.request === request else {
            promise?.fail(HTTPClientError.requestStreamCancelled)
            return
        }
        self.withBatch {
            if let action = self.streams[id]?.machine.requestStreamPartReceived(data, promise: promise) {
                self.run(action, streamID: id)
            }
        }
    }

    fileprivate func finishRequestBodyStream0(
        trailers: HTTPHeaders?,
        streamID id: HTTP2StreamID,
        request: any HTTPExecutableRequest,
        promise: EventLoopPromise<Void>?
    ) {
        guard self.streams[id]?.request === request else { return }
        self.withBatch {
            if let action = self.streams[id]?.machine.requestStreamFinished(trailers: trailers, promise: promise) {
                self.run(action, streamID: id)
            }
        }
    }

    fileprivate func demandResponseBodyStream0(streamID id: HTTP2StreamID, request: any HTTPExecutableRequest) {
        guard self.streams[id]?.request === request else { return }
        self.withBatch {
            if let action = self.streams[id]?.machine.demandMoreResponseBodyParts() {
                self.run(action, streamID: id)
            }
        }
    }

    fileprivate func cancelRequest0(streamID id: HTTP2StreamID, request: any HTTPExecutableRequest) {
        guard self.streams[id]?.request === request else { return }
        self.withBatch {
            if let action = self.streams[id]?.machine.requestCancelled() {
                self.run(action, streamID: id)
            }
        }
    }

    private func run(_ action: HTTPRequestStateMachine.Action, streamID id: HTTP2StreamID) {
        guard let request = self.streams[id]?.request else {
            // The stream is gone (removed by a nested call out); only promises still need an answer.
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

        switch action {
        case .sendRequestHead(let head, let sendEnd):
            self.sendRequestHead(head, sendEnd: sendEnd, streamID: id)

        case .notifyRequestHeadSendSuccessfully(let resumeRequestBodyStream, _):
            request.requestHeadSent()
            if resumeRequestBodyStream, self.streams[id] != nil {
                request.resumeRequestBodyStream()
            }

        case .pauseRequestBodyStream:
            request.pauseRequestBodyStream()

        case .resumeRequestBodyStream:
            request.resumeRequestBodyStream()

        case .sendBodyPart(let data, let promise):
            self.sendBodyPart(data, promise: promise, streamID: id)

        case .sendRequestEnd(let trailers, let writePromise, _):
            // The final action is HTTP/1 semantics; an h2 stream ends after every request anyway.
            // A caller's promise may belong to another event loop, so the callback is Sendable.
            let promise = writePromise ?? self.eventLoop.makePromise(of: Void.self)
            promise.futureResult.whenSuccess {
                request.requestBodyStreamSent()
            }
            self.sendRequestEnd(trailers, promise: promise, streamID: id)

        case .read:
            self.read0(id)

        case .wait:
            break

        case .forwardResponseHead(let head, let pauseRequestBodyStream):
            request.receiveResponseHead(head)
            if pauseRequestBodyStream, self.streams[id] != nil {
                request.pauseRequestBodyStream()
            }

        case .forwardResponseBodyParts(let parts):
            request.receiveResponseBodyParts(parts)

        case .failRequest(let error, let finalAction):
            // Drop the stream first so nothing re-entered from `fail` sees it.
            self.dropStream(id, error: error)
            request.fail(error)
            if case .close(let promise) = finalAction {
                promise?.fail(error)
            }

        case .forwardResponseEnd(let finalAction, let finalParts, let trailers):
            if case .close = finalAction {
                // The response ended while the upload hasn't (an early >= 300 response): abandon it.
                self.dropStream(id, error: HTTPClientError.cancelled)
            }
            request.receiveResponseEnd(finalParts, trailers: trailers)

        case .failSendBodyPart(let error, let promise), .failSendStreamFinished(let error, let promise):
            promise?.fail(error)
        }
    }

    private func writabilityChanged(_ id: HTTP2StreamID, to isWritable: Bool) {
        if let action = self.streams[id]?.machine.writabilityChanged(writable: isWritable) {
            self.run(action, streamID: id)
        }
    }

    private func sendRequestHead(_ head: HTTPRequestHead, sendEnd: Bool, streamID id: HTTP2StreamID) {
        let headers: HPACKHeaders
        do {
            headers = try HPACKHeaders(http2ClientRequestHead: head)
        } catch {
            if let action = self.streams[id]?.machine.errorHappened(error) {
                self.run(action, streamID: id)
            }
            return
        }

        let payload = HTTP2Frame.FramePayload.headers(.init(headers: headers, endStream: sendEnd))
        if sendEnd {
            self.streams[id]?.headersWritten = true
            self.streams[id]?.localEnded = true
            self.write(HTTP2Frame(streamID: id, payload: payload), promise: nil)
        } else {
            self.hold(payload, promise: nil, streamID: id)
        }

        if let action = self.streams[id]?.machine.headSent() {
            self.run(action, streamID: id)
        }
    }

    private func sendBodyPart(_ data: IOData, promise: EventLoopPromise<Void>?, streamID id: HTTP2StreamID) {
        guard self.streams[id] != nil else {
            promise?.fail(HTTPClientError.requestStreamCancelled)
            return
        }
        // The previous part goes out now; this one is held in case the end follows.
        self.releaseHeldWrite(id)
        let size = data.readableBytes
        self.hold(.data(.init(data: data)), promise: promise, streamID: id)
        if let newValue = self.streams[id]?.writability.bytesQueued(size) {
            self.writabilityChanged(id, to: newValue)
        }
    }

    private func sendRequestEnd(_ trailers: HTTPHeaders?, promise: EventLoopPromise<Void>, streamID id: HTTP2StreamID) {
        guard self.streams[id] != nil else {
            promise.fail(HTTPClientError.requestStreamCancelled)
            return
        }
        let held = self.streams[id]!.heldWrite.take()
        self.streams[id]!.localEnded = true
        self.streams[id]!.headersWritten = true

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

    private func hold(_ payload: HTTP2Frame.FramePayload, promise: EventLoopPromise<Void>?, streamID id: HTTP2StreamID)
    {
        self.streams[id]?.heldWrite = .init(payload: payload, promise: promise)
        if self.streams[id]?.isListedForHeldWrites == false {
            self.streams[id]?.isListedForHeldWrites = true
            self.streamsWithHeldWrites.append(id)
        }
    }

    /// Writes the held frame of a stream that isn't ending yet. DATA gets a promise so its bytes
    /// count against the watermarks until they're written, like the legacy child channel.
    private func releaseHeldWrite(_ id: HTTP2StreamID) {
        guard let held = self.streams[id]?.heldWrite.take() else { return }
        switch held.payload {
        case .data(let data):
            let size = data.data.readableBytes
            // Our own promise, on our loop: a caller's may belong to another event loop.
            let promise = self.eventLoop.makePromise(of: Void.self)
            if let callerPromise = held.promise {
                promise.futureResult.cascade(to: callerPromise)
            }
            promise.futureResult.assumeIsolated().whenComplete { _ in
                if let newValue = self.streams[id]?.writability.bytesWritten(size) {
                    self.withBatch { self.writabilityChanged(id, to: newValue) }
                }
            }
            self.write(HTTP2Frame(streamID: id, payload: held.payload), promise: promise)
        default:
            self.streams[id]?.headersWritten = true
            self.write(HTTP2Frame(streamID: id, payload: held.payload), promise: held.promise)
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
                    let id = self.streamsWithHeldWrites[index]
                    self.streams[id]?.isListedForHeldWrites = false
                    self.releaseHeldWrite(id)
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

    private func read0(_ id: HTTP2StreamID) {
        guard let stream = self.streams[id], !stream.unsatisfiedRead else { return }
        self.streams[id]!.unsatisfiedRead = true
        if !stream.pendingReads.isEmpty {
            self.tryToRead(id)
        }
    }

    private func tryToRead(_ id: HTTP2StreamID) {
        guard let stream = self.streams[id], stream.unsatisfiedRead, !stream.pendingReads.isEmpty else {
            return
        }
        self.streams[id]!.unsatisfiedRead = false
        self.deliverPendingReads(id)
        // Auto-read, like the child channel after delivering.
        if let action = self.streams[id]?.machine.read() {
            self.run(action, streamID: id)
        }
    }

    private func forceDeliverPendingReads(_ id: HTTP2StreamID) {
        guard let stream = self.streams[id], !stream.pendingReads.isEmpty else { return }
        self.streams[id]!.unsatisfiedRead = false
        self.deliverPendingReads(id)
    }

    /// Delivers every queued frame, then the stream's read complete, like
    /// `HTTP2StreamChannel.deliverPendingReads`.
    private func deliverPendingReads(_ id: HTTP2StreamID) {
        while let payload = self.streams[id]?.pendingReads.popFirst() {
            let parts: (first: HTTPClientResponsePart?, second: HTTPClientResponsePart?)
            do {
                parts = try self.streams[id]!.decoder.decode(payload)
            } catch {
                if let action = self.streams[id]?.machine.errorHappened(error) {
                    self.run(action, streamID: id)
                }
                return
            }

            if let first = parts.first, let action = self.streams[id]?.machine.channelRead(first) {
                self.run(action, streamID: id)
            }
            if let second = parts.second, let action = self.streams[id]?.machine.channelRead(second) {
                self.run(action, streamID: id)
            }

            if case .data(let data) = payload,
                let increment = self.streams[id]?.inboundWindow.bufferedFrameEmitted(size: data.data.readableBytes)
            {
                self.writeWindowUpdate(id, increment: increment)
            }
        }
        if let action = self.streams[id]?.machine.channelReadComplete() {
            self.run(action, streamID: id)
        }
    }

    private func streamClosed(_ id: HTTP2StreamID, reason: HTTP2ErrorCode?) {
        guard self.streams[id] != nil else { return }
        self.withBatch {
            self.streams[id]?.closedByNIO = true
            self.streams[id]?.inboundWindow.closed = true
            self.forceDeliverPendingReads(id)

            let action: HTTPRequestStateMachine.Action?
            if let reason {
                action = self.streams[id]?.machine.errorHappened(
                    NIOHTTP2Errors.streamClosed(streamID: id, errorCode: reason)
                )
            } else {
                action = self.streams[id]?.machine.channelInactive()
            }
            if let action {
                self.run(action, streamID: id)
            }
            self.removeStream(id)
        }
    }

    /// Removes a stream that is being given up, resetting it if it's still open on the wire.
    private func dropStream(_ id: HTTP2StreamID, error: any Error) {
        guard let stream = self.removeStream(id) else { return }
        if stream.needsReset {
            self.write(HTTP2Frame(streamID: id, payload: .rstStream(.cancel)), promise: nil)
        }
    }

    /// Streams above a GOAWAY's last stream ID that nio-http2 didn't close itself: their HEADERS
    /// were still buffered (over MAX_CONCURRENT_STREAMS) and will never be sent.
    private func failStreams(above lastStreamID: HTTP2StreamID) {
        self.forEachStream(sorted: true) { id in
            guard id > lastStreamID else { return }
            // Not marked closed: the RST that `failRequest` sends drops the buffered HEADERS.
            let error = NIOHTTP2Errors.streamClosed(streamID: id, errorCode: .refusedStream)
            if let action = self.streams[id]?.machine.errorHappened(error) {
                self.run(action, streamID: id)
            }
            self.removeStream(id)
        }
    }

    @discardableResult
    private func removeStream(_ id: HTTP2StreamID) -> HTTP2ClientStream? {
        guard let stream = self.streams.removeValue(forKey: id) else { return nil }
        stream.heldWrite?.promise?.fail(HTTPClientError.requestStreamCancelled)
        return stream
    }

    private func forEachStream(sorted: Bool = false, _ body: (HTTP2StreamID) -> Void) {
        var ids = self.scratchIDs
        self.scratchIDs = []
        ids.removeAll(keepingCapacity: true)
        ids.append(contentsOf: self.streams.keys)
        if sorted {
            ids.sort()
        }
        for id in ids {
            body(id)
        }
        ids.removeAll(keepingCapacity: true)
        self.scratchIDs = ids
    }
}

@available(*, unavailable)
extension HTTP2ClientConnectionHandler: Sendable {}

extension HTTP2ClientConnectionHandler {
    /// A request's handle on its stream. Any thread: runs inline on the event loop, hops otherwise.
    /// Stream IDs are never reused on a connection, so a call for a finished stream misses the
    /// table and does nothing.
    struct StreamExecutor: HTTPRequestExecutor {
        private let handler: NIOLoopBound<HTTP2ClientConnectionHandler>
        private let streamID: HTTP2StreamID

        init(handler: HTTP2ClientConnectionHandler, streamID: HTTP2StreamID) {
            self.handler = NIOLoopBound(handler, eventLoop: handler.eventLoop)
            self.streamID = streamID
        }

        func writeRequestBodyPart(_ data: IOData, request: any HTTPExecutableRequest, promise: EventLoopPromise<Void>?)
        {
            let streamID = self.streamID
            self.onLoop {
                $0.writeRequestBodyPart0(data, streamID: streamID, request: request, promise: promise)
            }
        }

        func finishRequestBodyStream(
            trailers: HTTPHeaders?,
            request: any HTTPExecutableRequest,
            promise: EventLoopPromise<Void>?
        ) {
            let streamID = self.streamID
            self.onLoop {
                $0.finishRequestBodyStream0(trailers: trailers, streamID: streamID, request: request, promise: promise)
            }
        }

        func demandResponseBodyStream(_ request: any HTTPExecutableRequest) {
            let streamID = self.streamID
            self.onLoop { $0.demandResponseBodyStream0(streamID: streamID, request: request) }
        }

        func cancelRequest(_ request: any HTTPExecutableRequest) {
            let streamID = self.streamID
            self.onLoop { $0.cancelRequest0(streamID: streamID, request: request) }
        }

        private func onLoop(_ body: @escaping @Sendable (HTTP2ClientConnectionHandler) -> Void) {
            self.handler.execute(body)
        }
    }
}
