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

/// nio-http2's consumer for an AHC connection: hands each visit to the driver.
struct HTTP2ClientStreamConsumer: NIOHTTP2ClientStreamConsumer {
    typealias StreamState = HTTP2ClientStream

    let driver: HTTP2ClientStreamDriver

    mutating func processStreams(_ streams: inout NIOHTTP2Streams<HTTP2ClientStreamConsumer>) {
        self.driver.process(&streams)
    }
}

/// Runs every request of one HTTP/2 client connection as a stream of nio-http2's consumer mode: no channel per
/// stream, and no pipeline hop for frames or stream events. nio-http2 owns stream IDs, flow control, END folding,
/// the GOAWAY sweep and the batch flush; this bridges streams to `HTTPRequestStateMachine` and runs what
/// `HTTP2IdleHandler` did. Event loop only.
///
/// Re-entrancy rule: nothing calls out (to the request or the pool) while nio-http2 lends a stream. Visits only
/// collect events and parts; the driver acts on them after the slot access. A call out may remove the stream, so code
/// after it checks `stream.isActive`.
final class HTTP2ClientStreamDriver {
    static let targetWindowSize = 8 * 1024 * 1024
    /// Client stream IDs are odd, from 1 to `Int32.max`.
    static let maximumStreamCount = Int(Int32.max / 2)

    let eventLoop: any EventLoop
    private let channel: any Channel
    private var connection: NIOHTTP2StreamConnection<HTTP2ClientStreamConsumer>?
    private var idle: HTTP2IdleHandler<HTTP2Connection>.StateMachine
    private var delegate: HTTP2Connection?

    private var streams: [HTTP2ClientStream] = []
    private var touched: [HTTP2ClientStream] = []
    private var touchedScratch: [HTTP2ClientStream] = []
    private var scratch: [HTTP2ClientStream] = []

    init(channel: any Channel) {
        self.channel = channel
        self.eventLoop = channel.eventLoop
        self.idle = .init(maximumUses: nil)
    }

    var openStreamCount: Int {
        self.streams.count
    }

    /// Adds the consumer `NIOHTTP2Handler` to the pipeline.
    func start(
        on pipeline: ChannelPipeline.SynchronousOperations,
        initialSettings: HTTP2Settings,
        delegate: HTTP2Connection,
        maximumUses: Int?
    ) throws {
        self.idle = .init(maximumUses: maximumUses)
        self.delegate = delegate
        var configuration = NIOHTTP2Handler.Configuration()
        configuration.connection.initialSettings = initialSettings
        configuration.connection.targetWindowSize = Self.targetWindowSize
        configuration.stream.targetWindowSize = Self.targetWindowSize
        configuration.stream.outboundBufferSizeHighWatermark = 8196
        configuration.stream.outboundBufferSizeLowWatermark = 4092
        self.connection = try pipeline.configureHTTP2Connection(configuration: configuration) { _ in
            HTTP2ClientStreamConsumer(driver: self)
        }
        if self.channel.isActive {
            self.idle.channelActive()
        }
    }

    func executeRequest(_ request: any HTTPExecutableRequest) {
        self.eventLoop.assertInEventLoop()
        let executed: Void? = self.withBatch {
            let stream = HTTP2ClientStream(request: request, driver: self, parentIsWritable: self.channel.isWritable)
            self.add(stream)
            request.willExecuteRequest(StreamExecutor(stream))
            guard stream.isActive else { return }
            self.run(
                stream.machine.startRequest(head: request.requestHead, metadata: request.requestFramingMetadata),
                stream
            )
            // The child channel's auto-read on activation.
            guard stream.isActive else { return }
            self.run(stream.machine.read(), stream)
        }
        if executed == nil {
            request.fail(HTTPClientError.cancelled)
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

    func shutdownRequested() {
        self.eventLoop.assertInEventLoop()
        self.runIdle(self.idle.closeEventReceived())
    }

    /// One batch on the connection: everything written inside leaves in one flush. `nil` once the connection is gone.
    @discardableResult
    private func withBatch<Result>(_ body: () -> Result) -> Result? {
        self.connection?.withStreams { _ in body() }
    }

    // A visit from nio-http2.

    fileprivate func process(_ streams: inout NIOHTTP2Streams<HTTP2ClientStreamConsumer>) {
        let connectionEvents = streams.connectionEvents
        streams.forEachReady { stream, state in
            let events = stream.events
            if events.contains(.closed) || state.unsatisfiedRead {
                Self.takeParts(&stream, into: state)
            } else if events.contains(.readable) {
                state.partsInSlot = true
            }
            state.pendingEvents.formUnion(events)
            if events.contains(.closed) {
                state.closeReason = stream.closeReason
            }
            if !state.isListedForEvents {
                state.isListedForEvents = true
                self.touched.append(state)
            }
        }

        if connectionEvents.contains(.closed) {
            // Before the streams' closes: like the idle handler's channelInactive, so the final closes don't report
            // free capacity on a dying connection.
            self.idle.channelInactive()
        }
        if connectionEvents.contains(.settingsChanged) {
            self.idle.channelActive()
            let maxStreams = streams.remoteMaxConcurrentStreams ?? 100
            self.runIdle(
                self.idle.settingsReceived([HTTP2Setting(parameter: .maxConcurrentStreams, value: maxStreams)])
            )
        }
        if connectionEvents.contains(.goAwayReceived) {
            self.runIdle(self.idle.goAwayReceived())
        }

        swap(&self.touchedScratch, &self.touched)
        var index = 0
        while index < self.touchedScratch.count {
            let stream = self.touchedScratch[index]
            stream.isListedForEvents = false
            self.handleEvents(stream)
            index += 1
        }
        self.touchedScratch.removeAll(keepingCapacity: true)

        if connectionEvents.contains(.closed) {
            self.delegate = nil
        }
    }

    private static func takeParts(_ stream: inout NIOHTTP2Stream, into state: HTTP2ClientStream) {
        while let part = stream.nextInboundPart() {
            state.pendingReads.append(part)
        }
        state.partsInSlot = false
    }

    private func handleEvents(_ stream: HTTP2ClientStream) {
        let events = stream.pendingEvents
        stream.pendingEvents = []
        // `.localEndSent` arrives with the stream's next visit, often its last: report it before the close delivers
        // the response end.
        if events.contains(.localEndSent), stream.awaitingBodySent, stream.isActive {
            stream.awaitingBodySent = false
            stream.request.requestBodyStreamSent()
        }
        if events.contains(.closed) {
            stream.closedByNIO = true
            if stream.countedByIdle {
                stream.countedByIdle = false
                // The idle handler used to see the close first.
                self.runIdle(self.idle.streamClosed())
            }
            self.streamClosed(stream)
            return
        }
        guard stream.isActive else { return }
        if events.contains(.writable), !stream.isWritable {
            stream.isWritable = true
            self.run(stream.machine.writabilityChanged(writable: true), stream)
        }
        if events.contains(.readable) {
            self.tryToRead(stream)
        }
    }

    private func runIdle(_ action: HTTP2IdleHandler<HTTP2Connection>.StateMachine.Action) {
        switch action {
        case .nothing:
            break
        case .notifyConnectionNewMaxStreamsSettings(let maxStreams):
            self.delegate?.http2SettingsReceived(maxStreams: maxStreams)
        case .notifyConnectionStreamClosed(let currentlyAvailable):
            self.delegate?.http2StreamClosed(availableStreams: currentlyAvailable)
        case .notifyConnectionGoAwayReceived:
            self.delegate?.http2GoAwayReceived()
        case .close:
            self.channel.close(mode: .all, promise: nil)
        }
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
            self.sendRequestEnd(trailers, promise: writePromise, stream)

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

    // Writing.

    private func sendRequestHead(_ head: HTTPRequestHead, sendEnd: Bool, _ stream: HTTP2ClientStream) {
        guard let connection = self.connection else {
            self.run(stream.machine.errorHappened(HTTPClientError.cancelled), stream)
            return
        }
        let headers: HPACKHeaders
        do {
            headers = try HPACKHeaders(http2ClientRequestHead: head)
        } catch {
            self.run(stream.machine.errorHappened(error), stream)
            return
        }
        do {
            stream.handle = try connection.openStream(headers: headers, endStream: sendEnd) { nioStream in
                stream.streamID = nioStream.id
                return stream
            }
        } catch {
            self.run(stream.machine.errorHappened(HTTPClientError.cancelled), stream)
            return
        }
        stream.headersWritten = true
        stream.localEnded = sendEnd
        stream.countedByIdle = true
        self.runIdle(self.idle.streamCreated())
        guard stream.isActive else { return }
        self.run(stream.machine.headSent(), stream)
    }

    private func sendBodyPart(_ data: IOData, promise: EventLoopPromise<Void>?, _ stream: HTTP2ClientStream) {
        // Promises are completed after the access: their callbacks may write to this stream again.
        // nil: stale stream or the write failed; otherwise the bytes still writable.
        let writableBytes: Int?? = self.withStream(stream) { nioStream -> Int? in
            guard (try? nioStream.writeData(data)) != nil else { return nil }
            if let promise {
                nioStream.completeWhenWritten(promise)
            }
            return nioStream.writableBytes
        }
        guard let writableBytes = writableBytes ?? nil else {
            promise?.fail(HTTPClientError.requestStreamCancelled)
            return
        }
        if writableBytes == 0, stream.isWritable, stream.isActive {
            stream.isWritable = false
            self.run(stream.machine.writabilityChanged(writable: false), stream)
        }
    }

    private func sendRequestEnd(_ trailers: HTTPHeaders?, promise: EventLoopPromise<Void>?, _ stream: HTTP2ClientStream) {
        stream.localEnded = true
        stream.awaitingBodySent = true
        let written: Bool? = self.withStream(stream) { nioStream in
            // Attached before the end, so it completes with the END frame's socket write.
            if let promise {
                nioStream.completeWhenWritten(promise)
            }
            do {
                try nioStream.writeEnd(trailers: trailers.map { HPACKHeaders(http2ClientTrailers: $0) })
                return true
            } catch {
                return false
            }
        }
        if written == nil {
            // Stale stream: the promise never reached nio-http2.
            promise?.fail(HTTPClientError.requestStreamCancelled)
        }
    }

    /// A short access to the stream's nio-http2 side. Never call out from `body`.
    private func withStream<Result>(_ stream: HTTP2ClientStream, _ body: (inout NIOHTTP2Stream) -> Result) -> Result? {
        guard let handle = stream.handle, let connection = self.connection else { return nil }
        return connection.withStream(handle) { nioStream, _ in body(&nioStream) }
    }

    // Reading.

    private func read0(_ stream: HTTP2ClientStream) {
        guard !stream.unsatisfiedRead else { return }
        stream.unsatisfiedRead = true
        // Only when a visit left parts behind: otherwise the next visit brings them, and the access is saved.
        if stream.pendingReads.isEmpty, stream.partsInSlot {
            _ = self.withStream(stream) { nioStream in Self.takeParts(&nioStream, into: stream) }
        }
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

    /// Delivers every queued part, then the stream's read complete, like `HTTP2StreamChannel.deliverPendingReads`.
    private func deliverPendingReads(_ stream: HTTP2ClientStream) {
        while stream.isActive, let part = stream.pendingReads.popFirst() {
            switch part {
            case .headers(_, let endStream), .data(_, let endStream):
                if endStream { stream.remoteEnded = true }
            }
            let parts: (first: HTTPClientResponsePart?, second: HTTPClientResponsePart?)
            do {
                parts = try stream.decoder.decode(part)
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
        }
        guard stream.isActive else { return }
        self.run(stream.machine.channelReadComplete(), stream)
    }

    // Closing streams.

    private func streamClosed(_ stream: HTTP2ClientStream) {
        guard stream.isActive else { return }
        if !stream.pendingReads.isEmpty {
            stream.unsatisfiedRead = false
            self.deliverPendingReads(stream)
        }
        guard stream.isActive else { return }
        switch stream.closeReason {
        case .reset(let code, _):
            self.run(
                stream.machine.errorHappened(NIOHTTP2Errors.streamClosed(streamID: stream.streamID, errorCode: code)),
                stream
            )
        case .refused:
            self.run(
                stream.machine.errorHappened(
                    NIOHTTP2Errors.streamClosed(streamID: stream.streamID, errorCode: .refusedStream)
                ),
                stream
            )
        case .writeFailed(let error):
            self.run(stream.machine.errorHappened(error), stream)
        case .ended, .connectionClosed, .none:
            self.run(stream.machine.channelInactive(), stream)
        }
        self.removeStream(stream)
    }

    /// Removes a stream that is being given up, resetting it if it's still open on the wire.
    private func dropStream(_ stream: HTTP2ClientStream) {
        guard stream.isActive else { return }
        self.removeStream(stream)
        if stream.needsReset {
            _ = self.withStream(stream) { nioStream in nioStream.reset(.cancel) }
        }
    }

    private func add(_ stream: HTTP2ClientStream) {
        stream.listIndex = self.streams.count
        self.streams.append(stream)
    }

    private func removeStream(_ stream: HTTP2ClientStream) {
        guard stream.isActive else { return }
        stream.isActive = false
        let index = stream.listIndex
        let last = self.streams.removeLast()
        if last !== stream {
            self.streams[index] = last
            last.listIndex = index
        }
    }

    private func forEachStream(_ body: (HTTP2ClientStream) -> Void) {
        var streams = self.scratch
        self.scratch = []
        streams.removeAll(keepingCapacity: true)
        streams.append(contentsOf: self.streams)
        for stream in streams where stream.isActive {
            body(stream)
        }
        streams.removeAll(keepingCapacity: true)
        self.scratch = streams
    }
}

@available(*, unavailable)
extension HTTP2ClientStreamDriver: Sendable {}

extension HTTP2ClientStreamDriver {
    /// A request's handle on its stream: 3 words (the loop and the stream), so it fits an existential's inline
    /// buffer. Any thread: calls straight through on the event loop, hops otherwise. A call for a finished stream
    /// sees `isActive == false` and does nothing.
    struct StreamExecutor: HTTPRequestExecutor {
        private let stream: NIOLoopBound<HTTP2ClientStream>

        init(_ stream: HTTP2ClientStream) {
            self.stream = NIOLoopBound(stream, eventLoop: stream.driver.eventLoop)
        }

        func writeRequestBodyPart(_ data: IOData, request: any HTTPExecutableRequest, promise: EventLoopPromise<Void>?)
        {
            if self.stream.eventLoop.inEventLoop {
                let stream = self.stream.value
                stream.driver.writeRequestBodyPart0(data, stream: stream, request: request, promise: promise)
            } else {
                let stream = self.stream
                stream.eventLoop.execute {
                    stream.value.driver.writeRequestBodyPart0(
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
                stream.driver.finishRequestBodyStream0(
                    trailers: trailers,
                    stream: stream,
                    request: request,
                    promise: promise
                )
            } else {
                let stream = self.stream
                stream.eventLoop.execute {
                    stream.value.driver.finishRequestBodyStream0(
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
                stream.driver.demandResponseBodyStream0(stream: stream, request: request)
            } else {
                let stream = self.stream
                stream.eventLoop.execute {
                    stream.value.driver.demandResponseBodyStream0(stream: stream.value, request: request)
                }
            }
        }

        func cancelRequest(_ request: any HTTPExecutableRequest) {
            if self.stream.eventLoop.inEventLoop {
                let stream = self.stream.value
                stream.driver.cancelRequest0(stream: stream, request: request)
            } else {
                let stream = self.stream
                stream.eventLoop.execute {
                    stream.value.driver.cancelRequest0(stream: stream.value, request: request)
                }
            }
        }
    }
}
