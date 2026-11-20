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

import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOEmbedded
import NIOHTTP1
import NIOHTTP2
import NIOTestUtils
import Testing

@testable import AsyncHTTPClient

@Suite struct RequestHandlerFlushTests {
    @Test(arguments: Handler.allCases)
    func knownLengthBodyIsFlushedOnce(handler: Handler) throws {
        let eventCounter = EventCounterHandler()
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: [eventCounter])
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)

        channel.write(request, promise: nil)

        #expect(eventCounter.flushCalls == 1)
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .head(request.requestHead))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .body(.byteBuffer(.helloWorld)))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .end(nil))
    }

    @Test(arguments: Handler.allCases)
    func streamingBodyPartsAreSentAsTheyAreWritten(handler: Handler) throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: [])
        let request = MockHTTPExecutableRequest.streamingPost()

        channel.write(request, promise: nil)
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .head(request.requestHead))
        executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: nil)
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .body(.byteBuffer(.helloWorld)))
        executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .end(nil))
    }

    @Test(arguments: Handler.allCases)
    func synchronouslyStreamedBodyIsSentCompletely(handler: Handler) throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: [])
        let request = MockHTTPExecutableRequest.streamingPost()
        request.resumeRequestBodyStreamCallback = {
            for _ in 0..<3 {
                executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: nil)
            }
            executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)
        }

        channel.write(request, promise: nil)

        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .head(request.requestHead))
        for _ in 0..<3 {
            #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .body(.byteBuffer(.helloWorld)))
        }
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .end(nil))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == nil)
    }

    @Test(arguments: Handler.allCases)
    func bodyIsDeferredWhenWritingTheHeadMakesTheChannelUnwritable(handler: Handler) throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(
            handler,
            after: [LoseWritabilityOnHeadHandler(regainsOnFlush: false)]
        )
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)

        channel.write(request, promise: nil)

        #expect(request.events.map(\.kind) == [.willExecuteRequest, .requestHeadSent])
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .head(request.requestHead))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == nil)
    }

    @Test(arguments: Handler.allCases)
    func bodyResumedFromWithinTheFlushIsFlushed(handler: Handler) throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(
            handler,
            after: [LoseWritabilityOnHeadHandler(regainsOnFlush: true)]
        )
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)

        channel.write(request, promise: nil)

        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .head(request.requestHead))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .body(.byteBuffer(.helloWorld)))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .end(nil))
    }

    @Test(arguments: Handler.allCases)
    func bodyLongerThanItsLengthFailsTheRequestAndCompletesEveryWrite(handler: Handler) throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: handler.frameCachingCodecs)
        let writes = WriteCompletions(count: 2, on: channel.eventLoop)
        defer { writes.failPending() }
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)
        request.resumeRequestBodyStreamCallback = {
            executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: writes.promises[0])
            executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: writes.promises[1])
            executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)
        }

        channel.write(request, promise: nil)

        #expect(request.events.map(\.kind).contains(.fail))
        #expect(writes.completedCount == 2)
    }

    @Test(arguments: Handler.allCases)
    func requestCancelledWhileWritesAreBatchedCompletesEveryWrite(handler: Handler) throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: handler.frameCachingCodecs)
        let writes = WriteCompletions(count: 1, on: channel.eventLoop)
        defer { writes.failPending() }
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)
        request.resumeRequestBodyStreamCallback = {
            executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: writes.promises[0])
            executor.cancelRequest(request)
            executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)
        }

        channel.write(request, promise: nil)

        #expect(request.events.map(\.kind).contains(.fail))
        #expect(writes.completedCount == 1)
    }

    @Test func http1RequestAfterABatchedRequestSendsPartsAsTheyAreWritten() throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(.http1, after: [])
        let firstRequest = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)
        channel.write(firstRequest, promise: nil)
        try channel.writeInbound(HTTPClientResponsePart.head(.init(version: .http1_1, status: .ok)))
        channel.read()
        try channel.writeInbound(HTTPClientResponsePart.end(nil))
        #expect(firstRequest.events.map(\.kind).contains(.receiveResponseEnd))
        while try channel.readOutbound(as: HTTPClientRequestPart.self) != nil {}
        let secondRequest = MockHTTPExecutableRequest.streamingPost()

        channel.write(secondRequest, promise: nil)
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .head(secondRequest.requestHead))
        executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: secondRequest, promise: nil)
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .body(.byteBuffer(.helloWorld)))
        executor.finishRequestBodyStream(trailers: nil, request: secondRequest, promise: nil)
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .end(nil))
    }

    @Test func http2KnownLengthBodyIsSentAsHeadersAndOneDataFrame() throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(
            .http2,
            after: [HTTP2FramePayloadToHTTP1ClientCodec(httpProtocol: .https)]
        )
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)

        channel.write(request, promise: nil)

        guard case .headers(let headers) = try channel.readOutbound(as: HTTP2Frame.FramePayload.self) else {
            Issue.record("expected a HEADERS frame first")
            return
        }
        #expect(!headers.endStream)
        guard case .data(let data) = try channel.readOutbound(as: HTTP2Frame.FramePayload.self) else {
            Issue.record("expected a DATA frame second")
            return
        }
        #expect(data.data == .byteBuffer(.helloWorld))
        #expect(data.endStream)
        #expect(try channel.readOutbound(as: HTTP2Frame.FramePayload.self) == nil)
    }
}

extension RequestHandlerFlushTests {
    enum Handler: CaseIterable {
        case http1
        case http2
    }
}

extension RequestHandlerFlushTests.Handler {
    // HTTP/2's codec holds the last frame and its promise until the next write or flush; HTTP/1's encoder holds
    // nothing.
    fileprivate var frameCachingCodecs: [any ChannelHandler] {
        switch self {
        case .http1:
            []
        case .http2:
            [HTTP2FramePayloadToHTTP1ClientCodec(httpProtocol: .https)]
        }
    }
}

/// Write promises that count how many of them have completed, successfully or not.
private final class WriteCompletions: Sendable {
    let promises: [EventLoopPromise<Void>]
    private let completed = NIOLockedValueBox(0)

    var completedCount: Int {
        self.completed.withLockedValue { $0 }
    }

    init(count: Int, on eventLoop: any EventLoop) {
        self.promises = (0..<count).map { _ in eventLoop.makePromise(of: Void.self) }
        for promise in self.promises {
            promise.futureResult.whenComplete { _ in
                self.completed.withLockedValue { $0 += 1 }
            }
        }
    }

    /// Completes the promises a failing test left open, so that they aren't reported as leaked.
    func failPending() {
        for promise in self.promises {
            promise.fail(ChannelError.ioOnClosedChannel)
        }
    }
}

/// Makes the channel non-writable when the request head is written, as a large head does on a real channel, and
/// optionally writable again once it is flushed.
private final class LoseWritabilityOnHeadHandler: ChannelOutboundHandler {
    typealias OutboundIn = HTTPClientRequestPart

    private let regainsOnFlush: Bool

    init(regainsOnFlush: Bool) {
        self.regainsOnFlush = regainsOnFlush
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let isHead: Bool
        if case .head = self.unwrapOutboundIn(data) {
            isHead = true
        } else {
            isHead = false
        }
        context.write(data, promise: promise)
        if isHead {
            (context.channel as! EmbeddedChannel).isWritable = false
            context.fireChannelWritabilityChanged()
        }
    }

    func flush(context: ChannelHandlerContext) {
        context.flush()
        if self.regainsOnFlush, !context.channel.isWritable {
            (context.channel as! EmbeddedChannel).isWritable = true
            context.fireChannelWritabilityChanged()
        }
    }
}

extension EmbeddedChannel {
    /// A connected channel whose last handler is the request handler, behind `handlers`.
    fileprivate static func makeConnected(
        _ handler: RequestHandlerFlushTests.Handler,
        after handlers: [any ChannelHandler]
    ) throws -> (EmbeddedChannel, any HTTPRequestExecutor) {
        let eventLoop = EmbeddedEventLoop()
        let requestHandler: any ChannelHandler
        let executor: any HTTPRequestExecutor
        switch handler {
        case .http1:
            let http1Handler = HTTP1ClientChannelHandler(
                eventLoop: eventLoop,
                backgroundLogger: Logger(label: "no-op", factory: SwiftLogNoOpLogHandler.init),
                connectionIdLoggerMetadata: "test connection"
            )
            requestHandler = http1Handler
            executor = http1Handler.requestExecutor
        case .http2:
            let http2Handler = HTTP2ClientRequestHandler(eventLoop: eventLoop)
            requestHandler = http2Handler
            executor = http2Handler.requestExecutor
        }
        let channel = EmbeddedChannel(handlers: handlers + [requestHandler], loop: eventLoop)
        try channel.connect(to: .init(ipAddress: "127.0.0.1", port: 80)).wait()
        return (channel, executor)
    }
}

extension MockHTTPExecutableRequest {
    /// A POST whose body is written and finished as soon as the handler resumes the body stream, the way
    /// `Transaction` writes `.byteBuffer` and `.sequence` bodies.
    fileprivate static func knownLengthPost(
        writingThrough executor: any HTTPRequestExecutor
    ) -> MockHTTPExecutableRequest {
        let request = MockHTTPExecutableRequest(
            head: .init(version: .http1_1, method: .POST, uri: "/", headers: ["host": "localhost"]),
            framingMetadata: RequestFramingMetadata(
                connectionClose: false,
                body: .fixedSize(Int64(ByteBuffer.helloWorld.readableBytes))
            ),
            raiseErrorIfUnimplementedMethodIsCalled: false
        )
        request.resumeRequestBodyStreamCallback = {
            executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: nil)
            executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)
        }
        return request
    }
}

extension MockHTTPExecutableRequest {
    /// A POST whose body is streamed: the test writes its parts through the executor.
    fileprivate static func streamingPost() -> MockHTTPExecutableRequest {
        MockHTTPExecutableRequest(
            head: .init(version: .http1_1, method: .POST, uri: "/", headers: ["host": "localhost"]),
            framingMetadata: RequestFramingMetadata(connectionClose: false, body: .stream),
            raiseErrorIfUnimplementedMethodIsCalled: false
        )
    }
}

extension ByteBuffer {
    fileprivate static let helloWorld = ByteBuffer(string: "Hello World")
}
