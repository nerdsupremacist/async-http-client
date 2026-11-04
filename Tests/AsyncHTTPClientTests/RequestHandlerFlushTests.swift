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
import NIOCore
import NIOEmbedded
import NIOHTTP1
import NIOHTTP2
import Testing

@testable import AsyncHTTPClient

@Suite struct RequestHandlerFlushTests {
    @Test(arguments: Handler.allCases)
    func knownLengthBodyIsFlushedOnce(handler: Handler) throws {
        let flushCounter = FlushCountingHandler()
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: [flushCounter])
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)

        channel.write(request, promise: nil)

        #expect(flushCounter.flushCount == 1)
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .head(request.requestHead))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .body(.byteBuffer(.helloWorld)))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .end(nil))
    }

    @Test(arguments: Handler.allCases)
    func streamingBodyIsFlushedPerPart(handler: Handler) throws {
        let flushCounter = FlushCountingHandler()
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: [flushCounter])
        let request = MockHTTPExecutableRequest(
            head: .init(version: .http1_1, method: .POST, uri: "/", headers: ["host": "localhost"]),
            framingMetadata: RequestFramingMetadata(connectionClose: false, body: .stream),
            raiseErrorIfUnimplementedMethodIsCalled: false
        )

        channel.write(request, promise: nil)
        let flushesAfterHead = flushCounter.flushCount
        executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: nil)
        let flushesAfterBodyPart = flushCounter.flushCount
        executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)

        #expect(flushesAfterHead == 1)
        #expect(flushesAfterBodyPart == 2)
        #expect(flushCounter.flushCount == 3)
    }

    @Test(arguments: Handler.allCases)
    func requestWithoutBodyIsFlushedOnce(handler: Handler) throws {
        let flushCounter = FlushCountingHandler()
        let (channel, _) = try EmbeddedChannel.makeConnected(handler, after: [flushCounter])
        let request = MockHTTPExecutableRequest(raiseErrorIfUnimplementedMethodIsCalled: false)

        channel.write(request, promise: nil)

        #expect(flushCounter.flushCount == 1)
    }

    @Test(arguments: Handler.allCases)
    func synchronouslyStreamedBodyIsOnlyHeldBackForItsFirstPart(handler: Handler) throws {
        let flushCounter = FlushCountingHandler()
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: [flushCounter])
        let request = MockHTTPExecutableRequest(
            head: .init(version: .http1_1, method: .POST, uri: "/", headers: ["host": "localhost"]),
            framingMetadata: RequestFramingMetadata(connectionClose: false, body: .stream),
            raiseErrorIfUnimplementedMethodIsCalled: false
        )
        request.resumeRequestBodyStreamCallback = {
            for _ in 0..<3 {
                executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: nil)
            }
            executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)
        }

        channel.write(request, promise: nil)

        // The head and the first part are flushed when the second part arrives. From then on every part and the end
        // are flushed as they are written, so a synchronous producer still sees backpressure.
        #expect(flushCounter.flushCount == 4)
    }

    @Test(arguments: Handler.allCases)
    func bodyIsDeferredWhenWritingTheHeadMakesTheChannelUnwritable(handler: Handler) throws {
        let flushCounter = FlushCountingHandler()
        let (channel, executor) = try EmbeddedChannel.makeConnected(
            handler,
            after: [flushCounter, LoseWritabilityOnHeadHandler(regainsOnFlush: false)]
        )
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)

        channel.write(request, promise: nil)

        #expect(request.events.map(\.kind) == [.willExecuteRequest, .requestHeadSent])
        #expect(flushCounter.flushCount == 1)
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

private final class FlushCountingHandler: ChannelOutboundHandler {
    typealias OutboundIn = Any

    private(set) var flushCount = 0

    func flush(context: ChannelHandlerContext) {
        self.flushCount += 1
        context.flush()
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

extension ByteBuffer {
    fileprivate static let helloWorld = ByteBuffer(string: "Hello World")
}
