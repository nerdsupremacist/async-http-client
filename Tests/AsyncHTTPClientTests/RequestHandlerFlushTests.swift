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
    func bodyResumedFromWithinTheFlushIsFlushed(handler: Handler) throws {
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: [UnwritableUntilFlushHandler()])
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)

        channel.write(request, promise: nil)

        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .head(request.requestHead))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .body(.byteBuffer(.helloWorld)))
        #expect(try channel.readOutbound(as: HTTPClientRequestPart.self) == .end(nil))
    }

    @Test(arguments: Handler.allCases)
    func requestCancelledWhileWritesAreBatchedCompletesTheBatchedWrite(handler: Handler) throws {
        let codecs: [any ChannelHandler] =
            handler == .http2 ? [HTTP2FramePayloadToHTTP1ClientCodec(httpProtocol: .https)] : []
        let (channel, executor) = try EmbeddedChannel.makeConnected(handler, after: codecs)
        let writePromise = channel.eventLoop.makePromise(of: Void.self)
        defer { writePromise.fail(ChannelError.ioOnClosedChannel) }
        let writeCompleted = NIOLockedValueBox(false)
        writePromise.futureResult.whenComplete { _ in writeCompleted.withLockedValue { $0 = true } }
        let request = MockHTTPExecutableRequest.knownLengthPost(writingThrough: executor)
        request.resumeRequestBodyStreamCallback = {
            executor.writeRequestBodyPart(.byteBuffer(.helloWorld), request: request, promise: writePromise)
            executor.cancelRequest(request)
            executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)
        }

        channel.write(request, promise: nil)

        #expect(request.events.map(\.kind).contains(.fail))
        #expect(writeCompleted.withLockedValue { $0 })
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

private final class UnwritableUntilFlushHandler: ChannelOutboundHandler {
    typealias OutboundIn = HTTPClientRequestPart

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let isHead = if case .head = self.unwrapOutboundIn(data) { true } else { false }
        context.write(data, promise: promise)
        if isHead {
            (context.channel as! EmbeddedChannel).isWritable = false
            context.fireChannelWritabilityChanged()
        }
    }

    func flush(context: ChannelHandlerContext) {
        context.flush()
        if !context.channel.isWritable {
            (context.channel as! EmbeddedChannel).isWritable = true
            context.fireChannelWritabilityChanged()
        }
    }
}

extension EmbeddedChannel {
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
