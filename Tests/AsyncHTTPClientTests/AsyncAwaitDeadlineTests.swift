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

import AsyncHTTPClient
import Dispatch
import Logging
import NIOCore
import NIOHTTP1
import NIOPosix
import NIOSSL
import Testing

@Suite("Async execute deadline")
struct AsyncAwaitDeadlineTests {
    private static func makeClient() -> HTTPClient {
        var config = HTTPClient.Configuration()
        config.tlsConfiguration = .clientDefault
        config.tlsConfiguration?.certificateVerification = .none
        config.httpVersion = .automatic
        return HTTPClient(eventLoopGroupProvider: .singleton, configuration: config)
    }

    @Test func deadlineFailsRequestWaitingForResponseHead() async throws {
        let bin = HTTPBin(.http2(compress: false))
        defer { #expect(throws: Never.self) { try bin.shutdown() } }
        let client = Self.makeClient()
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }
        let logger = Logger(label: "HTTPClient")

        // Warm up the connection, so the deadline can't race the connect timeout.
        let warmUp = try await client.execute(
            HTTPClientRequest(url: "https://localhost:\(bin.port)/get"),
            deadline: .now() + .seconds(5),
            logger: logger
        )
        _ = try await warmUp.body.collect(upTo: 1024 * 1024)

        // `/wait` never sends a response head.
        await #expect(throws: HTTPClientError.deadlineExceeded) {
            _ = try await client.execute(
                HTTPClientRequest(url: "https://localhost:\(bin.port)/wait"),
                deadline: .now() + .milliseconds(100),
                logger: logger
            )
        }
    }

    @Test func finishedRequestIsReleasedBeforeItsDeadline() async throws {
        let bin = HTTPBin(.http2(compress: false))
        defer { #expect(throws: Never.self) { try bin.shutdown() } }
        let client = Self.makeClient()
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }

        let body = try await Self.executePost(client: client, port: bin.port, deadline: .now() + .hours(1))

        // A deadline that's still armed keeps the transaction, and with it the request body, alive until it fires.
        let giveUp = NIODeadline.now() + .seconds(2)
        while body.value != nil && NIODeadline.now() < giveUp {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        #expect(body.value == nil)
    }

    @Test func shorterDeadlinesBehindALongerOneStillFire() async throws {
        let bin = HTTPBin(.http2(compress: false))
        defer { #expect(throws: Never.self) { try bin.shutdown() } }
        // One loop, so all three deadlines share it.
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { #expect(throws: Never.self) { try group.syncShutdownGracefully() } }
        var config = HTTPClient.Configuration()
        config.tlsConfiguration = .clientDefault
        config.tlsConfiguration?.certificateVerification = .none
        let client = HTTPClient(eventLoopGroupProvider: .shared(group), configuration: config)
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }
        let logger = Logger(label: "HTTPClient")

        let warmUp = try await client.execute(
            HTTPClientRequest(url: "https://localhost:\(bin.port)/get"),
            deadline: .now() + .seconds(5),
            logger: logger
        )
        _ = try await warmUp.body.collect(upTo: 1024 * 1024)

        // Both deadlines are shorter than the warm-up's, and the second passes after the first has fired.
        let start = NIODeadline.now()
        try await withThrowingTaskGroup(of: Void.self) { group in
            for delay in [100, 300] {
                group.addTask {
                    await #expect(throws: HTTPClientError.deadlineExceeded) {
                        _ = try await client.execute(
                            HTTPClientRequest(url: "https://localhost:\(bin.port)/wait"),
                            deadline: .now() + .milliseconds(Int64(delay)),
                            logger: logger
                        )
                    }
                }
            }
            try await group.waitForAll()
        }
        #expect(NIODeadline.now() - start < .seconds(5))
    }

    @Test func deadlineFiresOnALoopTheGroupDoesNotList() async throws {
        let bin = HTTPBin(.http2(compress: false))
        defer { #expect(throws: Never.self) { try bin.shutdown() } }
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { #expect(throws: Never.self) { try group.syncShutdownGracefully() } }
        var config = HTTPClient.Configuration()
        config.tlsConfiguration = .clientDefault
        config.tlsConfiguration?.certificateVerification = .none
        // No sweeper exists for a loop the group doesn't list, so the deadline falls back to `scheduleTask`.
        let client = HTTPClient(eventLoopGroup: UnlistedLoopGroup(base: group), configuration: config)
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }

        await #expect(throws: HTTPClientError.deadlineExceeded) {
            _ = try await client.execute(
                HTTPClientRequest(url: "https://localhost:\(bin.port)/wait"),
                deadline: .now() + .milliseconds(500),
                logger: Logger(label: "HTTPClient")
            )
        }
    }

    /// Runs a POST whose body is an object, consumes the response, and returns a weak reference to the body.
    private static func executePost(
        client: HTTPClient,
        port: Int,
        deadline: NIODeadline
    ) async throws -> Weak<OneChunkBody> {
        let body = OneChunkBody()
        var request = HTTPClientRequest(url: "https://localhost:\(port)/post")
        request.method = .POST
        request.body = .stream(body, length: .known(Int64(OneChunkBody.chunk.readableBytes)))
        let response = try await client.execute(request, deadline: deadline, logger: Logger(label: "HTTPClient"))
        #expect(response.status == .ok)
        _ = try await response.body.collect(upTo: 1024 * 1024)
        return Weak(value: body)
    }

    @Test func deadlinePassingAfterResponseHeadDoesNotFailTheRequest() async throws {
        let bin = HTTPBin(.http2(compress: false)) { _ in HTTPEchoHandler() }
        defer { #expect(throws: Never.self) { try bin.shutdown() } }
        let waitBin = HTTPBin(.http2(compress: false))
        defer { #expect(throws: Never.self) { try waitBin.shutdown() } }
        // One loop, so the second request's deadline fires on the same loop as the first's.
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { #expect(throws: Never.self) { try group.syncShutdownGracefully() } }
        var config = HTTPClient.Configuration()
        config.tlsConfiguration = .clientDefault
        config.tlsConfiguration?.certificateVerification = .none
        let client = HTTPClient(eventLoopGroupProvider: .shared(group), configuration: config)
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }

        var request = HTTPClientRequest(url: "https://localhost:\(bin.port)/handler")
        request.method = .POST
        let writer = AsyncSequenceWriter<ByteBuffer>()
        request.body = .stream(writer, length: .unknown)

        // The echo server sends the response head as soon as it gets the request head.
        let response = try await client.execute(
            request,
            deadline: .now() + .milliseconds(100),
            logger: Logger(label: "HTTPClient")
        )
        #expect(response.status == .ok)

        // A later deadline on the same loop firing proves the first one has passed; the body must still go through.
        await #expect(throws: HTTPClientError.deadlineExceeded) {
            _ = try await client.execute(
                HTTPClientRequest(url: "https://localhost:\(waitBin.port)/wait"),
                deadline: .now() + .milliseconds(200),
                logger: Logger(label: "HTTPClient")
            )
        }
        var iterator = response.body.makeAsyncIterator()
        writer.write(ByteBuffer(string: "after the deadline"))
        #expect(try await iterator.next() == ByteBuffer(string: "after the deadline"))
        writer.end()
        #expect(try await iterator.next() == nil)
    }
}

/// Hands out the loops of `base`, but lists none of them in `makeIterator()`.
private final class UnlistedLoopGroup: EventLoopGroup {
    let base: MultiThreadedEventLoopGroup

    init(base: MultiThreadedEventLoopGroup) {
        self.base = base
    }

    func next() -> any EventLoop {
        self.base.next()
    }

    func makeIterator() -> EventLoopIterator {
        EventLoopIterator([])
    }

    func shutdownGracefully(queue: DispatchQueue, _ callback: @escaping @Sendable ((any Error)?) -> Void) {
        callback(nil)
    }
}

private struct Weak<Value: AnyObject> {
    weak var value: Value?
}

/// A request body that's a class, so a test can tell when the request has been released.
private final class OneChunkBody: AsyncSequence, Sendable {
    typealias Element = ByteBuffer
    static let chunk = ByteBuffer(string: "hello")

    struct AsyncIterator: AsyncIteratorProtocol {
        var sent = false

        mutating func next() async -> ByteBuffer? {
            if self.sent { return nil }
            self.sent = true
            return OneChunkBody.chunk
        }
    }

    func makeAsyncIterator() -> AsyncIterator { AsyncIterator() }
}
