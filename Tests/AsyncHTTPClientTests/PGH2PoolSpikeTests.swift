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
import NIOHTTP1
import NIOSSL
import Testing

@testable import AsyncHTTPClient

// Spike: HTTP/2 requests from the async API run through postgres-nio's `ConnectionPool`.
struct PGH2PoolSpikeTests {
    @Test func sequentialAndConcurrentEchoShareOnePGPooledConnection() async throws {
        let bin = HTTPBin(.http2(compress: false)) { _ in HTTPEchoHandler() }
        defer { #expect(throws: Never.self) { try bin.shutdown() } }
        let client = HTTPClient(eventLoopGroupProvider: .singleton, configuration: .pgSpike)
        let request = HTTPClientRequest.echo(port: bin.port)

        for _ in 0..<10 {
            try await Self.echo(request, on: client)
        }
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<100 {
                group.addTask { [request] in try await Self.echo(request, on: client) }
            }
            try await group.waitForAll()
        }

        #expect(client.poolManager.__forTesting_pgPoolCount == 1)
        #expect(bin.createdConnections == 1)
        try await client.shutdown()
    }

    private static func echo(_ request: HTTPClientRequest, on client: HTTPClient) async throws {
        let response = try await client.execute(request, deadline: .now() + .seconds(10))
        #expect(response.version == .http2)
        let body = try await response.body.collect(upTo: 1024)
        #expect(body == ByteBuffer(string: "1234"))
    }
}

extension HTTPClient.Configuration {
    fileprivate static var pgSpike: Self {
        var config = HTTPClient.Configuration()
        config.tlsConfiguration = .clientDefault
        config.tlsConfiguration?.certificateVerification = .none
        return config
    }
}

extension HTTPClientRequest {
    fileprivate static func echo(port: Int) -> Self {
        var request = HTTPClientRequest(url: "https://localhost:\(port)/")
        request.method = .POST
        request.body = .bytes(ByteBuffer(string: "1234"))
        return request
    }
}
