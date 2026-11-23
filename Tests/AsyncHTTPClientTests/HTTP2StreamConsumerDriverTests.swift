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
import NIOHPACK
import NIOHTTP1
import NIOHTTP2
import XCTest

@testable import AsyncHTTPClient

/// Answers each request once it has ended: `:status 200` with END_STREAM, in one frame.
private struct RespondAtEndServer: NIOHTTP2ServerStreamConsumer {
    struct StreamState {}

    mutating func makeStreamState(for stream: inout NIOHTTP2Stream) -> StreamState { StreamState() }

    mutating func processStreams(_ streams: inout NIOHTTP2Streams<RespondAtEndServer>) {
        streams.forEachReady { stream, _ in
            while stream.nextInboundPart() != nil {}
            if stream.isInboundComplete, !stream.isOutboundComplete {
                try? stream.writeHeaders([":status": "200", "content-length": "0"])
                try? stream.writeEnd()
            }
        }
    }
}

/// AHC's HTTP/2 connection on nio-http2's stream consumer, against an in-memory server.
final class HTTP2StreamConsumerDriverTests: XCTestCase {
    private func interact(_ client: EmbeddedChannel, _ server: EmbeddedChannel) throws {
        var progressed = true
        while progressed {
            progressed = false
            client.embeddedEventLoop.run()
            for (from, to) in [(client, server), (server, client)] {
                while let data = try from.readOutbound(as: IOData.self) {
                    progressed = true
                    guard case .byteBuffer(let buffer) = data else { continue }
                    try to.writeInbound(buffer)
                }
            }
        }
    }

    func testBodySentIsReportedWhenTheResponseClosesTheStreamInTheSameVisit() throws {
        let client = EmbeddedChannel()
        let server = EmbeddedChannel()
        _ = try server.pipeline.syncOperations.configureHTTP2Connection { _ in RespondAtEndServer() }
        let address = try SocketAddress(ipAddress: "127.0.0.1", port: 3000)
        try client.connect(to: address).wait()
        try server.connect(to: address).wait()
        let connection = HTTP2Connection(
            channel: client,
            connectionID: 0,
            decompression: .disabled,
            maximumConnectionUses: nil,
            delegate: TestHTTP2ConnectionDelegate(),
            logger: Logger(label: "test"),
            streamMode: .connectionHandler
        )
        let started = connection._start0()
        try self.interact(client, server)
        XCTAssertNoThrow(try started.wait())

        let request = MockHTTPExecutableRequest(
            head: .init(version: .http1_1, method: .POST, uri: "/", headers: ["host": "localhost"]),
            framingMetadata: .init(connectionClose: false, body: .fixedSize(5)),
            raiseErrorIfUnimplementedMethodIsCalled: false
        )
        connection.sendableView.executeRequest(request)
        client.embeddedEventLoop.run()
        guard case .willExecuteRequest(let executor) = request.events.first else {
            return XCTFail("events: \(request.events.map(\.kind))")
        }
        executor.writeRequestBodyPart(.byteBuffer(ByteBuffer(string: "hello")), request: request, promise: nil)
        executor.finishRequestBodyStream(trailers: nil, request: request, promise: nil)
        // The END leaves in this flush; the response then arrives, with END_STREAM, in one read: that visit carries
        // `.localEndSent`, `.readable` and `.closed` together.
        try self.interact(client, server)

        for case .fail(let error) in request.events {
            XCTFail("request failed: \(error)")
        }
        let kinds = request.events.map(\.kind)
        XCTAssertTrue(kinds.contains(.receiveResponseEnd), "events: \(kinds)")
        XCTAssertTrue(kinds.contains(.requestBodySent), "events: \(kinds)")
        if let sent = kinds.firstIndex(of: .requestBodySent), let end = kinds.firstIndex(of: .receiveResponseEnd) {
            XCTAssertLessThan(sent, end, "the body is reported sent before the response ends")
        }
    }
}
