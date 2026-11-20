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
import NIOSSL
import _ConnectionPoolModule

// Measurement spike: HTTP/2 requests from the async API run through postgres-nio's `ConnectionPool`.
// HTTP/1, ALPN migration, event-loop affinity, backoff, queued-request cancellation and deadlines are left out.

protocol PGH2PoolSpike: Sendable {
    @available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
    func executeRequest(_ transaction: Transaction)
    func shutdown() async
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
typealias PGH2ConnectionPool = _ConnectionPoolModule.ConnectionPool<
    PGH2Connection,
    HTTPConnectionPool.Connection.ID,
    _ConnectionPoolModule.ConnectionIDGenerator,
    Transaction,
    ObjectIdentifier,
    NoOpKeepAliveBehavior<PGH2Connection>,
    NoOpConnectionPoolMetrics<HTTPConnectionPool.Connection.ID>,
    ContinuousClock
>

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PGH2Pool: PGH2PoolSpike {
    private let pool: PGH2ConnectionPool
    private let runTask: Task<Void, Never>

    init(
        key: ConnectionPool.Key,
        tlsConfiguration: TLSConfiguration?,
        clientConfiguration: HTTPClient.Configuration,
        sslContextCache: SSLContextCache,
        eventLoopGroup: any EventLoopGroup,
        logger: Logger
    ) {
        let factory = HTTPConnectionPool.ConnectionFactory(
            key: key,
            tlsConfiguration: tlsConfiguration,
            clientConfiguration: clientConfiguration,
            sslContextCache: sslContextCache
        )
        let connectTimeout = clientConfiguration.timeout.connect ?? .seconds(30)

        // One connection per key, like AHC's H2 pool with an indifferent event-loop requirement. A minimum of zero
        // keeps the idle timer, so its per-request cost shows in the sequential cells.
        var configuration = ConnectionPoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.idleTimeout = .nanoseconds(clientConfiguration.connectionPool.idleTimeout.nanoseconds)

        let pool = PGH2ConnectionPool(
            configuration: configuration,
            idGenerator: _ConnectionPoolModule.ConnectionIDGenerator(),
            requestType: Transaction.self,
            keepAliveBehavior: NoOpKeepAliveBehavior(connectionType: PGH2Connection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: HTTPConnectionPool.Connection.ID.self),
            clock: ContinuousClock()
        ) { connectionID, pool in
            try await withCheckedThrowingContinuation { continuation in
                let connection = PGH2Connection(id: connectionID, pool: pool)
                factory.makeConnection(
                    for: PGH2Requester(connection: connection, continuation: continuation),
                    connectionID: connectionID,
                    http1ConnectionDelegate: PGH2NoHTTP1Delegate(),
                    http2ConnectionDelegate: connection,
                    deadline: .now() + connectTimeout,
                    eventLoop: eventLoopGroup.next(),
                    logger: logger
                )
            }
        }
        self.pool = pool
        // Unstructured on purpose: HTTPClient has no structured scope to run the pool in. shutdown() awaits it.
        self.runTask = Task { await pool.run() }
    }

    func executeRequest(_ transaction: Transaction) {
        self.pool.leaseConnection(transaction)
    }

    func shutdown() async {
        self.pool.triggerForceShutdown()
        await self.runTask.value
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
@usableFromInline
final class PGH2Connection: PooledConnection, HTTP2ConnectionDelegate, @unchecked Sendable {
    @usableFromInline let id: Int
    private let pool: PGH2ConnectionPool
    // Written once on the event loop before the factory returns. The pool hands the connection out only after the
    // factory's result crossed a task boundary, which orders that write before every read.
    fileprivate(set) var view: HTTP2Connection.SendableView!

    init(id: HTTPConnectionPool.Connection.ID, pool: PGH2ConnectionPool) {
        self.id = id
        self.pool = pool
    }

    @usableFromInline func onClose(_ closure: @escaping @Sendable ((any Error)?) -> Void) {
        self.view.closeFuture.whenComplete { _ in closure(nil) }
    }

    @usableFromInline func close() {
        self.view.close(promise: nil)
    }

    func http2Connection(_: HTTPConnectionPool.Connection.ID, newMaxStreamSetting: Int) {
        self.pool.connectionReceivedNewMaxStreamSetting(
            self,
            newMaxStreamSetting: UInt16(clamping: newMaxStreamSetting)
        )
    }

    func http2ConnectionStreamClosed(_: HTTPConnectionPool.Connection.ID, availableStreams: Int) {
        self.pool.releaseConnection(self, streams: 1)
    }

    func http2ConnectionGoAwayReceived(_: HTTPConnectionPool.Connection.ID) {
        self.pool.connectionWillClose(self.id)
    }

    func http2ConnectionClosed(_: HTTPConnectionPool.Connection.ID) {
        // The pool learns about closes through `onClose`.
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension Transaction: ConnectionRequestProtocol {
    @usableFromInline
    var id: ObjectIdentifier { ObjectIdentifier(self) }

    @usableFromInline
    func complete(with result: Result<ConnectionLease<PGH2Connection>, ConnectionPoolError>) {
        switch result {
        case .success(let lease):
            lease.connection.view.executeRequest(self)
        case .failure(let error):
            self.fail(error)
        }
    }
}

struct PGH2SpikeHTTP1NotSupportedError: Error {}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private struct PGH2Requester: HTTPConnectionRequester {
    let connection: PGH2Connection
    let continuation: CheckedContinuation<ConnectionAndMetadata<PGH2Connection>, any Error>

    func http1ConnectionCreated(_ view: HTTP1Connection.SendableView) {
        view.close(promise: nil)
        self.continuation.resume(throwing: PGH2SpikeHTTP1NotSupportedError())
    }

    func http2ConnectionCreated(_ view: HTTP2Connection.SendableView, maximumStreams: Int) {
        self.connection.view = view
        self.continuation.resume(
            returning: ConnectionAndMetadata(
                connection: self.connection,
                maximalStreamsOnConnection: UInt16(clamping: maximumStreams)
            )
        )
    }

    func failedToCreateHTTPConnection(_: HTTPConnectionPool.Connection.ID, error: any Error) {
        self.continuation.resume(throwing: error)
    }

    func waitingForConnectivity(_: HTTPConnectionPool.Connection.ID, error: any Error) {}
}

private struct PGH2NoHTTP1Delegate: HTTP1ConnectionDelegate {
    func http1ConnectionReleased(_: HTTPConnectionPool.Connection.ID) {}
    func http1ConnectionClosed(_: HTTPConnectionPool.Connection.ID) {}
}
