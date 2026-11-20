//===----------------------------------------------------------------------===//
//
// This source file is part of the AsyncHTTPClient open source project
//
// Copyright (c) 2021 Apple Inc. and the AsyncHTTPClient project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of AsyncHTTPClient project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Atomics
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOHTTP1

extension HTTPConnectionPool {
    final class Manager: Sendable {
        private typealias Key = ConnectionPool.Key

        private enum RunState: Sendable {
            case active
            case shuttingDown(promise: EventLoopPromise<Bool>?, unclean: Bool)
            case shutDown
        }

        private struct State: Sendable {
            var runState: RunState = .active
            var pools: [Key: HTTPConnectionPool] = [:]
            var pgPools: [Key: any PGH2PoolSpike] = [:]
        }

        // Spike switch: async-API requests to https hosts go to postgres-nio's pool, which only speaks HTTP/2.
        private static let routeHTTPSThroughPGPool = true

        private let eventLoopGroup: EventLoopGroup
        private let configuration: HTTPClient.Configuration
        private let connectionIDGenerator = Connection.ID.globalGenerator
        private let logger: Logger

        private let state: NIOLockedValueBox<State> = NIOLockedValueBox(State())
        private let sslContextCache = SSLContextCache()

        init(
            eventLoopGroup: EventLoopGroup,
            configuration: HTTPClient.Configuration,
            backgroundActivityLogger logger: Logger
        ) {
            self.eventLoopGroup = eventLoopGroup
            self.configuration = configuration
            self.logger = logger
        }

        func executeRequest(_ request: HTTPSchedulableRequest) {
            let poolKey = request.poolKey
            if Self.routeHTTPSThroughPGPool, poolKey.scheme == .https,
                #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *),
                let transaction = request as? Transaction
            {
                return self.executeOnPGPool(transaction)
            }

            let poolResult = self.state.withLockedValue { state -> Result<HTTPConnectionPool, HTTPClientError> in
                switch state.runState {
                case .active:
                    if let pool = state.pools[poolKey] {
                        return .success(pool)
                    }

                    let pool = HTTPConnectionPool(
                        eventLoopGroup: self.eventLoopGroup,
                        sslContextCache: self.sslContextCache,
                        tlsConfiguration: request.tlsConfiguration,
                        clientConfiguration: self.configuration,
                        key: poolKey,
                        delegate: self,
                        idGenerator: self.connectionIDGenerator,
                        backgroundActivityLogger: self.logger
                    )
                    state.pools[poolKey] = pool
                    return .success(pool)

                case .shuttingDown, .shutDown:
                    return .failure(HTTPClientError.alreadyShutdown)
                }
            }

            switch poolResult {
            case .success(let pool):
                pool.executeRequest(request)
            case .failure(let error):
                request.fail(error)
            }
        }

        @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
        private func executeOnPGPool(_ transaction: Transaction) {
            let poolKey = transaction.poolKey
            let pool = self.state.withLockedValue { state -> (any PGH2PoolSpike)? in
                guard case .active = state.runState else { return nil }
                if let pool = state.pgPools[poolKey] {
                    return pool
                }
                let pool = PGH2Pool(
                    key: poolKey,
                    tlsConfiguration: transaction.tlsConfiguration,
                    clientConfiguration: self.configuration,
                    sslContextCache: self.sslContextCache,
                    eventLoopGroup: self.eventLoopGroup,
                    logger: self.logger
                )
                state.pgPools[poolKey] = pool
                return pool
            }
            guard let pool else {
                return transaction.fail(HTTPClientError.alreadyShutdown)
            }
            pool.executeRequest(transaction)
        }

        var __forTesting_pgPoolCount: Int {
            self.state.withLockedValue { $0.pgPools.count }
        }

        /// Shutdown the connection pool manager. You **must** shutdown the pool manager, since it leak otherwise.
        ///
        /// - Parameter promise: An `EventLoopPromise` that is succeeded once all connections pools are shutdown.
        /// - Returns: An EventLoopFuture that is succeeded once the pool is shutdown. The bool indicates if the
        ///            shutdown was unclean.
        func shutdown(promise: EventLoopPromise<Bool>?) {
            enum ShutdownAction {
                case done(EventLoopPromise<Bool>?)
                case shutdown([Key: HTTPConnectionPool])
            }

            // The PG pools shut down asynchronously; the caller's promise waits for them and for the AHC pools.
            var pgPools: [any PGH2PoolSpike] = []
            var poolsPromise = promise

            let action = self.state.withLockedValue { state -> ShutdownAction in
                switch state.runState {
                case .active:
                    pgPools = Array(state.pgPools.values)
                    state.pgPools = [:]
                    if !pgPools.isEmpty {
                        poolsPromise = self.eventLoopGroup.any().makePromise(of: Bool.self)
                    }
                    // If there aren't any pools, we can mark the pool as shut down right away.
                    if state.pools.isEmpty {
                        state.runState = .shutDown
                        return .done(poolsPromise)
                    } else {
                        // this promise will be succeeded once all connection pools are shutdown
                        state.runState = .shuttingDown(promise: poolsPromise, unclean: false)
                        return .shutdown(state.pools)
                    }

                case .shuttingDown, .shutDown:
                    preconditionFailure("PoolManager already shutdown")
                }
            }

            // if no pools are returned, the manager is already shutdown completely. Inform the
            // delegate. This is a very clean shutdown...
            switch action {
            case .done(let promise):
                promise?.succeed(false)

            case .shutdown(let pools):
                for pool in pools.values {
                    pool.shutdown()
                }
            }

            if let poolsPromise, !pgPools.isEmpty {
                // Unstructured on purpose: shutdown(promise:) is synchronous and the PG pools stop asynchronously.
                Task { [pgPools] in
                    for pool in pgPools {
                        await pool.shutdown()
                    }
                    let unclean = (try? await poolsPromise.futureResult.get()) ?? true
                    promise?.succeed(unclean)
                }
            }
        }
    }
}

extension HTTPConnectionPool.Manager: HTTPConnectionPoolDelegate {
    func connectionPoolDidShutdown(_ pool: HTTPConnectionPool, unclean: Bool) {
        enum CloseAction {
            case close(EventLoopPromise<Bool>?, unclean: Bool)
            case wait
        }

        let closeAction = self.state.withLockedValue { state -> CloseAction in
            switch state.runState {
            case .active, .shutDown:
                preconditionFailure("Why are pools shutting down, if the manager did not give a signal")

            case .shuttingDown(let promise, let soFarUnclean):
                guard state.pools.removeValue(forKey: pool.key) === pool else {
                    preconditionFailure(
                        "Expected that the pool was created by this manager and is known for this reason."
                    )
                }

                if state.pools.isEmpty {
                    state.runState = .shutDown
                    return .close(promise, unclean: soFarUnclean || unclean)
                } else {
                    state.runState = .shuttingDown(promise: promise, unclean: soFarUnclean || unclean)
                    return .wait
                }
            }
        }

        switch closeAction {
        case .close(let promise, let unclean):
            promise?.succeed(unclean)
        case .wait:
            break
        }
    }
}

extension HTTPConnectionPool.Connection.ID {
    static let globalGenerator = Generator()

    struct Generator {
        private let atomic: ManagedAtomic<Int>

        init() {
            self.atomic = .init(0)
        }

        func next() -> Int {
            self.atomic.loadThenWrappingIncrement(ordering: .relaxed)
        }
    }
}
