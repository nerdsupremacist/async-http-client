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

protocol IdleTimeoutReceiver: AnyObject, Sendable {
    func idleTimeoutFired(_ timeout: HTTPConnectionPool.IdleTimeout)
}

extension HTTPConnectionPool {
    struct IdleTimeout: DeadlineTarget {
        let connectionID: Connection.ID
        let eventLoop: any EventLoop
        let generation: UInt64
        let receiver: any IdleTimeoutReceiver

        func deadlineExceeded() {
            self.receiver.idleTimeoutFired(self)
        }
    }

    /// The pool's connection idle timeouts, as entries on per-loop deadline sweepers. Guarded by the pool's state lock.
    ///
    /// A timeout that fired may reach the pool after the connection was reused and went idle again; `claim` tells the
    /// current timeout from such a stale one by its generation.
    struct IdleTimers {
        private let timeout: TimeAmount
        private let sweepers: [ObjectIdentifier: DeadlineSweeper<IdleTimeout>]
        private var active: [Connection.ID: Active] = [:]
        private var nextGeneration: UInt64 = 0

        init(eventLoops: some Sequence<any EventLoop>, timeout: TimeAmount) {
            self.timeout = timeout
            var sweepers: [ObjectIdentifier: DeadlineSweeper<IdleTimeout>] = [:]
            for eventLoop in eventLoops {
                sweepers[ObjectIdentifier(eventLoop)] = DeadlineSweeper(eventLoop: eventLoop)
            }
            self.sweepers = sweepers
        }

        mutating func schedule(
            _ connectionID: Connection.ID,
            on eventLoop: any EventLoop,
            receiver: any IdleTimeoutReceiver,
            logger: Logger
        ) {
            let generation = self.nextGeneration
            self.nextGeneration &+= 1
            let timeout = IdleTimeout(
                connectionID: connectionID,
                eventLoop: eventLoop,
                generation: generation,
                receiver: receiver
            )
            let cancellation: Active.Cancellation
            if let sweeper = self.sweepers[ObjectIdentifier(eventLoop)] {
                let deadline = eventLoop.now + self.timeout
                cancellation = .swept(sweeper, sweeper.insert(deadline: deadline, target: timeout, logger: logger))
            } else {
                // A loop the group didn't list in `makeIterator()`.
                cancellation = .task(eventLoop.scheduleTask(in: self.timeout) { timeout.deadlineExceeded() })
            }

            assert(self.active[connectionID] == nil)
            self.active[connectionID] = Active(generation: generation, cancellation: cancellation)
        }

        mutating func cancel(_ connectionID: Connection.ID) {
            guard let active = self.active.removeValue(forKey: connectionID) else {
                preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
            }
            active.cancel()
        }

        /// Returns whether `timeout` is the connection's current idle timeout, and if so, forgets it.
        mutating func claim(_ timeout: IdleTimeout) -> Bool {
            guard self.active[timeout.connectionID]?.generation == timeout.generation else {
                return false
            }
            self.active.removeValue(forKey: timeout.connectionID)
            return true
        }

        /// Drops every timeout, so no sweeper entry keeps the pool alive, and leaves nothing armed on a shared loop.
        mutating func shutDown() {
            for active in self.active.values {
                active.cancel()
            }
            self.active.removeAll()
            for sweeper in self.sweepers.values {
                sweeper.shutDown()
            }
        }

        func __testOnly_isArmed(on eventLoop: any EventLoop) -> Bool {
            self.sweepers[ObjectIdentifier(eventLoop)]?.isArmed ?? false
        }
    }
}

extension HTTPConnectionPool.IdleTimers {
    private struct Active {
        enum Cancellation {
            case swept(DeadlineSweeper<HTTPConnectionPool.IdleTimeout>, DeadlineSweeper<HTTPConnectionPool.IdleTimeout>.EntryID)
            case task(Scheduled<Void>)
        }

        var generation: UInt64
        var cancellation: Cancellation

        func cancel() {
            switch self.cancellation {
            case .swept(let sweeper, let id): sweeper.remove(id)
            case .task(let task): task.cancel()
            }
        }
    }
}
