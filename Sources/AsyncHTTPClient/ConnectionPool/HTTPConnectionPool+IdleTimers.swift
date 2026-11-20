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
    /// A loop gets its sweeper the first time one of its connections goes idle.
    ///
    /// A timeout that fired may reach the pool after the connection was reused and went idle again; `claim` tells the
    /// current timeout from such a stale one by its generation.
    struct IdleTimers {
        private let timeout: TimeAmount
        private var sweepers: [ObjectIdentifier: DeadlineSweeper<IdleTimeout>] = [:]
        private var active: [Connection.ID: Active] = [:]
        private var nextGeneration: UInt64 = 0

        init(timeout: TimeAmount) {
            self.timeout = timeout
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
            let sweeper = self.sweeper(for: eventLoop)
            let entryID = sweeper.insert(deadline: eventLoop.now + self.timeout, target: timeout, logger: logger)

            assert(self.active[connectionID] == nil)
            self.active[connectionID] = Active(generation: generation, sweeper: sweeper, entryID: entryID)
        }

        mutating func cancel(_ connectionID: Connection.ID) {
            guard let active = self.active.removeValue(forKey: connectionID) else {
                preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
            }
            active.sweeper.remove(active.entryID)
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
                active.sweeper.remove(active.entryID)
            }
            self.active.removeAll()
            for sweeper in self.sweepers.values {
                sweeper.shutDown()
            }
        }

        func __testOnly_isArmed(on eventLoop: any EventLoop) -> Bool {
            self.sweepers[ObjectIdentifier(eventLoop)]?.isArmed ?? false
        }

        private mutating func sweeper(for eventLoop: any EventLoop) -> DeadlineSweeper<IdleTimeout> {
            let key = ObjectIdentifier(eventLoop)
            if let sweeper = self.sweepers[key] {
                return sweeper
            }
            // The sweeper holds the loop, so its identifier isn't reused while the entry exists.
            let sweeper = DeadlineSweeper<IdleTimeout>(eventLoop: eventLoop)
            self.sweepers[key] = sweeper
            return sweeper
        }
    }
}

extension HTTPConnectionPool.IdleTimers {
    private struct Active {
        var generation: UInt64
        var sweeper: DeadlineSweeper<HTTPConnectionPool.IdleTimeout>
        var entryID: DeadlineSweeper<HTTPConnectionPool.IdleTimeout>.EntryID
    }
}
