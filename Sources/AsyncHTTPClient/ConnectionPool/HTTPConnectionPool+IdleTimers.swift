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

    struct IdleTimers {
        init(eventLoops: some Sequence<any EventLoop>, timeout: TimeAmount) {}

        mutating func schedule(
            _ connectionID: Connection.ID,
            on eventLoop: any EventLoop,
            receiver: any IdleTimeoutReceiver,
            logger: Logger
        ) {}

        mutating func cancel(_ connectionID: Connection.ID) {}

        mutating func claim(_ timeout: IdleTimeout) -> Bool {
            false
        }

        mutating func shutDown() {}

        func isArmed(on eventLoop: any EventLoop) -> Bool {
            false
        }
    }
}
