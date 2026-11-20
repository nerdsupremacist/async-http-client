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
import Testing

@testable import AsyncHTTPClient

@Suite("Connection idle timers")
struct IdleTimersTests {
    private let logger = Logger(label: "IdleTimersTests")

    @Test func firesAfterTheTimeoutAndIsClaimedOnce() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let receiver = RecordingReceiver()
        var timers = HTTPConnectionPool.IdleTimers(eventLoops: [loop], timeout: .seconds(60))

        timers.schedule(1, on: loop, receiver: receiver, logger: self.logger)
        await loop.advanceTime(by: .seconds(60))

        let fired = try #require(receiver.fired.first)
        #expect(receiver.fired.count == 1)
        #expect(fired.connectionID == 1)
        let first = timers.claim(fired)
        let second = timers.claim(fired)
        #expect(first)
        #expect(!second)
        await loop.shutdownGracefully()
    }

    @Test func aCancelledTimerDoesNotFire() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let receiver = RecordingReceiver()
        var timers = HTTPConnectionPool.IdleTimers(eventLoops: [loop], timeout: .seconds(60))

        timers.schedule(1, on: loop, receiver: receiver, logger: self.logger)
        await loop.advanceTime(by: .seconds(30))
        timers.cancel(1)
        await loop.advanceTime(by: .seconds(60))

        #expect(receiver.fired.isEmpty)
        await loop.shutdownGracefully()
    }

    @Test func aReusedConnectionTimesOutAtItsSecondIdleDeadline() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let receiver = RecordingReceiver()
        var timers = HTTPConnectionPool.IdleTimers(eventLoops: [loop], timeout: .seconds(60))

        timers.schedule(1, on: loop, receiver: receiver, logger: self.logger)
        await loop.advanceTime(by: .seconds(30))
        timers.cancel(1)
        timers.schedule(1, on: loop, receiver: receiver, logger: self.logger)
        await loop.advanceTime(by: .seconds(40))
        #expect(receiver.fired.isEmpty)
        await loop.advanceTime(by: .seconds(20))

        #expect(receiver.fired.count == 1)
        await loop.shutdownGracefully()
    }

    @Test func aFireThatLostTheRaceToAReuseIsNotClaimed() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let receiver = RecordingReceiver()
        var timers = HTTPConnectionPool.IdleTimers(eventLoops: [loop], timeout: .seconds(60))

        timers.schedule(1, on: loop, receiver: receiver, logger: self.logger)
        await loop.advanceTime(by: .seconds(60))
        // The pool reused the connection and it went idle again before the fire took the pool's lock.
        timers.cancel(1)
        timers.schedule(1, on: loop, receiver: receiver, logger: self.logger)

        let stale = try #require(receiver.fired.first)
        let claimedStale = timers.claim(stale)
        #expect(!claimedStale)
        await loop.advanceTime(by: .seconds(60))
        let current = try #require(receiver.fired.last)
        #expect(receiver.fired.count == 2)
        let claimedCurrent = timers.claim(current)
        #expect(claimedCurrent)
        await loop.shutdownGracefully()
    }

    @Test func shutDownLeavesNothingArmed() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let receiver = RecordingReceiver()
        var timers = HTTPConnectionPool.IdleTimers(eventLoops: [loop], timeout: .seconds(60))

        timers.schedule(1, on: loop, receiver: receiver, logger: self.logger)
        timers.shutDown()
        await loop.advanceTime(by: .seconds(60))

        #expect(receiver.fired.isEmpty)
        #expect(!timers.__testOnly_isArmed(on: loop))
        await loop.shutdownGracefully()
    }

    @Test func shutDownReleasesTheReceiver() async throws {
        let loop = NIOAsyncTestingEventLoop()
        var timers = HTTPConnectionPool.IdleTimers(eventLoops: [loop], timeout: .seconds(60))
        weak var weakReceiver: RecordingReceiver?
        do {
            let receiver = RecordingReceiver()
            weakReceiver = receiver
            timers.schedule(1, on: loop, receiver: receiver, logger: self.logger)
        }
        // The sweeper entry holds the receiver, as it holds the pool.
        #expect(weakReceiver != nil)

        timers.shutDown()

        #expect(weakReceiver == nil)
        await loop.shutdownGracefully()
    }

    @Test func aLoopWithoutASweeperStillTimesOut() async throws {
        let listed = NIOAsyncTestingEventLoop()
        let unlisted = NIOAsyncTestingEventLoop()
        let receiver = RecordingReceiver()
        var timers = HTTPConnectionPool.IdleTimers(eventLoops: [listed], timeout: .seconds(60))

        timers.schedule(1, on: unlisted, receiver: receiver, logger: self.logger)
        await unlisted.advanceTime(by: .seconds(60))

        let fired = try #require(receiver.fired.first)
        let claimed = timers.claim(fired)
        #expect(claimed)
        await listed.shutdownGracefully()
        await unlisted.shutdownGracefully()
    }
}

private final class RecordingReceiver: IdleTimeoutReceiver {
    private let timeouts = NIOLockedValueBox<[HTTPConnectionPool.IdleTimeout]>([])

    var fired: [HTTPConnectionPool.IdleTimeout] { self.timeouts.withLockedValue { $0 } }

    func idleTimeoutFired(_ timeout: HTTPConnectionPool.IdleTimeout) {
        self.timeouts.withLockedValue { $0.append(timeout) }
    }
}
