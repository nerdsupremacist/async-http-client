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
import NIOPosix
import NIOSSL
import Testing

#if canImport(Network)
import NIOTransportServices
#endif

@testable import AsyncHTTPClient

@Suite("Deadline sweeper")
struct DeadlineSweeperTests {
    private let logger = Logger(label: "DeadlineSweeperTests")

    @Test func increasingDeadlinesArmOneTimer() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)

        for second in 1...10 {
            _ = sweeper.insert(
                deadline: loop.now + .seconds(Int64(second)),
                target: RecordingTarget(),
                logger: self.logger
            )
        }

        #expect(sweeper.armCount == 1)
        await loop.shutdownGracefully()
    }

    @Test func anEarlierDeadlineRearmsAndFiringRearmsForTheNextOne() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)
        let late = RecordingTarget()
        let early = RecordingTarget()

        _ = sweeper.insert(deadline: loop.now + .seconds(10), target: late, logger: self.logger)
        _ = sweeper.insert(deadline: loop.now + .seconds(5), target: early, logger: self.logger)
        #expect(sweeper.armCount == 2)

        await loop.advanceTime(by: .seconds(5))
        #expect(early.fireCount == 1)
        #expect(late.fireCount == 0)
        #expect(sweeper.armCount == 3)

        await loop.advanceTime(by: .seconds(5))
        #expect(early.fireCount == 1)
        #expect(late.fireCount == 1)
        #expect(sweeper.armCount == 3)
        await loop.shutdownGracefully()
    }

    @Test func removedEntriesNeverFire() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)
        let removed = RecordingTarget()
        let kept = RecordingTarget()

        let id = sweeper.insert(deadline: loop.now + .seconds(1), target: removed, logger: self.logger)
        _ = sweeper.insert(deadline: loop.now + .seconds(2), target: kept, logger: self.logger)
        sweeper.remove(id)

        await loop.advanceTime(by: .seconds(3))
        #expect(removed.fireCount == 0)
        #expect(kept.fireCount == 1)
        await loop.shutdownGracefully()
    }

    @Test func aStaleTimerLeavesTheCurrentOneArmed() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)
        let late = RecordingTarget()
        let early = RecordingTarget()

        // Generation 0 is armed for the late deadline, then replaced by generation 1 for the early one.
        _ = sweeper.insert(deadline: loop.now + .seconds(10), target: late, logger: self.logger)
        _ = sweeper.insert(deadline: loop.now + .seconds(5), target: early, logger: self.logger)

        // Generation 0 fires anyway, as if it raced its cancel: nothing has expired, and nothing is re-armed.
        sweeper.sweep(generation: 0)
        #expect(sweeper.armCount == 2)

        await loop.advanceTime(by: .seconds(5))
        #expect(early.fireCount == 1)
        #expect(late.fireCount == 0)
        await loop.shutdownGracefully()
    }

    @Test func anEmptySweepDisarmsUntilTheNextInsert() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)

        let id = sweeper.insert(deadline: loop.now + .seconds(1), target: RecordingTarget(), logger: self.logger)
        sweeper.remove(id)
        await loop.advanceTime(by: .seconds(2))
        #expect(sweeper.armCount == 1)

        let target = RecordingTarget()
        _ = sweeper.insert(deadline: loop.now + .seconds(1), target: target, logger: self.logger)
        #expect(sweeper.armCount == 2)
        await loop.advanceTime(by: .seconds(1))
        #expect(target.fireCount == 1)
        await loop.shutdownGracefully()
    }

    @Test func shutDownCancelsTheTimerAndNeverArmsAgain() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)
        let live = RecordingTarget()

        _ = sweeper.insert(deadline: loop.now + .seconds(1), target: live, logger: self.logger)
        sweeper.shutDown()
        #expect(!sweeper.isArmed)

        // Neither a later insert nor a fire that raced the shutdown arms again.
        let late = RecordingTarget()
        _ = sweeper.insert(deadline: loop.now + .seconds(1), target: late, logger: self.logger)
        sweeper.sweep(generation: 0)
        #expect(!sweeper.isArmed)
        #expect(sweeper.armCount == 1)

        await loop.advanceTime(by: .seconds(2))
        #expect(live.fireCount == 0)
        #expect(late.fireCount == 0)
        await loop.shutdownGracefully()
    }

    @Test func anInsertWithNothingArmedCoversTheEarliestEntry() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)
        let early = RecordingTarget()

        // As if arming the early deadline had thrown.
        _ = sweeper.insert(deadline: loop.now + .seconds(5), target: early, logger: self.logger)
        sweeper.disarm()
        _ = sweeper.insert(deadline: loop.now + .seconds(10), target: RecordingTarget(), logger: self.logger)

        await loop.advanceTime(by: .seconds(5))
        #expect(early.fireCount == 1)
        await loop.shutdownGracefully()
    }

    @Test func removedAndExpiredTargetsAreReleased() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)
        var removed: RecordingTarget? = RecordingTarget()
        var expired: RecordingTarget? = RecordingTarget()
        weak let weakRemoved = removed
        weak let weakExpired = expired

        let id = sweeper.insert(deadline: loop.now + .seconds(1), target: removed!, logger: self.logger)
        _ = sweeper.insert(deadline: loop.now + .seconds(1), target: expired!, logger: self.logger)
        removed = nil
        expired = nil
        sweeper.remove(id)
        #expect(weakRemoved == nil)

        await loop.advanceTime(by: .seconds(1))
        #expect(weakExpired == nil)
        await loop.shutdownGracefully()
    }

    @Test(arguments: LoopGroupKind.allCases)
    func clientShutdownLeavesNoTimerArmed(on kind: LoopGroupKind) async throws {
        let bin = HTTPBin(.http2(compress: false))
        defer { #expect(throws: Never.self) { try bin.shutdown() } }
        let group = kind.makeGroup()
        defer { #expect(throws: Never.self) { try group.syncShutdownGracefully() } }
        var config = HTTPClient.Configuration()
        config.tlsConfiguration = .clientDefault
        config.tlsConfiguration?.certificateVerification = .none
        let client = HTTPClient(eventLoopGroup: group, configuration: config)

        let response = try await client.execute(
            HTTPClientRequest(url: "https://localhost:\(bin.port)/get"),
            deadline: .now() + .hours(1),
            logger: self.logger
        )
        _ = try await response.body.collect(upTo: 1024 * 1024)
        // The finished request's deadline leaves the loop's timer armed.
        #expect(client.deadlineSweepers.values.contains { $0.isArmed })

        try await client.shutdown()
        #expect(client.deadlineSweepers.values.allSatisfy { !$0.isArmed })
    }

    @Test func aFireWithNothingArmedRearmsForTheRemainingDeadlines() async throws {
        let loop = NIOAsyncTestingEventLoop()
        let sweeper = DeadlineSweeper<RecordingTarget>(eventLoop: loop)
        let target = RecordingTarget()

        // As if arming had thrown after queueing: the entry is live, but nothing is recorded as armed.
        _ = sweeper.insert(deadline: loop.now + .seconds(5), target: target, logger: self.logger)
        sweeper.disarm()
        sweeper.sweep(generation: 0)
        #expect(sweeper.armCount == 2)

        await loop.advanceTime(by: .seconds(5))
        #expect(target.fireCount == 1)
        await loop.shutdownGracefully()
    }
}

/// NIOTS has no native `scheduleCallback`, so its timers go through NIOCore's default.
enum LoopGroupKind: CaseIterable, Sendable {
    case posix
    #if canImport(Network)
    case transportServices
    #endif

    func makeGroup() -> any EventLoopGroup {
        switch self {
        case .posix: MultiThreadedEventLoopGroup(numberOfThreads: 1)
        #if canImport(Network)
        case .transportServices: NIOTSEventLoopGroup(loopCount: 1)
        #endif
        }
    }
}

private final class RecordingTarget: DeadlineTarget {
    private let fired = NIOLockedValueBox(0)

    var fireCount: Int { self.fired.withLockedValue { $0 } }

    func deadlineExceeded() {
        self.fired.withLockedValue { $0 += 1 }
    }
}
