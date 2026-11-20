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

protocol DeadlineTarget: Sendable {
    func deadlineExceeded()
}

/// The deadlines of one event loop, behind a single armed `scheduleCallback`.
///
/// NIO cancels a timer with a linear scan of the loop's timer heap, and scheduling or cancelling from off the loop can
/// wake it. So rather than one timer per deadline, each loop keeps one timer, armed for the earliest deadline it knows
/// of. Removing a deadline only drops its entry; the timer isn't cancelled for it. When the timer fires, it notifies
/// the expired targets and re-arms for the earliest remaining deadline, so a loop with steady traffic fires about once
/// per timeout.
///
/// Costs: insert and remove are dictionary operations. A deadline earlier than the armed one re-arms (a cancel plus a
/// schedule), so deadlines arriving in decreasing order cost what one timer each would. Each fire scans the dictionary
/// under the lock, and its capacity doesn't shrink after a burst.
///
/// Locking: `scheduleCallback` and `cancel` run while the sweeper's lock is held, so the order is sweeper lock, then the
/// loop's own task lock. That assumes `scheduleCallback` never waits for the loop, which holds for NIOPosix, NIOTS and
/// `EmbeddedEventLoop`, but not for `NIOAsyncTestingEventLoop` called off the loop: tests mustn't insert while the loop
/// runs a sweep. NIO calls back into the sweeper only through `handleScheduledCallback`, without its lock held.
/// NIOPosix calls `didCancelScheduledCallback` with its task lock held, so `SweepHandler` must never implement it in a
/// way that touches the sweeper.
final class DeadlineSweeper<Target: DeadlineTarget>: Sendable {
    typealias EntryID = UInt64

    let eventLoop: any EventLoop
    private let state = NIOLockedValueBox(State())

    init(eventLoop: any EventLoop) {
        self.eventLoop = eventLoop
    }

    /// The number of attempts to arm a timer, including any that threw.
    var armCount: UInt64 {
        self.state.withLockedValue { $0.nextGeneration }
    }

    var isArmed: Bool {
        self.state.withLockedValue { $0.armed != nil }
    }

    func insert(deadline: NIODeadline, target: Target, logger: Logger) -> EntryID {
        self.state.withLockedValue { state in
            let id = state.nextID
            state.nextID &+= 1
            state.entries[id] = Entry(deadline: deadline, target: target)
            if state.isShutDown {
                return id
            }
            if let armed = state.armed {
                if armed.deadline <= deadline {
                    return id
                }
                armed.callback.cancel()
                state.armed = nil
                self.arm(at: deadline, state: &state, logger: logger)
            } else {
                // Nothing armed with other entries live means an arm threw: cover the earliest of them too.
                let earliest = state.entries.count == 1 ? deadline : state.entries.values.lazy.map(\.deadline).min()!
                self.arm(at: earliest, state: &state, logger: logger)
            }
            return id
        }
    }

    func remove(_ id: EntryID) {
        self.state.withLockedValue { state in
            _ = state.entries.removeValue(forKey: id)
        }
    }

    /// Cancels the armed timer and never arms another, so a client that shut down leaves nothing on a shared loop.
    func shutDown() {
        self.state.withLockedValue { state in
            state.isShutDown = true
            state.armed?.callback.cancel()
            state.armed = nil
        }
    }

    /// Cancels the armed timer, as if arming had thrown; the next insert or fire arms again.
    func disarm() {
        self.state.withLockedValue { state in
            state.armed?.callback.cancel()
            state.armed = nil
        }
    }

    /// Notifies the expired targets and, if `generation` is the armed timer's, re-arms for the earliest live deadline.
    func sweep(generation: UInt64) {
        let now = self.eventLoop.now
        let expired = self.state.withLockedValue { state -> [(EntryID, Target)] in
            var expired: [(EntryID, Target)] = []
            var earliest: NIODeadline?
            for (id, entry) in state.entries {
                if entry.deadline <= now {
                    expired.append((id, entry.target))
                } else if earliest.map({ entry.deadline < $0 }) ?? true {
                    earliest = entry.deadline
                }
            }
            for (id, _) in expired {
                state.entries.removeValue(forKey: id)
            }
            // A timer that was replaced while it fired leaves the current one alone. With nothing armed (arming threw),
            // this fire is the only one left, so it re-arms.
            if state.armed == nil || state.armed?.generation == generation {
                state.armed = nil
                if let earliest, !state.isShutDown {
                    self.arm(at: earliest, state: &state, logger: nil)
                }
            }
            return expired
        }
        for (_, target) in expired {
            target.deadlineExceeded()
        }
    }

    private func arm(at deadline: NIODeadline, state: inout State, logger: Logger?) {
        let generation = state.nextGeneration
        state.nextGeneration &+= 1
        do {
            let callback = try self.eventLoop.scheduleCallback(
                at: deadline,
                handler: SweepHandler(sweeper: self, generation: generation)
            )
            state.armed = Armed(deadline: deadline, generation: generation, callback: callback)
        } catch {
            // NIOPosix throws only if waking the loop fails, after queueing: that timer still fires, and the next
            // insert arms another.
            logger?.debug("failed to schedule the deadline sweep", metadata: ["ahc-error": "\(error)"])
        }
    }
}

extension DeadlineSweeper {
    private struct Entry {
        var deadline: NIODeadline
        var target: Target
    }

    private struct Armed {
        var deadline: NIODeadline
        var generation: UInt64
        var callback: NIOScheduledCallback
    }

    private struct State {
        var nextID: EntryID = 0
        var nextGeneration: UInt64 = 0
        var entries: [EntryID: Entry] = [:]
        var armed: Armed?
        var isShutDown = false
    }
}

private struct SweepHandler<Target: DeadlineTarget>: NIOScheduledCallbackHandler, Sendable {
    let sweeper: DeadlineSweeper<Target>
    let generation: UInt64

    func handleScheduledCallback(eventLoop: some EventLoop) {
        self.sweeper.sweep(generation: self.generation)
    }
}
