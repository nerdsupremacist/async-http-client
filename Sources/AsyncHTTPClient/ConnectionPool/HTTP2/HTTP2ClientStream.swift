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
import NIOHTTP2

/// One request on one HTTP/2 stream of an `HTTP2ClientConnectionHandler`.
///
/// A value in the handler's stream table. Its methods never call out: the handler mutates it in
/// place, ends the access, and only then talks to the request or the channel.
struct HTTP2ClientStream {
    let id: HTTP2StreamID
    let request: any HTTPExecutableRequest
    var machine: HTTPRequestStateMachine
    var decoder: HTTP2ClientResponseDecoder
    var inboundWindow: HTTP2InboundWindow
    var pendingReads = PendingReads()
    var writability: Writability

    /// The stream wants to read: frames are delivered at the next read complete, or right away on a
    /// later `read`. Mirrors `HTTP2StreamChannel.unsatisfiedRead`.
    var unsatisfiedRead = false
    var isListedForReads = false

    /// The last outbound frame, held so that END_STREAM can be folded into it (like the client codec).
    var heldWrite: HeldWrite?
    var isListedForHeldWrites = false

    var headersWritten = false
    var localEnded = false
    var remoteEnded = false
    /// nio-http2 has closed the stream, so it needs no RST_STREAM.
    var closedByNIO = false

    init(id: HTTP2StreamID, request: any HTTPExecutableRequest, parentIsWritable: Bool, targetWindowSize: Int32) {
        self.id = id
        self.request = request
        self.machine = HTTPRequestStateMachine(isChannelWritable: parentIsWritable)
        self.decoder = HTTP2ClientResponseDecoder(requestMethod: request.requestHead.method)
        self.inboundWindow = HTTP2InboundWindow(targetSize: targetWindowSize)
        self.writability = Writability(parentIsWritable: parentIsWritable)
    }

    /// RST_STREAM is needed when dropping the stream: something went out, and the stream isn't
    /// closed in both directions (or by nio-http2) yet.
    var needsReset: Bool {
        self.headersWritten && !self.closedByNIO && !(self.localEnded && self.remoteEnded)
    }
}

extension HTTP2ClientStream {
    struct HeldWrite {
        var payload: HTTP2Frame.FramePayload
        var promise: EventLoopPromise<Void>?
    }

    /// A FIFO of inbound frames that stores up to two frames inline (HEADERS + DATA for a small
    /// response) and only allocates for longer runs.
    struct PendingReads {
        private var first: HTTP2Frame.FramePayload?
        private var second: HTTP2Frame.FramePayload?
        private var rest: CircularBuffer<HTTP2Frame.FramePayload>?

        var isEmpty: Bool {
            self.first == nil && self.second == nil && (self.rest?.isEmpty ?? true)
        }

        mutating func append(_ payload: HTTP2Frame.FramePayload) {
            if let rest = self.rest, !rest.isEmpty {
                self.rest!.append(payload)
            } else if self.first == nil && self.second == nil {
                self.first = payload
            } else if self.second == nil && self.first != nil {
                self.second = payload
            } else {
                if self.rest == nil {
                    self.rest = CircularBuffer(initialCapacity: 4)
                }
                self.rest!.append(payload)
            }
        }

        mutating func popFirst() -> HTTP2Frame.FramePayload? {
            if let first = self.first {
                self.first = nil
                return first
            }
            if let second = self.second {
                self.second = nil
                return second
            }
            return self.rest?.popFirst()
        }
    }

    /// The child channel's writability: watermarks on bytes written but not yet completed, combined
    /// with the parent's writability (`HTTP2Connection.swift:113-114` today).
    struct Writability {
        static let highWatermark = 8196
        static let lowWatermark = 4092

        private var bufferedBytes = 0
        private var belowWatermark = true
        private var parentIsWritable: Bool

        init(parentIsWritable: Bool) {
            self.parentIsWritable = parentIsWritable
        }

        var isWritable: Bool {
            self.belowWatermark && self.parentIsWritable
        }

        /// Each returns the new writability only when it changed.
        mutating func bytesQueued(_ count: Int) -> Bool? {
            self.change {
                $0.bufferedBytes += count
                if $0.bufferedBytes > Self.highWatermark {
                    $0.belowWatermark = false
                }
            }
        }

        mutating func bytesWritten(_ count: Int) -> Bool? {
            self.change {
                $0.bufferedBytes -= count
                if $0.bufferedBytes <= Self.lowWatermark {
                    $0.belowWatermark = true
                }
            }
        }

        mutating func parentWritabilityChanged(_ isWritable: Bool) -> Bool? {
            self.change { $0.parentIsWritable = isWritable }
        }

        private mutating func change(_ body: (inout Self) -> Void) -> Bool? {
            let before = self.isWritable
            body(&self)
            let after = self.isWritable
            return before == after ? nil : after
        }
    }
}

@available(*, unavailable)
extension HTTP2ClientStream: Sendable {}
