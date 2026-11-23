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

/// One request on one HTTP/2 stream of an `HTTP2ClientStreamDriver`. Event loop only.
///
/// Also the stream's consumer state in nio-http2's slot (a reference, so the 3-word executor can hold it). Its
/// methods never call out; the driver changes it, then talks to the request or the connection, then checks
/// `isActive` again.
final class HTTP2ClientStream {
    let request: any HTTPExecutableRequest
    /// Strong: a late executor call can outlive the connection. The cycle breaks on removal.
    let driver: HTTP2ClientStreamDriver
    /// Set once nio-http2 opened the stream.
    var handle: NIOHTTP2StreamHandle?
    var streamID = HTTP2StreamID.rootStream
    /// In the driver's list. Cleared when the stream is removed; stale executor calls check it.
    var isActive = true
    var listIndex = 0
    var machine: HTTPRequestStateMachine
    var decoder: HTTP2ClientResponseDecoder
    var pendingReads = PendingReads()

    /// Events from the last visit, handled once the slot access has ended.
    var pendingEvents: NIOHTTP2StreamEvents = []
    var isListedForEvents = false
    var closeReason: NIOHTTP2StreamCloseReason?

    /// The stream wants to read: parts are taken from nio-http2 right away. Mirrors `HTTP2StreamChannel.unsatisfiedRead`.
    var unsatisfiedRead = false
    /// A visit left parts in nio-http2's slot (the stream wasn't reading): `read` takes them with an access.
    var partsInSlot = false
    var isWritable = true
    /// `sendRequestEnd` ran: the END's `.localEndSent` means `requestBodyStreamSent`.
    var awaitingBodySent = false
    /// Counted by the idle state machine as an open stream.
    var countedByIdle = false

    var headersWritten = false
    var localEnded = false
    var remoteEnded = false
    /// nio-http2 has closed the stream, so it needs no RST_STREAM.
    var closedByNIO = false

    init(request: any HTTPExecutableRequest, driver: HTTP2ClientStreamDriver, parentIsWritable: Bool) {
        self.request = request
        self.driver = driver
        self.machine = HTTPRequestStateMachine(isChannelWritable: parentIsWritable)
        self.decoder = HTTP2ClientResponseDecoder(requestMethod: request.requestHead.method)
        self.isWritable = parentIsWritable
    }

    /// RST_STREAM is needed when dropping the stream: something went out, and the stream isn't closed in both
    /// directions (or by nio-http2) yet.
    var needsReset: Bool {
        self.headersWritten && !self.closedByNIO && !(self.localEnded && self.remoteEnded)
    }
}

extension HTTP2ClientStream {
    /// A FIFO of inbound parts that stores up to two inline (HEADERS + DATA for a small response) and only allocates
    /// for longer runs.
    struct PendingReads {
        private var first: NIOHTTP2InboundPart?
        private var second: NIOHTTP2InboundPart?
        private var rest: CircularBuffer<NIOHTTP2InboundPart>?

        var isEmpty: Bool {
            self.first == nil && self.second == nil && (self.rest?.isEmpty ?? true)
        }

        mutating func append(_ part: NIOHTTP2InboundPart) {
            if let rest = self.rest, !rest.isEmpty {
                self.rest!.append(part)
            } else if self.first == nil && self.second == nil {
                self.first = part
            } else if self.second == nil && self.first != nil {
                self.second = part
            } else {
                if self.rest == nil {
                    self.rest = CircularBuffer(initialCapacity: 4)
                }
                self.rest!.append(part)
            }
        }

        mutating func popFirst() -> NIOHTTP2InboundPart? {
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
}

@available(*, unavailable)
extension HTTP2ClientStream: Sendable {}
