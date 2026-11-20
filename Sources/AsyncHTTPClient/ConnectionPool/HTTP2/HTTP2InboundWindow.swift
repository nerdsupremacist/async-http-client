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

/// Decides when to send an inbound WINDOW_UPDATE: once the window, counting bytes that arrived but
/// haven't been delivered yet as still in use, drops to half of the target or below.
///
/// A copy of swift-nio-http2's internal `InboundWindowManager`, so the channel-less path keeps the
/// policy the child channels and the multiplexer use today.
struct HTTP2InboundWindow {
    private var targetWindowSize: Int32
    private var lastWindowSize: Int?
    private var bufferedBytes: Int = 0
    var closed = false

    init(targetSize: Int32) {
        assert(targetSize >= 0)
        self.targetWindowSize = targetSize
    }

    mutating func newWindowSize(_ newSize: Int) -> Int? {
        self.lastWindowSize = newSize
        return self.calculateWindowIncrement(windowSize: newSize + self.bufferedBytes)
    }

    mutating func initialWindowSizeChanged(delta: Int) -> Int? {
        self.targetWindowSize += Int32(delta)
        if let lastWindowSize = self.lastWindowSize {
            return self.newWindowSize(lastWindowSize + delta)
        } else {
            return nil
        }
    }

    mutating func bufferedFrameReceived(size: Int) {
        self.bufferedBytes += size
    }

    mutating func bufferedFrameEmitted(size: Int) -> Int? {
        self.bufferedBytes -= size
        assert(self.bufferedBytes >= 0)
        guard let lastWindowSize = self.lastWindowSize else {
            return nil
        }
        return self.calculateWindowIncrement(windowSize: lastWindowSize + self.bufferedBytes)
    }

    private func calculateWindowIncrement(windowSize: Int) -> Int? {
        if self.closed {
            return nil
        }

        if windowSize >= self.targetWindowSize {
            return nil
        } else if windowSize >= 0 {
            let increment = self.targetWindowSize - Int32(windowSize)
            if increment >= windowSize {
                return Int(increment)
            } else {
                return nil
            }
        } else {
            let windowSize = Int64(windowSize)
            let targetWindowSize = Int64(self.targetWindowSize)
            let increment = min(abs(windowSize) + targetWindowSize, Int64(Int32.max))
            return Int(increment)
        }
    }
}
