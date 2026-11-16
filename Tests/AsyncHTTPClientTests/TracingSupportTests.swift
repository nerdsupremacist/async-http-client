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

import NIOConcurrencyHelpers
import NIOHTTP1
import Testing
import Tracing

@testable import AsyncHTTPClient

@Suite struct TracingSupportTests {
    private static let url = "https://user:secret@example.com:8443/some/path?query=1"

    @Test func asyncRequestAttributesAreSetOnRecordingSpan() {
        let span = MutationCountingSpan(isRecording: true)
        let request = HTTPClientRequest(url: Self.url)

        TracingSupport.handleRequestTracingAttributes(span, request, configuration: .init())

        #expect(span.mutations > 0)
    }
}

private final class MutationCountingSpan: Span {
    let context = ServiceContext.topLevel
    let isRecording: Bool
    private let mutationCount = NIOLockedValueBox(0)

    var mutations: Int {
        self.mutationCount.withLockedValue { $0 }
    }

    init(isRecording: Bool) {
        self.isRecording = isRecording
    }

    var operationName: String {
        get { "test" }
        set {}
    }

    var attributes: SpanAttributes {
        get { [:] }
        set { self.mutationCount.withLockedValue { $0 += 1 } }
    }

    func setStatus(_ status: SpanStatus) {
        self.mutationCount.withLockedValue { $0 += 1 }
    }

    func addEvent(_ event: SpanEvent) {}

    func recordError<Instant: TracerInstant>(
        _ error: Error,
        attributes: SpanAttributes,
        at instant: @autoclosure () -> Instant
    ) {}

    func addLink(_ link: SpanLink) {}

    func end<Instant: TracerInstant>(at instant: @autoclosure () -> Instant) {}
}
