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

import InMemoryTracing
import NIOCore
import NIOHTTP1
import ServiceContextModule
import Testing
import Tracing

@testable @_spi(Tracing) import AsyncHTTPClient

@Suite("Tracing with the no-op tracer")
struct NoOpTracerTests {
    @Test func noOpTracerIsStillExposedButNotActive() {
        var configuration = HTTPClient.TracingConfiguration()
        configuration.tracer = NoOpTracer()

        #expect(configuration.tracer is NoOpTracer)
        withKnownIssue("activeTracer still returns NoOpTracer") {
            #expect(configuration.activeTracer == nil)
        }
    }

    @Test func missingTracerIsNotActive() {
        var configuration = HTTPClient.TracingConfiguration()
        configuration.tracer = nil

        #expect(configuration.activeTracer == nil)
    }

    @Test func recordingTracerIsActive() {
        var configuration = HTTPClient.TracingConfiguration()
        configuration.tracer = InMemoryTracer()

        #expect(configuration.activeTracer is InMemoryTracer)
    }

    @Test func requestSucceedsWithNoOpTracer() async throws {
        let httpBin = HTTPBin(.http1_1(ssl: false))
        defer { #expect(throws: Never.self) { try httpBin.shutdown() } }

        var configuration = HTTPClient.Configuration()
        configuration.tracing.tracer = NoOpTracer()
        let client = HTTPClient(eventLoopGroupProvider: .singleton, configuration: configuration)
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }

        let request = HTTPClientRequest(url: "http://localhost:\(httpBin.port)/echo-method")
        let response = try await client.execute(request, deadline: .distantFuture)

        #expect(response.status == .ok)
        #expect(client.tracer is NoOpTracer)
    }

    @Test func recordingTracerStillRecordsTheRequestSpan() async throws {
        let httpBin = HTTPBin(.http1_1(ssl: false))
        defer { #expect(throws: Never.self) { try httpBin.shutdown() } }

        let tracer = InMemoryTracer()
        var configuration = HTTPClient.Configuration()
        configuration.tracing.tracer = tracer
        let client = HTTPClient(eventLoopGroupProvider: .singleton, configuration: configuration)
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }

        let request = HTTPClientRequest(url: "http://localhost:\(httpBin.port)/echo-method")
        let response = try await client.execute(request, deadline: .distantFuture)

        #expect(response.status == .ok)
        #expect(tracer.activeSpans.isEmpty)
        let span = try #require(tracer.finishedSpans.first)
        #expect(tracer.finishedSpans.count == 1)
        #expect(span.operationName == "GET")
        #expect(span.attributes.get(client.tracing.attributeKeys.requestMethod) == "GET")
        #expect(span.attributes.get(client.tracing.attributeKeys.urlPath) == "/echo-method")
        #expect(span.attributes.get(client.tracing.attributeKeys.responseStatusCode) == 200)
    }

    /// `withSpan` would set the body's context to `.topLevel` even for `NoOpTracer`, so a `nil` context
    /// shows that `withRequestSpan` skipped the span.
    @Test func requestSpanIsSkippedForNoOpTracer() async {
        var configuration = HTTPClient.Configuration()
        configuration.tracing.tracer = NoOpTracer()
        let client = HTTPClient(eventLoopGroupProvider: .singleton, configuration: configuration)
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }

        var contextInBody: ServiceContext?
        _ = await client.withRequestSpan(HTTPClientRequest(url: "http://localhost/")) {
            contextInBody = ServiceContext.current
            return HTTPClientResponse()
        }

        withKnownIssue("withRequestSpan still opens a span for NoOpTracer") {
            #expect(contextInBody == nil)
        }
    }

    @Test func requestSpanIsCreatedForRecordingTracer() async {
        let tracer = InMemoryTracer()
        var configuration = HTTPClient.Configuration()
        configuration.tracing.tracer = tracer
        let client = HTTPClient(eventLoopGroupProvider: .singleton, configuration: configuration)
        defer { #expect(throws: Never.self) { try client.syncShutdown() } }

        var contextInBody: ServiceContext?
        _ = await client.withRequestSpan(HTTPClientRequest(url: "http://localhost/")) {
            contextInBody = ServiceContext.current
            return HTTPClientResponse()
        }

        #expect(contextInBody != nil)
        #expect(tracer.finishedSpans.count == 1)
    }
}
