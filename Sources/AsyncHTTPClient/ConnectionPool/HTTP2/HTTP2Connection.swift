//===----------------------------------------------------------------------===//
//
// This source file is part of the AsyncHTTPClient open source project
//
// Copyright (c) 2021 Apple Inc. and the AsyncHTTPClient project authors
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
@_spi(Spike) import NIOHTTP2
import NIOHTTPCompression

protocol HTTP2ConnectionDelegate: Sendable {
    func http2Connection(_: HTTPConnectionPool.Connection.ID, newMaxStreamSetting: Int)
    func http2ConnectionStreamClosed(_: HTTPConnectionPool.Connection.ID, availableStreams: Int)
    func http2ConnectionGoAwayReceived(_: HTTPConnectionPool.Connection.ID)
    func http2ConnectionClosed(_: HTTPConnectionPool.Connection.ID)
}

struct HTTP2PushNotSupportedError: Error {}

struct HTTP2ReceivedGoAwayBeforeSettingsError: Error {}

final class HTTP2Connection {
    internal static let defaultSettings = nioDefaultSettings + [HTTP2Setting(parameter: .enablePush, value: 0)]

    let channel: Channel
    let logger: Logger

    /// How the connection runs its streams, decided once per connection.
    enum StreamMode: Hashable, Sendable {
        /// A child channel and pipeline per request, via `HTTP2StreamMultiplexer`.
        case childChannels
        /// Every request is an entry in one `HTTP2ClientConnectionHandler`.
        case connectionHandler

        /// Child channels whenever a feature needs a stream `Channel` (the stream debug
        /// initializer, the decompression handler) or isn't ported yet (idle read/write timeouts).
        init(configuration: HTTPClient.Configuration) {
            if configuration.http2StreamChannelDebugInitializer != nil {
                self = .childChannels
            } else if case .enabled = configuration.decompression {
                self = .childChannels
            } else if configuration.timeout.read != nil || configuration.timeout.write != nil {
                self = .childChannels
            } else {
                self = configuration._http2StreamMode ?? .connectionHandler
            }
        }
    }

    private enum StreamDriver {
        case childChannels(HTTP2StreamMultiplexer)
        case connectionHandler(HTTP2ClientConnectionHandler)
    }

    private let streamDriver: StreamDriver

    /// A method with access to the stream channel that is called when creating the stream.
    let streamChannelDebugInitializer: (@Sendable (Channel) -> EventLoopFuture<Void>)?

    /// the connection pool that created the connection
    let delegate: HTTP2ConnectionDelegate

    enum State {
        case initialized
        case starting(EventLoopPromise<Int>)
        case active(maxStreams: Int)
        case closing
        case closed
    }

    /// A structure to store a http/2 stream channel in a set.
    private struct ChannelBox: Hashable {
        struct ID: Hashable {
            private let id: ObjectIdentifier

            init(_ channel: Channel) {
                self.id = ObjectIdentifier(channel)
            }
        }

        let channel: Channel

        var id: ID {
            ID(self.channel)
        }

        init(_ channel: Channel) {
            self.channel = channel
        }

        static func == (lhs: Self, rhs: Self) -> Bool {
            lhs.id == rhs.id
        }

        func hash(into hasher: inout Hasher) {
            hasher.combine(self.id)
        }
    }

    private var state: State

    /// We use this channel set to remember, which open streams we need to inform that
    /// we want to close the connection. The channels shall than cancel their currently running
    /// request. This property must only be accessed from the connections `EventLoop`.
    private var openStreams = Set<ChannelBox>()
    let id: HTTPConnectionPool.Connection.ID
    let decompression: HTTPClient.Decompression
    let maximumConnectionUses: Int?

    var closeFuture: EventLoopFuture<Void> {
        self.channel.closeFuture
    }

    init(
        channel: Channel,
        connectionID: HTTPConnectionPool.Connection.ID,
        decompression: HTTPClient.Decompression,
        maximumConnectionUses: Int?,
        delegate: HTTP2ConnectionDelegate,
        logger: Logger,
        streamMode: StreamMode = .childChannels,
        streamChannelDebugInitializer: (@Sendable (Channel) -> EventLoopFuture<Void>)? = nil
    ) {
        self.channel = channel
        self.id = connectionID
        self.decompression = decompression
        self.maximumConnectionUses = maximumConnectionUses
        self.logger = logger
        switch streamMode {
        case .childChannels:
            self.streamDriver = .childChannels(
                HTTP2StreamMultiplexer(
                    mode: .client,
                    channel: channel,
                    targetWindowSize: 8 * 1024 * 1024,  // 8mb
                    outboundBufferSizeHighWatermark: 8196,
                    outboundBufferSizeLowWatermark: 4092,
                    inboundStreamInitializer: { channel -> EventLoopFuture<Void> in
                        channel.eventLoop.makeFailedFuture(HTTP2PushNotSupportedError())
                    }
                )
            )
        case .connectionHandler:
            self.streamDriver = .connectionHandler(HTTP2ClientConnectionHandler(eventLoop: channel.eventLoop))
        }
        self.delegate = delegate
        self.state = .initialized
        self.streamChannelDebugInitializer = streamChannelDebugInitializer
    }

    deinit {
        guard case .closed = self.state else {
            preconditionFailure("Connection must be closed, before we can deinit it. Current state: \(self.state)")
        }
    }

    static func start(
        channel: Channel,
        connectionID: HTTPConnectionPool.Connection.ID,
        delegate: HTTP2ConnectionDelegate,
        decompression: HTTPClient.Decompression,
        maximumConnectionUses: Int?,
        logger: Logger,
        streamMode: StreamMode = .childChannels,
        streamChannelDebugInitializer: (@Sendable (Channel) -> EventLoopFuture<Void>)? = nil
    ) -> EventLoopFuture<(HTTP2Connection, Int)>.Isolated {
        let connection = HTTP2Connection(
            channel: channel,
            connectionID: connectionID,
            decompression: decompression,
            maximumConnectionUses: maximumConnectionUses,
            delegate: delegate,
            logger: logger,
            streamMode: streamMode,
            streamChannelDebugInitializer: streamChannelDebugInitializer
        )

        return connection._start0().assumeIsolated().map { maxStreams in
            (connection, maxStreams)
        }
    }

    var sendableView: SendableView {
        SendableView(self)
    }

    struct SendableView: Sendable {
        private let connection: NIOLoopBound<HTTP2Connection>
        let id: HTTPConnectionPool.Connection.ID
        let channel: Channel

        var eventLoop: EventLoop {
            self.connection.eventLoop
        }

        var closeFuture: EventLoopFuture<Void> {
            self.channel.closeFuture
        }

        func __forTesting_getStreamChannels() -> [Channel] {
            self.connection.value.__forTesting_getStreamChannels()
        }

        init(_ connection: HTTP2Connection) {
            self.connection = NIOLoopBound(connection, eventLoop: connection.channel.eventLoop)
            self.id = connection.id
            self.channel = connection.channel
        }

        func executeRequest(_ request: HTTPExecutableRequest) {
            self.connection.execute {
                $0.executeRequest0(request)
            }
        }

        func shutdown() {
            self.connection.execute {
                $0.shutdown0()
            }
        }

        func close(promise: EventLoopPromise<Void>?) {
            self.channel.close(mode: .all, promise: promise)
        }

        func close() -> EventLoopFuture<Void> {
            let promise = self.eventLoop.makePromise(of: Void.self)
            self.close(promise: promise)
            return promise.futureResult
        }
    }

    func _start0() -> EventLoopFuture<Int> {
        self.channel.eventLoop.assertInEventLoop()

        let readyToAcceptConnectionsPromise = self.channel.eventLoop.makePromise(of: Int.self)

        self.state = .starting(readyToAcceptConnectionsPromise)
        self.channel.closeFuture.assumeIsolated().whenComplete { _ in
            switch self.state {
            case .initialized, .closed:
                preconditionFailure("invalid state \(self.state)")
            case .starting(let readyToAcceptConnectionsPromise):
                self.state = .closed
                readyToAcceptConnectionsPromise.fail(HTTPClientError.remoteConnectionClosed)
            case .active, .closing:
                self.state = .closed
                self.delegate.http2ConnectionClosed(self.id)
            }
        }

        do {
            // We create and add the http handlers ourselves here, since we need to inject an
            // `HTTP2IdleHandler` between the `NIOHTTP2Handler` and the `HTTP2StreamMultiplexer`.
            // The purpose of the `HTTP2IdleHandler` is to count open streams in the multiplexer.
            // We use the HTTP2IdleHandler's information to notify our delegate, whether more work
            // can be scheduled on this connection.
            let sync = self.channel.pipeline.syncOperations

            switch self.streamDriver {
            case .childChannels(let multiplexer):
                let http2Handler = NIOHTTP2Handler(mode: .client, initialSettings: Self.defaultSettings)
                let idleHandler = HTTP2IdleHandler(
                    delegate: self,
                    logger: self.logger,
                    maximumConnectionUses: self.maximumConnectionUses
                )
                try sync.addHandler(http2Handler, position: .last)
                try sync.addHandler(idleHandler, position: .last)
                try sync.addHandler(multiplexer, position: .last)

            case .connectionHandler(let handler):
                // Spike: the handler is nio-http2's frame consumer and runs the idle state machine
                // itself, so no idle handler sits in between.
                handler.configureIdle(
                    delegate: self,
                    // Retire the connection before odd stream IDs run out.
                    maximumUses: min(
                        self.maximumConnectionUses ?? .max,
                        HTTP2ClientConnectionHandler.maximumStreamCount
                    )
                )
                let http2Handler = NIOHTTP2Handler(
                    mode: .client,
                    initialSettings: Self.defaultSettings,
                    consumer: handler
                )
                try sync.addHandler(http2Handler, position: .last)
                try sync.addHandler(handler, position: .last)
            }
        } catch {
            self.channel.close(mode: .all, promise: nil)
            readyToAcceptConnectionsPromise.fail(error)
        }

        return readyToAcceptConnectionsPromise.futureResult
    }

    private func executeRequest0(_ request: HTTPExecutableRequest) {
        self.channel.eventLoop.assertInEventLoop()

        switch self.state {
        case .initialized, .starting:
            preconditionFailure("Invalid state: \(self.state). Sending requests is not allowed before we are started.")

        case .active:
            let multiplexer: HTTP2StreamMultiplexer
            switch self.streamDriver {
            case .connectionHandler(let handler):
                handler.executeRequest(request)
                return
            case .childChannels(let childMultiplexer):
                multiplexer = childMultiplexer
            }
            let createStreamChannelPromise = self.channel.eventLoop.makePromise(of: Channel.self)
            let loopBoundSelf = NIOLoopBound(self, eventLoop: self.channel.eventLoop)

            multiplexer.createStreamChannel(
                promise: createStreamChannelPromise
            ) { [streamChannelDebugInitializer] channel -> EventLoopFuture<Void> in
                let connection = loopBoundSelf.value

                do {
                    // the connection may have been asked to shutdown while we created the child. in
                    // this
                    // channel.
                    guard case .active = connection.state else {
                        throw HTTPClientError.cancelled
                    }

                    // We only support http/2 over an https connection – using the Application-Layer
                    // Protocol Negotiation (ALPN). For this reason it is safe to fix this to `.https`.
                    let translate = HTTP2FramePayloadToHTTP1ClientCodec(httpProtocol: .https)
                    try channel.pipeline.syncOperations.addHandler(translate)

                    if case .enabled(let limit) = connection.decompression {
                        let decompressHandler = NIOHTTPResponseDecompressor(limit: limit)
                        try channel.pipeline.syncOperations.addHandler(decompressHandler)
                    }

                    let handler = HTTP2ClientRequestHandler(eventLoop: channel.eventLoop)
                    try channel.pipeline.syncOperations.addHandler(handler)

                    // We must add the new channel to the list of open channels BEFORE we write the
                    // request to it. In case of an error, we are sure that the channel was added
                    // before.
                    let box = ChannelBox(channel)
                    connection.openStreams.insert(box)
                    channel.closeFuture.assumeIsolated().whenComplete { _ in
                        connection.openStreams.remove(box)
                    }

                    if let streamChannelDebugInitializer = streamChannelDebugInitializer {
                        return streamChannelDebugInitializer(channel).map { _ in
                            channel.write(request, promise: nil)
                        }
                    } else {
                        channel.pipeline.syncOperations.write(NIOAny(request), promise: nil)
                        return channel.eventLoop.makeSucceededVoidFuture()
                    }
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
            }

            createStreamChannelPromise.futureResult.whenFailure { error in
                request.fail(error)
            }

        case .closing, .closed:
            // Because of race conditions requests might reach this point, even though the
            // connection is already closing
            return request.fail(HTTPClientError.cancelled)
        }
    }

    private func shutdown0() {
        self.channel.eventLoop.assertInEventLoop()

        switch self.state {
        case .active:
            self.state = .closing

            // inform all open streams, that the currently running request should be cancelled.
            switch self.streamDriver {
            case .childChannels:
                for box in self.openStreams {
                    box.channel.triggerUserOutboundEvent(HTTPConnectionEvent.shutdownRequested, promise: nil)
                }
            case .connectionHandler(let handler):
                handler.cancelAllStreams()
            }

            // inform the idle connection handler, that connection should be closed, once all streams
            // are closed.
            switch self.streamDriver {
            case .childChannels:
                self.channel.triggerUserOutboundEvent(HTTPConnectionEvent.shutdownRequested, promise: nil)
            case .connectionHandler(let handler):
                handler.shutdownRequested()
            }

        case .closed, .closing:
            // we are already closing/closed and we need to tolerate this
            break

        case .initialized, .starting:
            preconditionFailure("invalid state \(self.state)")
        }
    }

    func __forTesting_getStreamChannels() -> [Channel] {
        self.channel.eventLoop.preconditionInEventLoop()
        return self.openStreams.map { $0.channel }
    }
}

extension HTTP2Connection: HTTP2IdleHandlerDelegate {
    func http2SettingsReceived(maxStreams: Int) {
        self.channel.eventLoop.assertInEventLoop()

        switch self.state {
        case .initialized:
            preconditionFailure("Invalid state: \(self.state)")

        case .starting(let promise):
            self.state = .active(maxStreams: maxStreams)
            promise.succeed(maxStreams)

        case .active:
            self.state = .active(maxStreams: maxStreams)
            self.delegate.http2Connection(self.id, newMaxStreamSetting: maxStreams)

        case .closing, .closed:
            // ignore. we only wait for all connections to be closed anyway.
            break
        }
    }

    func http2GoAwayReceived() {
        self.channel.eventLoop.assertInEventLoop()

        switch self.state {
        case .initialized:
            preconditionFailure("Invalid state: \(self.state)")

        case .starting(let promise):
            self.state = .closing
            promise.fail(HTTP2ReceivedGoAwayBeforeSettingsError())

        case .active:
            self.state = .closing
            self.delegate.http2ConnectionGoAwayReceived(self.id)

        case .closing, .closed:
            // we are already closing. Nothing new
            break
        }
    }

    func http2StreamClosed(availableStreams: Int) {
        self.channel.eventLoop.assertInEventLoop()

        self.delegate.http2ConnectionStreamClosed(self.id, availableStreams: availableStreams)
    }
}

@available(*, unavailable)
extension HTTP2Connection: Sendable {}
