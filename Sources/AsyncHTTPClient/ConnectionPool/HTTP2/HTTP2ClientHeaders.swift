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
import NIOHPACK
import NIOHTTP1
import NIOHTTP2

// The HTTP/1 ⇄ HTTP/2 header conversions of swift-nio-http2's `HTTP2FramePayloadToHTTP1ClientCodec`
// (HTTP2ToHTTP1Codec.swift in 1.46.0). nio-http2 keeps them fileprivate/internal, so the channel-less
// path carries a copy. It must stay byte-for-byte equivalent to the codec.

extension HPACKHeaders {
    init(http2ClientRequestHead head: HTTPRequestHead) throws {
        // :path, :method, :scheme and :authority come first; Host becomes :authority.
        var newHeaders: [(String, String)] = []
        newHeaders.reserveCapacity(head.headers.count + 3)
        newHeaders.append((":path", head.uri))
        newHeaders.append((":method", head.method.rawValue))
        newHeaders.append((":scheme", "https"))
        newHeaders.append((":authority", ""))

        var authorityHeader: String? = nil
        for header in head.headers {
            if header.name.lowercased() == "host" {
                if authorityHeader != nil {
                    throw NIOHTTP2Errors.duplicateHostHeader()
                }
                authorityHeader = header.value
            } else {
                newHeaders.append((header.name, header.value))
            }
        }

        guard let actualAuthorityHeader = authorityHeader else {
            throw NIOHTTP2Errors.missingHostHeader()
        }
        newHeaders[3].1 = actualAuthorityHeader

        self.init(httpHeaders: HTTPHeaders(newHeaders), normalizeHTTPHeaders: true)
    }

    init(http2ClientTrailers trailers: HTTPHeaders) {
        self.init(httpHeaders: trailers, normalizeHTTPHeaders: true)
    }

    fileprivate func http2ClientPeekPseudoHeader(name: String) throws -> String {
        var headerValue: String? = nil
        for (fieldName, fieldValue, _) in self {
            if name == fieldName {
                guard headerValue == nil else {
                    throw NIOHTTP2Errors.duplicatePseudoHeader(name)
                }
                headerValue = fieldValue
            }
        }

        guard let headerValue else {
            throw NIOHTTP2Errors.missingPseudoHeader(name)
        }
        // CTLs, SP and DEL would allow HTTP/2 to HTTP/1 smuggling once the value lands in an
        // HTTP/1 message, so the codec rejects them.
        if headerValue.utf8.contains(where: { $0 <= 0x20 || $0 == 0x7F }) {
            throw NIOHTTP2Errors.invalidPseudoHeaderValue(name: name, value: headerValue)
        }
        return headerValue
    }

    fileprivate func http2ClientIsInformationalResponse() throws -> Bool {
        try self.http2ClientPeekPseudoHeader(name: ":status").first! == "1"
    }
}

extension HTTPHeaders {
    init(http2ClientRegularHeadersFrom headers: HPACKHeaders) {
        var newHeaders: [(String, String)] = []
        newHeaders.reserveCapacity(headers.count)
        for (name, value, _) in headers where name.first != ":" {
            newHeaders.append((name, value))
        }
        self.init(newHeaders)
    }
}

extension HTTPResponseHead {
    fileprivate init(http2ClientHeaderBlock headers: HPACKHeaders) throws {
        let statusHeader = try headers.http2ClientPeekPseudoHeader(name: ":status")
        guard let integerStatus = Int(statusHeader, radix: 10) else {
            throw NIOHTTP2Errors.invalidStatusValue(statusHeader)
        }
        self.init(
            version: .init(major: 2, minor: 0),
            status: HTTPResponseStatus(statusCode: integerStatus),
            headers: HTTPHeaders(http2ClientRegularHeadersFrom: headers)
        )
    }

    fileprivate init(http2ClientHeaderBlock headers: HPACKHeaders, requestMethod: HTTPMethod, endStream: Bool) throws {
        try self.init(http2ClientHeaderBlock: headers)
        let statusCode = self.status.code
        let addsFraming =
            !(100..<200).contains(statusCode)
            && statusCode != 204
            && statusCode != 304
            && requestMethod != .HEAD
            && requestMethod != .CONNECT
            && !headers.contains(name: "content-length")
        if addsFraming {
            if endStream {
                self.headers.add(name: "content-length", value: "0")
            } else {
                self.headers.add(name: "transfer-encoding", value: "chunked")
            }
        }
    }
}

/// Turns the HEADERS and DATA frames of one client stream into HTTP/1 response parts, like
/// `HTTP2FramePayloadToHTTP1ClientCodec.channelRead`.
struct HTTP2ClientResponseDecoder {
    private enum PreviousHeaders {
        case none
        case informational
        case final
        case trailers
    }

    private let requestMethod: HTTPMethod
    private var previous: PreviousHeaders = .none

    init(requestMethod: HTTPMethod) {
        self.requestMethod = requestMethod
    }

    mutating func decode(
        _ payload: HTTP2Frame.FramePayload
    ) throws -> (first: HTTPClientResponsePart?, second: HTTPClientResponsePart?) {
        switch payload {
        case .headers(let content):
            switch self.previous {
            case .none, .informational:
                if try content.headers.http2ClientIsInformationalResponse() {
                    self.previous = .informational
                    return (.head(try HTTPResponseHead(http2ClientHeaderBlock: content.headers)), nil)
                }
                self.previous = .final
                let head = try HTTPResponseHead(
                    http2ClientHeaderBlock: content.headers,
                    requestMethod: self.requestMethod,
                    endStream: content.endStream
                )
                return (.head(head), content.endStream ? .end(nil) : nil)

            case .final:
                self.previous = .trailers
                return (.end(HTTPHeaders(http2ClientRegularHeadersFrom: content.headers)), nil)

            case .trailers:
                preconditionFailure("Sending too many header blocks.")
            }

        case .data(let content):
            guard case .byteBuffer(let buffer) = content.data else {
                preconditionFailure("Received DATA frame with non-bytebuffer IOData")
            }
            if content.endStream {
                if buffer.readableBytes == 0 {
                    return (.end(nil), nil)
                }
                return (.body(buffer), .end(nil))
            }
            return (.body(buffer), nil)

        case .alternativeService, .rstStream, .priority, .windowUpdate, .settings, .pushPromise, .ping, .goAway,
            .origin:
            return (nil, nil)
        }
    }
}
