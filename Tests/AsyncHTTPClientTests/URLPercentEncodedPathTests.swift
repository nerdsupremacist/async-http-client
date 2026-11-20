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

import Foundation
import Testing

@testable import AsyncHTTPClient

@Suite("URL percent-encoded path")
struct URLPercentEncodedPathTests {
    // Escapes that don't decode to valid UTF-8 must reach the server as written, not collapse to `/`.
    static let invalidUTF8Paths: [(url: String, path: String)] = [
        ("http://x/%FF", "/%FF"),
        ("http://x/%C3", "/%C3"),
        ("http://x/%FF%FE/a", "/%FF%FE/a"),
    ]

    static let validPaths: [(url: String, path: String)] = [
        ("http://x", "/"),
        ("http://x/", "/"),
        ("http://x?foo=bar", "/"),
        ("http://x/some/path/", "/some/path/"),
        ("http://x/some%20path", "/some%20path"),
        ("http://x/some%2Fpathsegment1/pathsegment2", "/some%2Fpathsegment1/pathsegment2"),
    ]

    // Relative URLs must send the path resolved against their base, as `URLComponents` does.
    static let relativePaths: [(base: String, relative: String, path: String)] = [
        ("http://x", "percent%2Fencoded/hello", "/percent%2Fencoded/hello"),
        ("http://x/a/b/c", "d/e", "/a/b/d/e"),
        ("http://x/a/b/c", "../d", "/a/d"),
        ("http://x/a/b/c?q=1", "?z=2", "/a/b/c"),
        ("unix:///tmp/socket", "/echo-uri", "/echo-uri"),
    ]

    // Darwin's decoded `path` is empty for invalid UTF-8 escapes, and the emptiness check reads it.
    #if canImport(Darwin)
    static let decodedPathDropsInvalidUTF8 = true
    #else
    static let decodedPathDropsInvalidUTF8 = false
    #endif

    @Test(arguments: Self.validPaths)
    func validPathsAreKept(url: String, path: String) throws {
        let url = try #require(URL(string: url))
        #expect(url.percentEncodedPath == path)
    }

    @Test(arguments: Self.relativePaths)
    func relativePathsAreResolved(base: String, relative: String, path: String) throws {
        let url = try #require(URL(string: relative, relativeTo: URL(string: base)))
        #expect(url.percentEncodedPath == path)
    }

    @Test(arguments: Self.invalidUTF8Paths)
    func invalidUTF8EscapesAreKept(url: String, path: String) throws {
        let url = try #require(URL(string: url))
        withKnownIssue("Darwin sends `/` for paths whose escapes aren't valid UTF-8") {
            #expect(url.percentEncodedPath == path)
        } when: {
            Self.decodedPathDropsInvalidUTF8
        }
    }
}
