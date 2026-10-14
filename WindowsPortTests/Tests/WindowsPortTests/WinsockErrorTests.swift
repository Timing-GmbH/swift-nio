//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2025 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

// Windows reports socket failures as winsock codes, including a socket's
// pending error (`SO_ERROR`). These tests check that such an error reads and
// compares as the POSIX code of the same meaning, as it does on epoll/kqueue.

import NIOCore
import NIOPosix
import XCTest

#if os(Windows)
import let WinSDK.WSAEADDRINUSE
import let WinSDK.WSASYSNOTREADY
#endif

final class WinsockErrorTests: XCTestCase {
    private var group: MultiThreadedEventLoopGroup!

    override func setUp() {
        super.setUp()
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    override func tearDown() {
        try? self.group.syncShutdownGracefully()
        self.group = nil
        super.tearDown()
    }

    /// A refused connect fails through the socket's `SO_ERROR`, which must read
    /// as `ECONNREFUSED`, not as an unknown `errno` holding a winsock code.
    func testARefusedConnectReadsAsConnectionRefused() throws {
        // A loopback port that was free a moment ago: bind it, note it, close it.
        let server = try ServerBootstrap(group: self.group).bind(host: "127.0.0.1", port: 0).wait()
        let address = try XCTUnwrap(server.localAddress)
        try server.close().wait()

        XCTAssertThrowsError(try ClientBootstrap(group: self.group).connect(to: address).wait()) { error in
            guard let ioError = error as? IOError else {
                return XCTFail("expected an IOError, got \(error)")
            }
            XCTAssertEqual(ioError.errnoCode, ECONNREFUSED, "\(ioError)")
            XCTAssertFalse(ioError.description.contains("Unknown error"), ioError.description)
        }
    }

    /// Binding a port another socket holds fails with `EADDRINUSE`.
    func testBindingATakenPortReadsAsAddressInUse() throws {
        let holder = try ServerBootstrap(group: self.group).bind(host: "127.0.0.1", port: 0).wait()
        defer { try? holder.close().wait() }
        let address = try XCTUnwrap(holder.localAddress)

        XCTAssertThrowsError(try ServerBootstrap(group: self.group).bind(to: address).wait()) { error in
            XCTAssertEqual((error as? IOError)?.errnoCode, EADDRINUSE, "\(error)")
        }
    }

    #if os(Windows)
    /// `errnoCode` answers for every domain: the POSIX code of a winsock error
    /// where one exists, else the code itself.
    func testErrnoCodeOfAWinsockError() {
        XCTAssertEqual(IOError(winsock: WSAEADDRINUSE, reason: "bind").errnoCode, EADDRINUSE)
        XCTAssertEqual(IOError(winsock: WSASYSNOTREADY, reason: "WSAStartup").errnoCode, WSASYSNOTREADY)
        XCTAssertEqual(IOError(errnoCode: EADDRINUSE, reason: "bind").errnoCode, EADDRINUSE)
    }
    #endif
}
