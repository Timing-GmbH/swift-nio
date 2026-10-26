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

// A non-blocking socket that is not ready fails its call with `WSAEWOULDBLOCK`.
// That is no error: the channel must wait for the selector, as it does for
// `EWOULDBLOCK` on epoll/kqueue, and the connection must stay up. These tests
// put a live connection into each such state on purpose.

import Dispatch
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

private struct Timeout: Error, CustomStringConvertible {
    var what: String
    var description: String { "timed out waiting for \(self.what)" }
}

/// Waits for `future` at most `seconds`, so a regression fails instead of hanging.
private func outcome<T>(_ future: EventLoopFuture<T>, within seconds: Double = 10, _ what: String) throws -> T {
    let semaphore = DispatchSemaphore(value: 0)
    let box = NIOLockedValueBox<Result<T, Error>?>(nil)
    future.whenComplete { result in
        box.withLockedValue { $0 = result }
        semaphore.signal()
    }
    guard semaphore.wait(timeout: .now() + seconds) == .success else {
        throw Timeout(what: what)
    }
    return try box.withLockedValue { $0! }.get()
}

/// Counts inbound bytes, echoes them if asked, and records errors and the channel going inactive.
private final class Recorder: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    let echo: Bool
    let bytes = NIOLockedValueBox(0)
    let errors = NIOLockedValueBox<[String]>([])
    let inactive = NIOLockedValueBox(false)

    init(echo: Bool) {
        self.echo = echo
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let count = self.unwrapInboundIn(data).readableBytes
        self.bytes.withLockedValue { $0 += count }
        if self.echo {
            context.writeAndFlush(data, promise: nil)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.errors.withLockedValue { $0.append("\(error)") }
        context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.inactive.withLockedValue { $0 = true }
        context.fireChannelInactive()
    }
}

/// Polls `condition` until it holds or `seconds` pass.
private func eventually(within seconds: Double = 10, _ condition: () -> Bool) -> Bool {
    let deadline = DispatchTime.now() + seconds
    while DispatchTime.now() < deadline {
        if condition() { return true }
        Thread.sleep(forTimeInterval: 0.01)
    }
    return condition()
}

final class WouldBlockTests: XCTestCase {
    /// A sender whose peer reads nothing fills its send buffer, and the next send
    /// fails with `WSAEWOULDBLOCK`. The writes must wait and complete once the
    /// peer reads; the connection must not close.
    func testAFullSendBufferMakesTheWriteWaitAndNotClose() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        defer { XCTAssertNoThrow(try group.syncShutdownGracefully()) }

        let serverSide = Recorder(echo: false)
        let accepted = group.next().makePromise(of: Channel.self)
        let server = try ServerBootstrap(group: group)
            .childChannelOption(.autoRead, value: false)
            .childChannelOption(.socketOption(.so_rcvbuf), value: 8192)
            .childChannelInitializer { channel in
                accepted.succeed(channel)
                return channel.pipeline.addHandler(serverSide)
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
        defer { XCTAssertNoThrow(try server.close().wait()) }

        let clientSide = Recorder(echo: false)
        let client = try ClientBootstrap(group: group)
            .channelOption(.socketOption(.so_sndbuf), value: 8192)
            .channelInitializer { $0.pipeline.addHandler(clientSide) }
            .connect(to: server.localAddress!)
            .wait()
        defer { client.close(promise: nil) }
        let child = try outcome(accepted.futureResult, "the server to accept")

        // Many separately flushed writes against a peer that reads nothing: one of
        // the sends meets a full buffer.
        let chunk = ByteBuffer(repeating: 0x41, count: 1 << 20)
        let chunks = 32
        let writes = (0..<chunks).map { _ in client.writeAndFlush(chunk) }
        let allWritten = EventLoopFuture.andAllSucceed(writes, on: client.eventLoop)
        Thread.sleep(forTimeInterval: 0.3)
        XCTAssertTrue(client.isActive, "the client closed while its peer was not reading")
        XCTAssertEqual(clientSide.errors.withLockedValue { $0 }, [])

        // Now the peer reads; every byte arrives and every write completes.
        try outcome(child.setOption(.autoRead, value: true), "autoRead on")
        XCTAssertNoThrow(try outcome(allWritten, within: 20, "the writes to complete"))
        XCTAssertTrue(eventually { serverSide.bytes.withLockedValue { $0 } == chunks * chunk.readableBytes })
        XCTAssertEqual(serverSide.bytes.withLockedValue { $0 }, chunks * chunk.readableBytes)
        XCTAssertFalse(clientSide.inactive.withLockedValue { $0 })
        XCTAssertFalse(serverSide.inactive.withLockedValue { $0 })
        XCTAssertEqual(serverSide.errors.withLockedValue { $0 }, [])
    }

    /// A read that fills its buffer exactly reads again, and that read finds no
    /// data and fails with `WSAEWOULDBLOCK`. The channel must simply wait for
    /// more; the connection must stay up and keep echoing.
    func testAReadThatFindsNoMoreDataWaitsAndDoesNotFail() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try group.syncShutdownGracefully()) }

        let size = 1024
        let serverSide = Recorder(echo: true)
        let server = try ServerBootstrap(group: group)
            .childChannelOption(.recvAllocator, value: FixedSizeRecvByteBufferAllocator(capacity: size))
            .childChannelOption(.maxMessagesPerRead, value: 4)
            .childChannelInitializer { $0.pipeline.addHandler(serverSide) }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
        defer { XCTAssertNoThrow(try server.close().wait()) }

        let clientSide = Recorder(echo: false)
        let client = try ClientBootstrap(group: group)
            .channelInitializer { $0.pipeline.addHandler(clientSide) }
            .connect(to: server.localAddress!)
            .wait()
        defer { client.close(promise: nil) }

        // Each round sends exactly one receive buffer's worth, so the server's
        // second read in the same wake-up finds the socket empty.
        for round in 1...3 {
            try outcome(client.writeAndFlush(ByteBuffer(repeating: 0x42, count: size)), "round \(round)'s write")
            XCTAssertTrue(
                eventually { clientSide.bytes.withLockedValue { $0 } == round * size || clientSide.inactive.withLockedValue { $0 } },
                "round \(round)'s echo")
            XCTAssertEqual(clientSide.bytes.withLockedValue { $0 }, round * size, "round \(round)")
        }
        XCTAssertEqual(serverSide.errors.withLockedValue { $0 }, [])
        XCTAssertFalse(serverSide.inactive.withLockedValue { $0 })
        XCTAssertFalse(clientSide.inactive.withLockedValue { $0 })
    }
}
