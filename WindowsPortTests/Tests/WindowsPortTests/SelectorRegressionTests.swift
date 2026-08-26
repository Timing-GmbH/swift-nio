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

// Regression tests for the Windows `WSAPoll` selector backend.
//
// Every test here uses only public NIO API and passes on epoll/kqueue by
// construction: each one asserts a guarantee the portable backends already
// give. They are the executable form of three findings against
// `Sources/NIOPosix/SelectorWSAPoll.swift`.

import Dispatch
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import XCTest

private struct HarnessTimeout: Error, CustomStringConvertible {
    var what: String
    var description: String { "timed out waiting for \(self.what)" }
}

/// `EventLoopFuture.wait()` blocks forever if the future never fires; every
/// wait in this file is bounded so a regression shows up as a failure rather
/// than a hung test run.
private func expect<T>(
    _ future: EventLoopFuture<T>,
    within seconds: Double = 10,
    _ what: String
) throws -> T {
    let semaphore = DispatchSemaphore(value: 0)
    let box = NIOLockedValueBox<Result<T, Error>?>(nil)
    future.whenComplete { result in
        box.withLockedValue { $0 = result }
        semaphore.signal()
    }
    guard semaphore.wait(timeout: .now() + seconds) == .success else {
        throw HarnessTimeout(what: what)
    }
    return try box.withLockedValue { $0! }.get()
}

// MARK: - handlers

/// Counts every inbound byte and fulfils `reached` once `target` have arrived,
/// or on `channelInactive` with whatever did arrive.
private final class ByteCountingHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer

    private let counter: NIOLockedValueBox<Int>
    private let target: Int
    private let reached: EventLoopPromise<Int>

    init(target: Int, reached: EventLoopPromise<Int>, counter: NIOLockedValueBox<Int>) {
        self.target = target
        self.reached = reached
        self.counter = counter
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        let total = self.counter.withLockedValue { value -> Int in
            value += buffer.readableBytes
            return value
        }
        if total >= self.target {
            self.reached.succeed(total)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        // Surfacing the truncated count is what makes a failure legible: the
        // assertion then reads "peer got 66 KB of 100 KB", not "timed out".
        self.reached.succeed(self.counter.withLockedValue { $0 })
        context.fireChannelInactive()
    }
}

/// Half-closes its own write side the moment the first byte arrives, then keeps
/// reading. This is a plain peer FIN: the socket stays perfectly writable from
/// the other end, and a correct selector reports EOF, not a reset.
///
/// The FIN is deliberately triggered by the *first read* rather than by
/// `channelActive`, so that it is guaranteed to arrive while the writer still
/// has a large flushed-but-unwritten backlog. That is the state in which the
/// bug destroys data.
private final class HalfCloseOnFirstReadHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer

    private let done = NIOLockedValueBox(false)

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let already = self.done.withLockedValue { value -> Bool in
            defer { value = true }
            return value
        }
        context.fireChannelRead(data)
        if !already {
            context.close(mode: .output, promise: nil)
        }
    }
}

/// Writes `payload` as one flushed write as soon as the channel goes active.
private final class BulkWriterHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let payloadSize: Int
    private let written: EventLoopPromise<Void>

    init(payloadSize: Int, written: EventLoopPromise<Void>) {
        self.payloadSize = payloadSize
        self.written = written
    }

    func channelActive(context: ChannelHandlerContext) {
        context.fireChannelActive()
        var buffer = context.channel.allocator.buffer(capacity: self.payloadSize)
        buffer.writeRepeatingByte(0x41, count: self.payloadSize)
        context.writeAndFlush(self.wrapOutboundOut(buffer)).cascade(to: self.written)
    }
}

/// Records `ChannelEvent.inputClosed` and any error caught.
private final class InputClosedRecorder: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer

    let inputClosed: EventLoopPromise<Void>
    let errors: NIOLockedValueBox<[String]>

    init(inputClosed: EventLoopPromise<Void>, errors: NIOLockedValueBox<[String]>) {
        self.inputClosed = inputClosed
        self.errors = errors
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case ChannelEvent.inputClosed = event {
            self.inputClosed.succeed(())
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.errors.withLockedValue { $0.append("\(error)") }
        context.fireErrorCaught(error)
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.inputClosed.fail(ChannelError.alreadyClosed)
        context.fireChannelInactive()
    }
}

private final class EchoHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        context.writeAndFlush(data, promise: nil)
    }
}

/// Reconnects from `channelInactive`, which NIO fires *inline* while the
/// selector is still inside the tick that deregistered the dying socket
/// (`BaseSocketChannel.close0` calls out to the pipeline before deferring the
/// close promise). That is the window in which the new socket can be handed the
/// handle value the old one just released.
private final class ReconnectOnInactiveHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer

    private let reconnect: @Sendable () -> Void
    private let once = NIOLockedValueBox(false)

    init(reconnect: @escaping @Sendable () -> Void) {
        self.reconnect = reconnect
    }

    func channelInactive(context: ChannelHandlerContext) {
        let alreadyFired = self.once.withLockedValue { value -> Bool in
            defer { value = true }
            return value
        }
        context.fireChannelInactive()
        if !alreadyFired {
            self.reconnect()
        }
    }
}

private final class EchoedBytesHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer

    let echoed: EventLoopPromise<String>

    init(echoed: EventLoopPromise<String>) {
        self.echoed = echoed
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = self.unwrapInboundIn(data)
        self.echoed.succeed(buffer.readString(length: buffer.readableBytes) ?? "")
    }
}

// MARK: - tests

final class SelectorRegressionTests: XCTestCase {

    // MARK: D1 - a plain peer FIN is an EOF, not a reset

    /// A peer that calls `shutdown(SD_SEND)` has not reset the connection: the
    /// socket is still writable, and bytes written after the FIN are received in
    /// full. Measured directly against Winsock for this port: a 14-byte `send()`
    /// issued after the peer's `shutdown` arrived intact while `WSAPoll` was
    /// already reporting `POLLHUP`.
    ///
    /// A backend that maps `POLLHUP` to `.reset` cannot express that: the
    /// `.reset` arm of `SelectableEventLoop.handleEvent` short-circuits
    /// `writable()` and tears the channel down, so everything still queued is
    /// thrown away. The payload here is far larger than any socket buffer, and
    /// the peer half-closes on its *first read*, so the FIN is guaranteed to
    /// land while a large flushed backlog is still outstanding.
    ///
    /// Three things are pinned at once: the write completes, the FIN arrives as
    /// `ChannelEvent.inputClosed` rather than as an error, and every byte
    /// reaches the peer.
    func testPlainPeerFINIsAHalfCloseAndNotAReset() throws {
        let serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try serverGroup.syncShutdownGracefully())
            XCTAssertNoThrow(try clientGroup.syncShutdownGracefully())
        }

        let payloadSize = 2 * 1024 * 1024
        let marker = "written-after-the-peer-said-goodbye"
        let expectedTotal = payloadSize + marker.utf8.count
        let counter = NIOLockedValueBox(0)
        let receivedAll = serverGroup.next().makePromise(of: Int.self)
        // NIO traps on a promise that is deallocated unfulfilled, which would
        // turn a plain test failure into a process abort and lose the output.
        // Completing an already-completed promise is a no-op.
        defer { receivedAll.fail(HarnessTimeout(what: "the peer to receive the payload")) }

        let server = try ServerBootstrap(group: serverGroup)
            .serverChannelOption(ChannelOptions.backlog, value: 8)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            // Small buffers on both ends so the payload cannot possibly be
            // handed to the kernel in one go: the write has to stall, be
            // registered for writability, and resume across many selector
            // ticks. Otherwise a fast loopback can swallow the whole payload
            // before the FIN is ever observed and the test stops testing
            // anything.
            .childChannelOption(ChannelOptions.socketOption(.so_rcvbuf), value: 8192)
            .childChannelInitializer { channel in
                channel.pipeline.addHandlers([
                    HalfCloseOnFirstReadHandler(),
                    ByteCountingHandler(target: expectedTotal, reached: receivedAll, counter: counter),
                ])
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
        defer { XCTAssertNoThrow(try server.close().wait()) }

        let loop = clientGroup.next()
        let inputClosed = loop.makePromise(of: Void.self)
        let written = loop.makePromise(of: Void.self)
        defer {
            inputClosed.fail(HarnessTimeout(what: "ChannelEvent.inputClosed"))
            written.fail(HarnessTimeout(what: "the flushed payload to be written"))
        }
        let errors = NIOLockedValueBox<[String]>([])

        let client = try ClientBootstrap(group: clientGroup)
            .channelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .channelOption(ChannelOptions.socketOption(.so_sndbuf), value: 8192)
            .connectTimeout(.seconds(5))
            .channelInitializer { channel in
                channel.pipeline.addHandlers([
                    InputClosedRecorder(inputClosed: inputClosed, errors: errors),
                    BulkWriterHandler(payloadSize: payloadSize, written: written),
                ])
            }
            .connect(to: server.localAddress!)
            .wait()

        // 1. The flushed backlog must survive the peer's FIN.
        XCTAssertNoThrow(
            try expect(written.futureResult, within: 20, "the flushed payload to be written"),
            "the write was cancelled by the peer's half-close; errors seen: \(errors.withLockedValue { $0 })"
        )

        // 2. The FIN must arrive as an EOF. If the backend calls it a reset, the
        //    channel is torn down instead and this promise fails.
        XCTAssertNoThrow(
            try expect(inputClosed.futureResult, within: 20, "ChannelEvent.inputClosed"),
            "peer FIN was not reported as a half-close; errors seen: \(errors.withLockedValue { $0 })"
        )

        // 3. The half-closed channel must still be usable, not reset. Winsock's
        //    POLLHUP is set for the rest of the socket's life, so this also
        //    exercises the backend's ability to keep waiting rather than spin.
        XCTAssertTrue(
            try client.eventLoop.submit { client.isActive }.wait(),
            "the channel was closed by the peer's half-close"
        )
        XCTAssertEqual([], errors.withLockedValue { $0 }, "a clean half-close raised an error")

        // 4. And a write issued *after* the FIN must still reach the peer. On
        //    the unfixed backend this is where the bytes went to zero.
        var tail = client.allocator.buffer(capacity: marker.utf8.count)
        tail.writeString(marker)
        XCTAssertNoThrow(try expect(client.writeAndFlush(tail), within: 20, "the post-FIN write"))

        // 5. Every byte must arrive.
        let received = try expect(receivedAll.futureResult, within: 20, "peer to receive the payload")
        XCTAssertEqual(expectedTotal, received, "peer received a truncated stream after its own half-close")

        XCTAssertNoThrow(try client.close().wait())
    }

    // MARK: D2 - a refused connect must not wait out the connect timeout

    /// `WSAPoll` is widely documented not to report failed connection attempts.
    /// If that held here, a refused connect would only ever resolve when
    /// `connectTimeout` fires. The timeout below is set deliberately long so
    /// that the operating system, not NIO, has to be what fails the connect.
    func testConnectToAClosedPortFailsWithoutWaitingOutTheTimeout() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try group.syncShutdownGracefully()) }

        let start = DispatchTime.now()
        let connect = ClientBootstrap(group: group)
            .connectTimeout(.seconds(30))
            .connect(host: "127.0.0.1", port: 1)

        var caught: Error?
        do {
            let channel = try expect(connect, within: 25, "refused connect to resolve")
            XCTAssertNoThrow(try channel.close().wait())
            XCTFail("connect to 127.0.0.1:1 unexpectedly succeeded")
            return
        } catch {
            caught = error
        }
        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - start.uptimeNanoseconds) / 1e9

        print("[D2] refused connect resolved in \(elapsed)s as \(String(describing: caught))")
        XCTAssertLessThan(elapsed, 8.0, "a refused connect took \(elapsed)s to be reported")
        if let channelError = caught as? ChannelError, channelError == ChannelError.connectTimeout(.seconds(30)) {
            XCTFail("refused connect surfaced as connectTimeout rather than a connection error")
        }
    }

    // MARK: D3 - a reconnect that reuses the just-closed handle must still work

    /// The realistic shape of the handle-reuse hazard: a connection drops and
    /// the reconnect is kicked off from the close future, which runs inline on
    /// the event loop, inside the very selector tick that deregistered the old
    /// socket. The OS hands the new socket the same handle value (measured:
    /// deterministic on Winsock), so any backend bookkeeping keyed on the raw
    /// handle can delete the *new* registration.
    ///
    /// Symptom if it does: the second channel is polled but its events are
    /// dropped forever - no error, no log. The connect simply never completes.
    func testReconnectStartedFromTheCloseFutureIsNotOrphaned() throws {
        let serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try serverGroup.syncShutdownGracefully())
            XCTAssertNoThrow(try clientGroup.syncShutdownGracefully())
        }

        let connectionCount = NIOLockedValueBox(0)
        let server = try ServerBootstrap(group: serverGroup)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let ordinal = connectionCount.withLockedValue { value -> Int in
                    value += 1
                    return value
                }
                if ordinal == 1 {
                    // Drop the first connection the moment it is up.
                    return channel.eventLoop.makeSucceededVoidFuture().map {
                        channel.close(promise: nil)
                    }
                }
                return channel.pipeline.addHandler(EchoHandler())
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
        defer { XCTAssertNoThrow(try server.close().wait()) }

        let address = server.localAddress!
        let loop = clientGroup.next()
        let echoed = loop.makePromise(of: String.self)
        let secondConnected = loop.makePromise(of: Channel.self)
        defer {
            echoed.fail(HarnessTimeout(what: "the reconnect's echo"))
            secondConnected.fail(HarnessTimeout(what: "the reconnect to connect"))
        }

        // The reconnect is issued from `channelInactive`, which runs inline
        // inside the selector tick that deregistered the first socket - unlike
        // the close *future*, which `close0` defers with `eventLoop.execute`
        // and which therefore lands on a later tick, after any compaction.
        let reconnect: @Sendable () -> Void = {
            ClientBootstrap(group: clientGroup)
                .connectTimeout(.seconds(5))
                .channelInitializer { channel in
                    channel.pipeline.addHandler(EchoedBytesHandler(echoed: echoed))
                }
                .connect(to: address)
                .cascade(to: secondConnected)
        }

        let first = try ClientBootstrap(group: clientGroup)
            .connectTimeout(.seconds(5))
            .channelInitializer { channel in
                channel.pipeline.addHandler(ReconnectOnInactiveHandler(reconnect: reconnect))
            }
            .connect(to: address)
            .wait()
        _ = first

        let second = try expect(
            secondConnected.futureResult,
            within: 15,
            "the reconnect started from the close future to connect"
        )
        defer { XCTAssertNoThrow(try second.close().wait()) }

        var buffer = second.allocator.buffer(capacity: 4)
        buffer.writeString("PING")
        XCTAssertNoThrow(try expect(second.writeAndFlush(buffer), within: 10, "the reconnect's write"))
        XCTAssertEqual("PING", try expect(echoed.futureResult, within: 10, "the reconnect's echo"))
    }
}
