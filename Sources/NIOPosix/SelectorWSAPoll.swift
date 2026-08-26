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

#if !os(WASI)

#if os(Windows)
import CNIOWindows
import NIOConcurrencyHelpers
import NIOCore
import WinSDK

/// Outcome of a single attempt to fill a wide-string buffer via a Win32 API.
/// Declared at file scope because it is used from a method of a generic type,
/// where a locally-nested type is not permitted.
private enum _WideStringFillResult {
    case filled(String)
    case insufficient
    case failed(DWORD)
}

extension SelectorEventSet {
    // Use this property to create pollfd's event field. Reset and errors are (hopefully) always included.
    // According to the docs we don't need to listen for them explicitly.
    // Source: https://learn.microsoft.com/en-us/windows/win32/api/winsock2/ns-winsock2-wsapollfd
    var wsaPollEvent: Int16 {
        var result: Int16 = 0
        if self.contains(.read) {
            result |= Int16(WinSDK.POLLRDNORM)
        }
        if self.contains(.write) || self.contains(.writeEOF) {
            result |= Int16(WinSDK.POLLWRNORM)
        }
        return result
    }

    // Use this initializer to create a EventSet from the wsa pollfd's revent field
    @usableFromInline
    init(revents: Int16) {
        // Event Constant   Meaning	                What to do (Typical Socket API)
        // POLLRDNORM       Normal data readable    Use recv, WSARecv, or ReadFile
        // POLLRDBAND       Priority data readable  Use recv, WSARecv (with MSG_OOB for out-of-band)
        // POLLWRNORM       Normal data writable    Use send, WSASend, or WriteFile
        // POLLWRBAND       Priority data writable  Use send (with MSG_OOB for out-of-band data)
        // POLLERR          Error condition         Use getsockopt with SO_ERROR; may need closesocket
        // POLLHUP          Closed                  Usually just cleanup: closesocket
        // POLLNVAL         Invalid fd (not open)   Fix your code; close and remove fd
        self.rawValue = 0
        let mapped = Int32(revents)
        if mapped & WinSDK.POLLRDNORM != 0 {
            self.formUnion(.read)
        }
        if mapped & WinSDK.POLLWRNORM != 0 {
            self.formUnion(.write)
        }
        if mapped & WinSDK.POLLERR != 0 {
            self.formUnion(.error)
        }
        if mapped & WinSDK.POLLHUP != 0 {
            // Winsock collapses what epoll splits across EPOLLRDHUP and
            // EPOLLHUP into this single bit, so which of the two it means has
            // to be recovered from POLLERR. Measured on Windows 11 (26200),
            // one socket per row:
            //
            //   peer shutdown(SD_SEND), socket still open   ->  HUP
            //   peer sends 5 bytes then shutdown(SD_SEND)   ->  HUP|RDNORM
            //   peer closesocket()                          ->  HUP
            //   peer RST (SO_LINGER 0)                      ->  HUP|ERR
            //   refused non-blocking connect                ->  HUP|ERR|WRNORM
            //
            // So POLLHUP on its own is a *plain peer FIN* and nothing more:
            // measured, a `send()` of 14 bytes issued while WSAPoll was already
            // reporting POLLHUP succeeded and the peer received all 14. That is
            // EPOLLRDHUP, i.e. `.readEOF` — reporting it as `.reset` would tear
            // the connection down on every orderly close, discarding whatever
            // was flushed but not yet written, because
            // `SelectableEventLoop.handleEvent`'s `.reset` arm short-circuits
            // `writable()`.
            //
            // POLLERR alongside it *is* a reset or a failed connect, which is
            // also how a RST surfaces on epoll (EPOLLERR|EPOLLHUP).
            if mapped & WinSDK.POLLERR != 0 {
                self.formUnion(.reset)
            } else {
                self.formUnion(.readEOF)
            }
        }
        if mapped & WinSDK.POLLNVAL != 0 {
            preconditionFailure("Invalid fd supplied.")
        }
    }
}

extension Selector: _SelectorBackendProtocol {

    func initialiseState0() throws {
        self.pollFDs.reserveCapacity(16)
        self.deregisteredFDs.reserveCapacity(16)

        // Wake-up mechanism.
        //
        // On Linux we use eventfd and on Darwin we use a kevent-only EVFILT_USER.
        // Windows has no direct equivalent. The natural-looking choice would be
        // QueueUserAPC against the event-loop thread, but APCs only fire while
        // the target thread is in an alertable wait. WSAPoll is not alertable —
        // there is no `WSAPollEx`/`SleepEx`-style waitable variant that both
        // observes the registered sockets *and* picks up APCs in one call. That
        // would force us to either spin or split the wait into two phases, both
        // of which are unacceptable from a correctness/latency perspective.
        //
        // Instead, we mirror the eventfd pattern: create a connected socket pair
        // ourselves and include the read end as the first entry in `pollFDs`.
        // `wakeup0` writes a byte to the write end; the next WSAPoll returns and
        // the read end gets drained at the top of `whenReady0`. We use a UNIX
        // domain socket pair because Windows does not have `socketpair(2)` (so
        // we emulate it via listen/connect/accept on a temporary AF_UNIX path),
        // and AF_UNIX is loopback-only — no firewall prompts, no port collisions.
        let (readSocket, writeSocket) = try Self.createWakeupSocketPair()
        self.wakeupReadSocket = readSocket
        self.wakeupWriteSocket = writeSocket

        // The read end of the wakeup pair is always the first entry in pollFDs;
        // `whenReady0` relies on this invariant to drain wakeup bytes cheaply.
        // We deliberately do *not* publish the wakeup socket in `pollFDIndexes`
        // — user-facing register/reregister/deregister operations should never
        // see or touch the wakeup slot.
        let wakeupPollFD = pollfd(fd: UInt64(readSocket), events: Int16(WinSDK.POLLRDNORM), revents: 0)
        self.pollFDs.append(wakeupPollFD)

        self.lifecycleState = .open
    }

    /// Creates a pair of connected UNIX domain sockets for wakeup signaling.
    /// Since Windows doesn't support socketpair(), we emulate it with listen/connect/accept.
    /// Returns (readSocket, writeSocket) tuple.
    private static func createWakeupSocketPair() throws -> (NIOBSDSocket.Handle, NIOBSDSocket.Handle) {
        // Get a unique path for the UNIX domain socket
        let socketPath = try Self.generateUniqueSocketPath()

        // Create the listener socket. The listener and the on-disk socket path
        // are only needed long enough to bootstrap the connected pair, so we
        // tear them down unconditionally on the way out.
        let listenerSocket = try NIOBSDSocket.socket(domain: .unix, type: .stream, protocolSubtype: .default)
        defer {
            _ = try? NIOBSDSocket.close(socket: listenerSocket)
            // Use the wide API to delete the on-disk path for the same
            // Unicode/long-path safety reasons as the temp-directory lookup.
            _ = socketPath.withCString(encodedAs: UTF16.self) { DeleteFileW($0) }
        }

        // Set up the sockaddr_un structure
        var addr = sockaddr_un()
        addr.sun_family = ADDRESS_FAMILY(AF_UNIX)

        // Copy the path into sun_path
        let pathBytes = socketPath.utf8CString
        // A trap here would abort the process from inside `Selector.init`, i.e.
        // from inside `MultiThreadedEventLoopGroup`, with no diagnostic — and it
        // is reachable on a real machine, not just in theory: `sun_path` holds
        // 108 bytes, the generated name is `<TEMP>\nio-wakeup-<pid>-<tid>-<tick>.sock`
        // whose suffix is ~37 bytes and *grows with uptime* (`GetTickCount64` is
        // 10 digits today, 11 after roughly four months), so any `%TEMP%` longer
        // than about 69 characters trips it. A long user name, a redirected or
        // roaming TEMP, or a managed enterprise profile will do it. Every other
        // failure on this path throws, so throw here too.
        guard pathBytes.count <= MemoryLayout.size(ofValue: addr.sun_path) else {
            throw IOError(
                windows: DWORD(ERROR_INSUFFICIENT_BUFFER),
                reason: "wakeup socket path does not fit in sun_path: \(socketPath)"
            )
        }
        withUnsafeMutableBytes(of: &addr.sun_path) { destPtr in
            pathBytes.withUnsafeBufferPointer { srcPtr in
                destPtr.copyMemory(from: UnsafeRawBufferPointer(srcPtr))
            }
        }

        // Bind the listener socket. We rebind the typed `sockaddr_un`
        // pointer to the generic `sockaddr` that bind(2) expects but pass
        // the length of the original concrete struct (`addr`), which is
        // what the OS uses to interpret the address family payload.
        let addrLen = socklen_t(MemoryLayout.size(ofValue: addr))
        try withUnsafePointer(to: &addr) { addrPtr in
            try addrPtr.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketPointer in
                try NIOBSDSocket.bind(
                    socket: listenerSocket,
                    address: socketPointer,
                    address_len: addrLen
                )
            }
        }

        // Listen for connections (backlog of 1 is enough)
        if WinSDK.listen(listenerSocket, 1) == SOCKET_ERROR {
            throw IOError(winsock: WSAGetLastError(), reason: "listen")
        }

        // Create the client (write) socket and connect
        let writeSocket = try NIOBSDSocket.socket(domain: .unix, type: .stream, protocolSubtype: .default)

        do {
            // Connect to the listener (same rebind+length pattern as above).
            try withUnsafePointer(to: &addr) { addrPtr in
                try addrPtr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPtr in
                    guard
                        try NIOBSDSocket.connect(
                            socket: writeSocket,
                            address: sockaddrPtr,
                            address_len: addrLen
                        )
                    else {
                        throw IOError(winsock: WSAGetLastError(), reason: "connect")
                    }
                }
            }

            // Accept the connection to get the read socket
            let readSocket = WinSDK.accept(listenerSocket, nil, nil)
            if readSocket == INVALID_SOCKET {
                throw IOError(winsock: WSAGetLastError(), reason: "accept")
            }

            return (readSocket, writeSocket)
        } catch {
            _ = try? NIOBSDSocket.close(socket: writeSocket)
            throw error
        }
    }

    /// Generates a unique socket path in the Windows temp directory.
    private static func generateUniqueSocketPath() throws -> String {
        // Retrieve the temp directory using the wide (`W`) API so that paths
        // containing non-ASCII characters (e.g. a user profile whose name has
        // Unicode in it) survive intact, and size the buffer up to the Windows
        // maximum rather than the 260-char `MAX_PATH` soft limit. The returned
        // path is terminated with a trailing backslash.
        let tempDir = try Self.fillWideStringBuffer(
            initialSize: DWORD(MAX_PATH) + 1,
            maxSize: DWORD(Int16.max),
            reason: "GetTempPath2W"
        ) { buffer in
            GetTempPath2W(DWORD(buffer.count), buffer.baseAddress)
        }

        // Generate a unique identifier using process ID, thread ID, and tick count
        // This combination ensures uniqueness within a machine
        let processID = GetCurrentProcessId()
        let threadID = GetCurrentThreadId()
        let tickCount = GetTickCount64()
        let uniqueID = "\(processID)-\(threadID)-\(tickCount)"

        return "\(tempDir)nio-wakeup-\(uniqueID).sock"
    }

    /// Calls a Win32 API that fills a null-terminated wide-string (UTF-16)
    /// buffer, growing the allocation until the value fits or `maxSize` is
    /// reached. This avoids the ANSI (`A`) APIs, which cannot represent paths
    /// outside the active code page, and the 260-char `MAX_PATH` soft limit.
    ///
    /// `body` receives the destination buffer and must return the Win32
    /// convention for these APIs: `0` on failure (with the error retrievable
    /// via `GetLastError`), the number of `WCHAR`s written excluding the null
    /// terminator on success, or a value greater than or equal to the buffer's
    /// capacity when the buffer was too small. Modeled on the equivalent helper
    /// in swift-subprocess.
    private static func fillWideStringBuffer(
        initialSize: DWORD,
        maxSize: DWORD,
        reason: String,
        _ body: (UnsafeMutableBufferPointer<WCHAR>) -> DWORD
    ) throws -> String {
        var bufferCount = max(1, min(initialSize, maxSize))
        while bufferCount <= maxSize {
            let result: _WideStringFillResult = withUnsafeTemporaryAllocation(
                of: WCHAR.self,
                capacity: Int(bufferCount)
            ) { buffer in
                let count = body(buffer)
                switch count {
                case 0:
                    return .failed(GetLastError())
                case 1..<DWORD(buffer.count):
                    return .filled(String(decodingCString: buffer.baseAddress!, as: UTF16.self))
                default:
                    return .insufficient
                }
            }
            switch result {
            case .filled(let string):
                return string
            case .failed(let win32Error):
                throw IOError(windows: win32Error, reason: reason)
            case .insufficient:
                bufferCount *= 2
            }
        }
        throw IOError(windows: DWORD(ERROR_INSUFFICIENT_BUFFER), reason: reason)
    }

    func deinitAssertions0() {
        assert(
            self.wakeupReadSocket == NIOBSDSocket.invalidHandle,
            "wakeupReadSocket == \(self.wakeupReadSocket) in deinitAssertions0, forgot close?"
        )
        assert(
            self.wakeupWriteSocket == NIOBSDSocket.invalidHandle,
            "wakeupWriteSocket == \(self.wakeupWriteSocket) in deinitAssertions0, forgot close?"
        )
    }

    @inlinable
    func whenReady0(
        strategy: SelectorStrategy,
        onLoopBegin: () -> Void,
        _ body: (SelectorEvent<R>) throws -> Void
    ) throws {
        let time: Int32 =
            switch strategy {
            case .now:
                0

            case .block:
                -1

            case .blockUntilTimeout(let timeAmount):
                // Round *up*: truncating division turns every sub-millisecond
                // deadline into 0, and `WSAPoll(…, 0)` returns immediately
                // (measured: 0 ms, against 250 ms for a 250 ms timeout). The
                // loop would then spin at 100 % CPU through the sub-millisecond
                // tail of every timer expiry. Waking up to 999 µs late is
                // correct — the deadline is re-checked after the wait — and
                // spinning is not.
                Int32(clamping: timeAmount.nanoseconds / 1_000_000 + (timeAmount.nanoseconds % 1_000_000 == 0 ? 0 : 1))
            }

        // Retire anything deregistered since the last poll *before* polling, so
        // WSAPoll is never handed a handle we have already closed. Doing this
        // only after a poll that returned > 0 (as this used to) left stale
        // entries in `pollFDs` indefinitely, because POLLNVAL entries are not
        // counted in WSAPoll's return value (measured: rc=0 with
        // `revents[i] == POLLNVAL`), so a stale entry can never trigger the
        // compaction that would remove it.
        self.compactDeregisteredPollFDs()

        precondition(
            !self.pollFDs.isEmpty,
            "pollFDs should never be empty here, since we need an eventFD for waking up on demand"
        )
        // We always have at least the wakeup socket in pollFDs
        let result = self.pollFDs.withUnsafeMutableBufferPointer { ptr in
            WSAPoll(ptr.baseAddress!, UInt32(ptr.count), time)
        }

        if result == WinSDK.SOCKET_ERROR {
            throw IOError(winsock: WSAGetLastError(), reason: "WSAPoll")
        }

        // Every other backend calls this right after the wait returns and
        // before dispatching; this one accepted the parameter and dropped it.
        onLoopBegin()

        if result > 0 {
            // something has happened
            for i in self.pollFDs.indices {
                let pollFD = self.pollFDs[i]
                guard pollFD.revents != 0 else {
                    continue
                }
                // reset the revents
                self.pollFDs[i].revents = 0
                let fd = pollFD.fd

                // Check if this is the wakeup socket
                if NIOBSDSocket.Handle(fd) == self.wakeupReadSocket {
                    // Drain the wakeup socket by reading the data that was sent
                    var buffer: UInt8 = 0
                    _ = withUnsafeMutablePointer(to: &buffer) { ptr in
                        WinSDK.recv(self.wakeupReadSocket, ptr, 1, 0)
                    }
                    continue
                }

                // If the registration is not in the Map anymore we deregistered it during the processing of whenReady(...). In this case just skip it.
                guard let registration = self.registrations[Int(fd)] else {
                    continue
                }

                var selectorEvent = SelectorEventSet(revents: pollFD.revents)
                // in any case we only want what the user is currently registered for & what we got
                selectorEvent = selectorEvent.intersection(registration.interested)

                if selectorEvent == ._none, pollFD.revents & Int16(WinSDK.POLLHUP) != 0 {
                    // POLLHUP cannot be masked. epoll has no such problem:
                    // `BaseSocketChannel.readEOF0` drops EPOLLRDHUP from the
                    // interest set once EOF has been delivered, and epoll then
                    // goes quiet. Here the bit stays set for the rest of the
                    // socket's life — measured, including for `events == 0` —
                    // so plainly `continue`ing would make the next WSAPoll
                    // return instantly too and spin the loop at 100 % CPU.
                    //
                    // This is only reachable for a half-closed channel
                    // (`allowRemoteHalfClosure`); without it the EOF closes the
                    // channel and the descriptor is deregistered in the same
                    // tick. Two cases:
                    if registration.interested.contains(.write) {
                        // The channel still has something to write. A
                        // half-closed socket whose send buffer is full reports
                        // POLLHUP *without* POLLWRNORM (measured), so WSAPoll
                        // cannot distinguish "writable" from "not writable"
                        // here at all. Attempting the write is the only way to
                        // make progress: it either drains or returns
                        // `wouldBlock` harmlessly.
                        //
                        // TODO: this busy-polls until the peer reads. Reporting
                        // writability on a half-closed socket properly needs
                        // `select()`'s writefds, which has no such ambiguity.
                        selectorEvent = .write
                    } else {
                        // Nothing left to report. Withdraw the descriptor so the
                        // next wait can actually block; `reregister0` re-admits
                        // it the moment the channel wants anything again.
                        let handle = NIOBSDSocket.Handle(fd)
                        if let index = self.pollFDIndexes.removeValue(forKey: handle) {
                            self.deregisteredFDs.insert(index)
                            self.parkedFDs.insert(handle)
                        }
                        continue
                    }
                }

                guard selectorEvent != ._none else {
                    continue
                }

                try body((SelectorEvent(io: selectorEvent, registration: registration)))
            }
        }
    }

    /// Drops every `pollFDs` entry whose index is in `deregisteredFDs` in a
    /// single linear in-place pass: walk once with a read/write cursor and
    /// update `pollFDIndexes` for any surviving entry that shifted left. This is
    /// O(n) and avoids both a `sorted(by: >)` (O(k log k)) and per-call
    /// `pollFDs.remove(at:)` (O(n) each, O(k·n) total).
    ///
    /// The wakeup socket is always at `pollFDs[0]` and is never published in
    /// `pollFDIndexes`, so index 0 is a fixed survivor.
    ///
    /// This deliberately does **not** touch `registrations`. `Selector.deregister`
    /// owns that dictionary and has already removed the key by the time
    /// `deregister0` is called, so a removal here can never do the job it looks
    /// like it is doing — it can only fire when the handle *value* has since been
    /// re-registered by a different socket, and then it deletes that live
    /// registration. Winsock reuses handle values immediately and
    /// deterministically (measured: `socket()`/`closesocket()` returned the same
    /// handle six times running), so "the new socket got the old socket's handle"
    /// is the normal case here, not a corner case, and a reconnect issued from
    /// `channelInactive` — which NIO fires inline, inside this very tick — was
    /// reproducibly orphaned: polled forever, every event dropped at the
    /// `registrations` lookup, no error and no log.
    @inlinable
    func compactDeregisteredPollFDs() {
        guard !self.deregisteredFDs.isEmpty else {
            return
        }
        var write = 0
        for read in 0..<self.pollFDs.count {
            if self.deregisteredFDs.contains(read) {
                continue
            }
            if write != read {
                self.pollFDs[write] = self.pollFDs[read]
                if write != 0 {
                    self.pollFDIndexes[NIOBSDSocket.Handle(self.pollFDs[write].fd)] = write
                }
            }
            write += 1
        }
        self.pollFDs.removeLast(self.pollFDs.count - write)
        self.deregisteredFDs.removeAll(keepingCapacity: true)
    }

    func register0(
        selectableFD: NIOBSDSocket.Handle,
        fileDescriptor: NIOBSDSocket.Handle,
        interested: SelectorEventSet,
        registrationID: SelectorRegistrationID
    ) throws {
        // Winsock hands a fresh socket the handle value the last one released,
        // immediately and deterministically, so a stale entry for this value
        // must never be allowed to speak for the new socket.
        self.parkedFDs.remove(fileDescriptor)
        let poll = pollfd(fd: UInt64(fileDescriptor), events: interested.wsaPollEvent, revents: 0)
        self.pollFDIndexes[fileDescriptor] = self.pollFDs.count
        self.pollFDs.append(poll)
    }

    func reregister0(
        selectableFD: NIOBSDSocket.Handle,
        fileDescriptor: NIOBSDSocket.Handle,
        oldInterested: SelectorEventSet,
        newInterested: SelectorEventSet,
        registrationID: SelectorRegistrationID
    ) throws {
        if self.parkedFDs.contains(fileDescriptor) {
            // Re-admit a withdrawn descriptor as soon as the channel is
            // interested in more than the `[.reset, .error]` every registration
            // must carry. Note that `.readEOF` maps to no WSAPoll event bit, so
            // this test is on the *interest set*, not on `wsaPollEvent`.
            guard !newInterested.subtracting([.reset, .error]).isEmpty else {
                return
            }
            self.parkedFDs.remove(fileDescriptor)
            self.pollFDIndexes[fileDescriptor] = self.pollFDs.count
            self.pollFDs.append(
                pollfd(fd: UInt64(fileDescriptor), events: newInterested.wsaPollEvent, revents: 0)
            )
            return
        }
        if let index = self.pollFDIndexes[fileDescriptor] {
            self.pollFDs[index].events = newInterested.wsaPollEvent
        }
    }

    func deregister0(
        selectableFD: NIOBSDSocket.Handle,
        fileDescriptor: NIOBSDSocket.Handle,
        oldInterested: SelectorEventSet,
        registrationID: SelectorRegistrationID
    ) throws {
        if self.parkedFDs.remove(fileDescriptor) != nil {
            // Withdrawn descriptors are not in `pollFDs`; their old slot was
            // queued for removal when they were parked.
            return
        }
        if let index = self.pollFDIndexes.removeValue(forKey: fileDescriptor) {
            self.deregisteredFDs.insert(index)
        }
    }

    func wakeup0() throws {
        // Will be called from a different thread.
        // Write a single byte to the wakeup socket to wake up the event loop.
        try self.externalSelectorFDLock.withLock {
            guard self.wakeupWriteSocket != NIOBSDSocket.invalidHandle else {
                throw EventLoopError.shutdown
            }
            var byte: UInt8 = 0
            let result = withUnsafePointer(to: &byte) { ptr in
                WinSDK.send(self.wakeupWriteSocket, ptr, 1, 0)
            }
            if result == SOCKET_ERROR {
                throw IOError(winsock: WSAGetLastError(), reason: "send (wakeup)")
            }
        }
    }

    func close0() throws {
        // Like the epoll and kqueue selectors, serialize mutation of the wakeup
        // sockets against `wakeup0` (which may run on another thread) by taking
        // `externalSelectorFDLock`.
        self.externalSelectorFDLock.withLock {
            // Close the wakeup sockets
            if self.wakeupReadSocket != NIOBSDSocket.invalidHandle {
                try? NIOBSDSocket.close(socket: self.wakeupReadSocket)
                self.wakeupReadSocket = NIOBSDSocket.invalidHandle
            }
            if self.wakeupWriteSocket != NIOBSDSocket.invalidHandle {
                try? NIOBSDSocket.close(socket: self.wakeupWriteSocket)
                self.wakeupWriteSocket = NIOBSDSocket.invalidHandle
            }
            self.pollFDs.removeAll()
            self.pollFDIndexes.removeAll()
            self.deregisteredFDs.removeAll()
            self.parkedFDs.removeAll()
        }
    }
}
#endif
#endif  // !os(WASI)
