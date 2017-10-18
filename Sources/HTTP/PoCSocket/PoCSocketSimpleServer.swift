// This source file is part of the Swift.org Server APIs open source project
//
// Copyright (c) 2017 Swift Server API project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See http://swift.org/LICENSE.txt for license information
//

import Dispatch
import Foundation


// MARK: Server

/// An HTTP server that listens for connections on a TCP socket and spawns Listeners to handle them.
///:nodoc:
public class PoCSocketSimpleServer: CurrentConnectionCounting {
    /// PoCSocket to listen on for connections
    private let serverSocket: PoCSocket = PoCSocket()

    /// Collection of listeners of sockets. Used to kill connections on timeout or shutdown
    private var connectionListenerList = ConnectionListenerCollection()

    // Timer that cleans up idle sockets on expire
    private let pruneSocketTimer: DispatchSourceTimer = DispatchSource.makeTimerSource(queue: DispatchQueue(label: "pruneSocketTimer"))

    /// The port we're listening on. Used primarily to query a randomly assigned port during XCTests
    public var port: Int {
        return Int(serverSocket.listeningPort)
    }

    /// Tuning parameter to set the number of queues
    private var queueMax: Int = 4 //sensible default

    /// Tuning parameter to set the number of sockets we can accept at one time
    private var acceptMax: Int = 8 //sensible default

    ///Used to stop `accept(2)`ing while shutdown in progress to avoid spurious logs
    private let _isShuttingDownLock = DispatchSemaphore(value: 1)
    private var _isShuttingDown: Bool = false
    var isShuttingDown: Bool {
        get {
            _isShuttingDownLock.wait()
            defer {
                _isShuttingDownLock.signal()
            }
            return _isShuttingDown
        }
        set {
            _isShuttingDownLock.wait()
            defer {
                _isShuttingDownLock.signal()
            }
            _isShuttingDown = newValue
        }
    }

    /// Starts the server listening on a given port
    ///
    /// - Parameters:
    ///   - port: TCP port. See listen(2)
    ///   - handler: Function that creates the HTTP Response from the HTTP Request
    /// - Throws: Error (usually a socket error) generated
    public func start(port: Int = 0,
                      queueCount: Int = 0,
                      acceptCount: Int = 0,
                      maxReadLength: Int = 1048576,
                      keepAliveTimeout: Double = 30.0,
                      handler: @escaping HTTPRequestHandler) throws {

        // Don't let a signal generated by a broken socket kill the server
        signal(SIGPIPE, SIG_IGN);

        if queueCount > 0 {
            queueMax = queueCount
        }
        if acceptCount > 0 {
            acceptMax = acceptCount
        }
        try self.serverSocket.bindAndListen(on: port)

        pruneSocketTimer.setEventHandler { [weak self] in
            self?.connectionListenerList.prune()
        }
        #if swift(>=4.0)
            pruneSocketTimer.schedule(deadline: .now() + keepAliveTimeout,
                                     repeating: .seconds(Int(keepAliveTimeout)))
        #else
            pruneSocketTimer.scheduleRepeating(deadline: .now() + keepAliveTimeout,
                                               interval: .seconds(Int(keepAliveTimeout)))
        #endif
        pruneSocketTimer.resume()

        var readQueues = [DispatchQueue]()
        var writeQueues = [DispatchQueue]()
        let acceptQueue = DispatchQueue(label: "Accept Queue", qos: .default, attributes: .concurrent)

        let acceptSemaphore = DispatchSemaphore.init(value: acceptMax)

        for idx in 0..<queueMax {
            readQueues.append(DispatchQueue(label: "Read Queue \(idx)"))
            writeQueues.append(DispatchQueue(label: "Write Queue \(idx)"))
        }

        print("Started server on port \(self.serverSocket.listeningPort) with \(self.queueMax) serial queues of each type and \(self.acceptMax) accept sockets")

        var listenerCount = 0
        DispatchQueue.global().async {
            repeat {
                do {
                    let acceptedClientSocket = try self.serverSocket.acceptClientConnection()
                    guard let clientSocket = acceptedClientSocket else {
                        if self.isShuttingDown {
                            print("Received nil client socket - exiting accept loop")
                        }
                        break
                    }
                    print("accepted Client Socket \(clientSocket.socketfd)")
                    let streamingParser = StreamingParser(handler: handler, connectionCounter: self, keepAliveTimeout: keepAliveTimeout)
                    let readQueue = readQueues[listenerCount % self.queueMax]
                    let writeQueue = writeQueues[listenerCount % self.queueMax]
                    let listener = PoCSocketConnectionListener(socket: clientSocket, parser: streamingParser, readQueue:readQueue, writeQueue: writeQueue, maxReadLength: maxReadLength)
                    listenerCount += 1
                    acceptSemaphore.wait()
                    acceptQueue.async { [weak listener] in
                        listener?.process()
                        acceptSemaphore.signal()
                    }
                    self.connectionListenerList.add(listener)
                } catch let error {
                    print("Error accepting client connection: \(error)")
                }
            } while !self.isShuttingDown && self.serverSocket.isListening
        }
    }

    /// Stop the server and close the sockets
    public func stop() {
        isShuttingDown = true
        connectionListenerList.closeAll()
        serverSocket.shutdownAndClose()
    }

    /// Count the connections - can be used in XCTests
    public var connectionCount: Int {
        return connectionListenerList.count
    }
}

/// Collection of ConnectionListeners, wrapped with weak references, so the memory can be freed when the socket closes
class ConnectionListenerCollection {
    /// Weak wrapper class
    class WeakConnectionListener<T: AnyObject> {
        weak var value: T?
        init (_ value: T) {
            self.value = value
        }
    }

    let lock = DispatchSemaphore(value: 1)

    /// Storage for weak connection listeners
    var storage = [WeakConnectionListener<PoCSocketConnectionListener>]()

    /// Add a new connection to the collection
    ///
    /// - Parameter listener: socket manager object
    func add(_ listener: PoCSocketConnectionListener) {
        lock.wait()
        storage.append(WeakConnectionListener(listener))
        lock.signal()
    }

    /// Used when shutting down the server to close all connections
    func closeAll() {
        print("closeAll called")
        lock.wait()
        storage.filter { nil != $0.value }.forEach { $0.value?.close() }
        lock.signal()
    }

    /// Close any idle sockets and remove any weak pointers to closed (and freed) sockets from the collection
    func prune() {
        lock.wait()
        storage.filter { nil != $0.value }.forEach { $0.value?.closeIfIdleSocket() }
        storage = storage.filter { nil != $0.value }.filter { $0.value?.isOpen ?? false }
        lock.signal()
    }

    /// Count of collections
    var count: Int {
        lock.wait()
        let count = storage.filter { nil != $0.value }.count
        lock.signal()
        return count
    }
}

