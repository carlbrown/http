// This source file is part of the Swift.org Server APIs open source project
//
// Copyright (c) 2017 Swift Server API project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See http://swift.org/LICENSE.txt for license information
//

import Foundation
import Dispatch

///:nodoc:
public class PoCSocketConnectionListener: ParserConnecting {
    
    ///socket(2) wrapper object
    var socket: PoCSocket?

    ///ivar for the thing that manages the CHTTP Parser
    var parser: StreamingParser?

    ///Save the socket file descriptor so we can loook at it for debugging purposes
    var socketFD: Int32
    var shouldShutdown: Bool = false

    /// Queues for managing access to the socket without blocking the world
    let socketReaderQueue: DispatchQueue
    let socketWriterQueue: DispatchQueue

    ///Event handler for reading from the socket
    private var readerSource: DispatchSourceRead?
    
    ///Event handler for writing to the socket
    private let writerSource: DispatchSourceWrite
    
    private var stuffToWrite = [WriteCollection]()

    ///Flag to track whether writer Source is suspended or not (with lock)
    private let _writerSuspendedLock = DispatchSemaphore(value: 1)
    private var _writerSuspended: Bool = true
    var writerSuspended: Bool {
        get {
            _writerSuspendedLock.wait()
            defer {
                _writerSuspendedLock.signal()
            }
            return _writerSuspended
        }
        set {
            _writerSuspendedLock.wait()
            defer {
                _writerSuspendedLock.signal()
            }
            _writerSuspended = newValue
        }
    }

    ///Flag to track whether we're in the middle of a response or not (with lock)
    private let _responseCompletedLock = DispatchSemaphore(value: 1)
    private var _responseCompleted: Bool = false
    var responseCompleted: Bool {
        get {
            _responseCompletedLock.wait()
            defer {
                _responseCompletedLock.signal()
            }
            return _responseCompleted
        }
        set {
            _responseCompletedLock.wait()
            defer {
                _responseCompletedLock.signal()
            }
            _responseCompleted = newValue
        }
    }

    ///Flag to track whether we've received a socket error or not (with lock)
    private let _errorOccurredLock = DispatchSemaphore(value: 1)
    private var _errorOccurred: Bool = false
    var errorOccurred: Bool {
        get {
            _errorOccurredLock.wait()
            defer {
                _errorOccurredLock.signal()
            }
            return _errorOccurred
        }
        set {
            _errorOccurredLock.wait()
            defer {
                _errorOccurredLock.signal()
            }
            _errorOccurred = newValue
        }
    }
    
    ///Flag to track whether we've already called cleanup or not (with lock)
    private let _cleanupCalledLock = DispatchSemaphore(value: 1)
    private var _cleanupCalled: Bool = false
    var cleanupCalled: Bool {
        get {
            _cleanupCalledLock.wait()
            defer {
                _cleanupCalledLock.signal()
            }
            return _cleanupCalled
        }
        set {
            _cleanupCalledLock.wait()
            defer {
                _cleanupCalledLock.signal()
            }
            _cleanupCalled = newValue
        }
    }

    ///Largest number of bytes we're willing to allocate for a Read
    // it's an anti-heartbleed-type paranoia check
    private var maxReadLength: Int = 1048576

    /// initializer
    ///
    /// - Parameters:
    ///   - socket: thin PoCSocket wrapper around system calls
    ///   - parser: Manager of the CHTTPParser library
    internal init(socket: PoCSocket, parser: StreamingParser, readQueue: DispatchQueue, writeQueue: DispatchQueue, maxReadLength: Int = 0) {
        self.socket = socket
        socketFD = socket.socketfd
        socketReaderQueue = readQueue
        socketWriterQueue = writeQueue
        writerSource = DispatchSource.makeWriteSource(fileDescriptor: socket.socketfd, queue: socketWriterQueue)

        self.parser = parser
        parser.parserConnector = self
        if maxReadLength > 0 {
            self.maxReadLength = maxReadLength
        }
    }

    /// Check if socket is still open. Used to decide whether it should be closed/pruned after timeout
    public var isOpen: Bool {
        guard let socket = self.socket else {
            return false
        }
        return socket.isOpen()
    }

    /// Close the socket and free up memory unless we're in the middle of a request
    func close() {
        self.shouldShutdown = true
        
        if !self.responseCompleted && !self.errorOccurred {
            return
        }
        if (self.socket?.socketfd ?? -1) > 0 {
            self.socket?.shutdownAndClose()
        }

        //In a perfect world, we wouldn't have to clean this all up explicitly,
        // but KDE/heaptrack informs us we're in far from a perfect world

        if !(self.readerSource?.isCancelled ?? true) {
            /*
             OK, so later macOS wants `cancel()` to be called from inside the readerSource,
             otherwise, there's a very intermittent thread-dependent crash, (ask me how I know)
             so in that case, we set a Bool variable and call `activate()`.  Older macOS doesn't
             have `activate()` so we call back to calling `cancel()` directly.
             
             Linux *DOES* have activate(), but it doesn't seem to do anything at present, so we call `cancel()`
             directly in that case, too (Although I suspect that might need to change in future releases).
             */
            #if os(Linux)
                // Call Cancel directory on Linux
                self.writerSource.cancel()
                self.readerSource?.cancel()
                self.cleanup()
            #else
                if #available(OSX 10.12, *) {
                    //Set Flag and Activate the readerSource so it can run `cancel()` for us
                    self.shouldShutdown = true
                    self.writerSource.activate()
                    self.readerSource?.activate()
                } else {
                    // Fallback on earlier versions
                    self.writerSource.cancel()
                    self.readerSource?.cancel()
                    self.cleanup()
                }
            #endif
        }
    }

    /// Called by the parser to let us know that it's done with this socket
    public func closeWriter() {
        self.socketWriterQueue.async { [weak self] in
            if self?.readerSource?.isCancelled ?? true {
                self?.close()
            }
        }
    }

    /// Check if the socket is idle, and if so, call close()
    func closeIfIdleSocket() {
        if !self.responseCompleted {
            //We're in the middle of a connection - we're not idle
            return
        }
        let now = Date().timeIntervalSinceReferenceDate
        if let keepAliveUntil = parser?.keepAliveUntil, now >= keepAliveUntil {
            print("Closing idle socket \(socketFD)")
            close()
        }
    }
    
    func cleanup() {
        guard !cleanupCalled else {
            // This prevents a rare crash (~1 in 300,000) where cleanup is called from both reader and writer
            //  queues simultaneously
            return
        }
        
        //allow for memory to be reclaimed
        if let strongReaderSource = self.readerSource {
            strongReaderSource.setEventHandler(handler: nil)
            strongReaderSource.setCancelHandler(handler: nil)
        }
        if let strongParser = self.parser {
            strongParser.parserConnector = nil
        }
        
        cleanupCalled = true
    }

    /// Called by the parser to let us know that a response has started being created
    public func responseBeginning() {
        self.responseCompleted = false
    }

    /// Called by the parser to let us know that a response is complete, and we can close after timeout
    public func responseComplete() {
        self.responseCompleted = true
        self.socketWriterQueue.async { [weak self] in
            if self?.readerSource?.isCancelled ?? true {
                self?.close()
            }
        }
    }
    
    /// Called by the parser to let us know that a response is complete and we should close the socket
    public func responseCompleteCloseWriter() {
        self.responseCompleted = true
        self.socketWriterQueue.async { [weak self] in
            self?.close()
        }
    }

    /// Starts reading from the socket and feeding that data to the parser
    public func process() {
        let tempReaderSource: DispatchSourceRead
        //Make sure we have a socket here.  Don't use guard so that
        //  we don't encourage strongSocket to be used in the
        //  event handler, which could cause a leak
        if let strongSocket = socket {
            do {
                try strongSocket.setBlocking(mode: false)
                tempReaderSource = DispatchSource.makeReadSource(fileDescriptor: strongSocket.socketfd,
                                                                     queue: socketReaderQueue)
            } catch {
                print("Socket cannot be set to Blocking in process(): \(error)")
                return
            }
        } else {
            print("Socket is nil in process()")
            return
        }

        tempReaderSource.setEventHandler { [weak self] in
            guard let strongSelf = self else {
                return
            }
            guard strongSelf.socket?.socketfd ?? -1 > 0 else {
                strongSelf.readerSource?.cancel()
                strongSelf.cleanup()
                return
            }
            guard !strongSelf.shouldShutdown else {
                strongSelf.readerSource?.cancel()
                strongSelf.cleanup()
                return
            }
            
            if let strongSocket = strongSelf.socket {
                var length = 1 //initial value
                do {
                    if strongSocket.socketfd > 0 {
                        var maxLength: Int = Int(strongSelf.readerSource?.data ?? 0)
                        if (maxLength > strongSelf.maxReadLength) || (maxLength <= 0) {
                            maxLength = strongSelf.maxReadLength
                        }
                        var readBuffer: UnsafeMutablePointer<Int8> = UnsafeMutablePointer<Int8>.allocate(capacity: maxLength)
                        length = try strongSocket.socketRead(into: &readBuffer, maxLength:maxLength)
                        if length > 0 {
                            strongSelf.responseCompleted = false
                            
                            let data = Data(bytes: readBuffer, count: length)
                            let numberParsed = strongSelf.parser?.readStream(data:data) ?? 0
                            
                            if numberParsed != data.count {
                                print("Error: wrong number of bytes consumed by parser (\(numberParsed) instead of \(data.count)")
                            }
                        }
                        readBuffer.deallocate(capacity: maxLength)
                    } else {
                        print("bad socket FD while reading")
                        length = -1
                    }
                } catch {
                    //print("ReaderSource Event Error: \(error)")
                    strongSelf.readerSource?.cancel()
                    strongSelf.errorOccurred = true
                    strongSelf.close()
                }
                if length == 0 {
                    if (errno == EWOULDBLOCK || errno == EAGAIN) {
                        //Nothing to do - wait for us to get triggered again
                        return
                    }
                    //print("ReaderSource Read count zero. Cancelling.")
                    strongSelf.readerSource?.cancel()
                }
                if length < 0 {
                    //print("ReaderSource Read count negative. Closing.")
                    strongSelf.errorOccurred = true
                    strongSelf.readerSource?.cancel()
                    strongSelf.close()
                }
            } else {
                //print("ReaderSource Read found nil socket. Closing.")
                strongSelf.errorOccurred = true
                strongSelf.readerSource?.cancel()
                strongSelf.close()
            }
        }
        
        tempReaderSource.setCancelHandler { [weak self] in
            if let strongSelf = self {
                strongSelf.close() //close if we can
            }
        }
        
        self.readerSource = tempReaderSource
        
        writerSource.setEventHandler { [ weak self ] in
            guard let strongSelf = self else {
                return
            }
            
            if (strongSelf.shouldShutdown) {
                strongSelf.writerSource.cancel()
            }
            
//            if strongSelf.writerSuspended {
//                //We're obviously not suspended now
//                strongSelf.writerSuspended = false
//            }
            
            if strongSelf.stuffToWrite.count > 0 {
                let thingToWrite = strongSelf.stuffToWrite.removeFirst()
                strongSelf.write(thingToWrite.data, completion: thingToWrite.completion)
            } else {
                strongSelf.writerSuspended = true
                //strongSelf.writerSource.suspend()
            }
        }
        
        writerSource.setCancelHandler { [weak self] in
            if let strongSelf = self {
                if let readerSource = strongSelf.readerSource {
                    if !readerSource.isCancelled {
                        readerSource.cancel()
                    }
                }
            }
        }
        
        self.readerSource?.resume()
        self.writerSource.resume()
    }
    
    /// Called by the parser to give us data to send back out of the socket
    ///
    /// - Parameter bytes: Data object to be queued to be written to the socket
    public func queueSocketWrite(_ bytes: Data, completion:@escaping (Result) -> Void) {
        self.socketWriterQueue.async { [weak self] in
            self?.stuffToWrite.append(WriteCollection(data: bytes, completion: completion))
        }
//        if writerSuspended {
//            writerSuspended = false
//            writerSource.resume()
//        }
    }

    /// Write data to a socket. Should be called in an `async` block on the `socketWriterQueue`
    ///
    /// - Parameter data: data to be written
    private func write(_ data: Data, completion:@escaping (Result) -> Void) {
        do {
            var written: Int = 0
            var offset = 0

            while written < data.count && !errorOccurred {
                try data.withUnsafeBytes { (ptr: UnsafePointer<UInt8>) in
                    if let strongSocket = socket {
                        let result = try strongSocket.socketWrite(from: ptr + offset, bufSize:
                            data.count - offset)
                        if result < 0 {
                            //print("Received broken write socket indication")
                            errorOccurred = true
                        } else if result == 0 {
                            if errno == EAGAIN || errno == EWOULDBLOCK {
                                //Put what's left back on the front of the queue and exit
                                //FIXME: test partially written case
                                self.stuffToWrite.insert(WriteCollection(data:Data(),completion: completion), at: 0)
                                return
                            }
                        } else {
                            written += result
                        }
                    } else {
                        //print("Socket unexpectedly nil during write")
                        errorOccurred = true
                    }
                }
                offset = data.count - written
                /*
                if (offset > 0) {
                    print("Socket write left remainder. Retrying \(offset) bytes")
                }
                 */
            }
            if errorOccurred {
                close()
                completion(.error(PoCSocketError.UnknownError))
                return
            }
        } catch {
            print("Received write socket error: \(error)")
            errorOccurred = true
            close()
            completion(.error(error))
            return
        }
        completion(.ok)
    }
    
    internal class WriteCollection {
        let data: Data
        let completion: (Result) -> Void
        
        init(data: Data, completion: @escaping (Result) -> Void) {
            self.data = data
            self.completion = completion
        }
    }
}
