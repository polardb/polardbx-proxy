/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.proxy.net;

import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.utils.AutoCloseableContainer;
import com.alibaba.polardbx.proxy.utils.FastBufferPool;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

// Note: Leak check on connection is useless, because selector always hold a reference of this object.
public abstract class NIOConnection implements AutoCloseable, Comparable<NIOConnection> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnection.class);
    private static final AtomicLong INTERNAL_ID_GENERATOR = new AtomicLong(0);
    private final long internalId = INTERNAL_ID_GENERATOR.incrementAndGet();

    // for client send_buf is small, recv_buf is large
    // for server send_buf is large, recv_buf is small
    private static final int socketLargeBuffer = 64 * 1024;
    private static final int socketSmallBuffer = 16 * 1024;

    public static void ensureMinimumTcpBuffer(Socket s, int sendBuffer, int receiveBuffer) throws SocketException {
        if (s.getSendBufferSize() < sendBuffer) {
            s.setSendBufferSize(sendBuffer);
        }
        if (s.getReceiveBufferSize() < receiveBuffer) {
            s.setReceiveBufferSize(receiveBuffer);
        }
    }

    public static void ensureMinimumTcpBuffer(Socket s, boolean client, int smallBuffer, int largeBuffer)
        throws SocketException {
        if (client) {
            // small send large recv for client
            ensureMinimumTcpBuffer(s, smallBuffer, largeBuffer);
        } else {
            // large send small recv for server
            ensureMinimumTcpBuffer(s, largeBuffer, smallBuffer);
        }
    }

    public static void ensureMinimumTcpBuffer(Socket s, boolean client) throws SocketException {
        ensureMinimumTcpBuffer(s, client, socketSmallBuffer, socketLargeBuffer);
    }

    /**
     * Connect target TCP with block mode and timeout.
     *
     * @param address target address
     * @param timeout timeout in ms
     * @return socket channel
     * @throws IOException if connect failed
     */
    public static SocketChannel connectBlocking(SocketAddress address, int timeout) throws IOException {
        final long startNanos = System.nanoTime();
        final SocketChannel c = SocketChannel.open();
        try {
            c.setOption(StandardSocketOptions.TCP_NODELAY, true);
            c.socket().connect(address, timeout);
            c.configureBlocking(false); // Switch to non-block mode.
            final long endNanos = System.nanoTime();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("NIO-Connection {} to {} connect success, connect time: {} us.", c.getLocalAddress(),
                    c.getRemoteAddress(), (endNanos - startNanos) / 1000L);
            }
            return c;
        } catch (Throwable e) {
            final long endNanos = System.nanoTime();
            LOGGER.info("NIO-Connection to {} connect error with timeout {} ms. real: {} us.",
                address, timeout, (endNanos - startNanos) / 1000L);
            // Close and free the resources.
            try {
                final Socket socket = c.socket();
                if (socket != null) {
                    socket.close();
                }
            } catch (Throwable ignore) {
            }
            try {
                c.close();
            } catch (Throwable ignore) {
            }
            throw e;
        }
    }

    /**
     * Connect target TCP with non-block mode.
     *
     * @param address target address
     * @return socket channel
     * @throws IOException if connect failed
     */
    public static SocketChannel connectNonBlocking(SocketAddress address) throws IOException {
        final SocketChannel c = SocketChannel.open();
        try {
            c.setOption(StandardSocketOptions.TCP_NODELAY, true);
            c.configureBlocking(false); // Switch to non-block mode.
            c.connect(address);
            return c;
        } catch (Throwable e) {
            // Close and free the resources.
            try {
                final Socket socket = c.socket();
                if (socket != null) {
                    socket.close();
                }
            } catch (Throwable ignore) {
            }
            try {
                c.close();
            } catch (Throwable ignore) {
            }
            throw e;
        }
    }

    // target socket channel
    private final SocketChannel channel;
    @Getter
    protected final NIOProcessor processor;

    private enum State {
        ConnectingNotRegistered,
        ConnectingRegistered,
        ConnectedNotRegistered,
        ConnectedRegistered
    }

    // NIO and TCP flags, writing is protected by synchronization of this object
    private volatile State state;
    private final AtomicBoolean tcpClosed = new AtomicBoolean(false);

    // valid after registered(set once and never invalid)
    private SelectionKey processKey = null;
    // lock for ops on selection key
    private final ReentrantLock keyLock = new ReentrantLock();

    // packet info
    private final int maxPacketSize;
    private final int reservedBufferSize;

    // this lock to prevent conflict between cleanup & read
    private final ReentrantLock readLock = new ReentrantLock();
    // allocate when needed, and managed by reference count
    private FastBufferPool.BufferHolder readBufferHolder = null;
    private ByteBuffer readBuffer = null;
    private int readBufferBaseOffset = 0;
    private int readBufferCapacity = 0;
    private int readBufferConsumed = 0;

    // this lock to prevent conflict between cleanup & write
    private final ReentrantLock writeLock = new ReentrantLock();
    private final Queue<Slice> writeQueue = new ConcurrentLinkedQueue<>();
    private Slice lastWrite = null;
    private final AtomicBoolean writeBlocking = new AtomicBoolean(false);
    private final List<WeakReference<Runnable>> writeResumeListener = new CopyOnWriteArrayList<>();

    // for idle time check
    private final AtomicLong lastSend = new AtomicLong(0);
    private final AtomicLong lastRecv = new AtomicLong(0);

    // for perf check
    private final AtomicLong recvBytes = new AtomicLong(0);
    private final AtomicLong sendBytes = new AtomicLong(0);

    /**
     * Usage:
     * 1. construct with socket channel and processor
     * 2. processor.postRegister(client);
     */
    public NIOConnection(SocketChannel channel, NIOProcessor processor, boolean connected, int maxPacketSize,
                         int reservedBufferSize) {
        this.channel = channel;
        this.processor = processor;
        this.state = connected ? State.ConnectedNotRegistered : State.ConnectingNotRegistered;
        this.maxPacketSize = maxPacketSize;
        this.reservedBufferSize = reservedBufferSize;
        // reset idle time
        final long nanos = System.nanoTime();
        lastSend.setRelease(nanos);
        lastRecv.setRelease(nanos);
        // set socket buffer when connected
        if (connected && FastConfig.tcpEnsureMinimumBuffer) {
            try {
                ensureMinimumTcpBuffer(channel.socket(), true);
            } catch (Throwable t) {
                LOGGER.warn("Failed to ensure minimum tcp buffer.", t);
            }
        }
    }

    public long idleTime() {
        final long snd = lastSend.getAcquire(), rcv = lastRecv.getAcquire();
        final long nowNanos = System.nanoTime();
        return Math.max(0, Math.min(nowNanos - snd, nowNanos - rcv));
    }

    public boolean isValid() {
        return channel.isOpen() && State.ConnectedRegistered == state;
    }

    public InetSocketAddress remoteAddress() {
        final Socket socket = channel.socket();
        return new InetSocketAddress(socket.getInetAddress(), socket.getPort());
    }

    private void clearSelectionKey() {
        keyLock.lock();
        try {
            final SelectionKey key = this.processKey;
            if (key != null && key.isValid()) {
                assert processor != null;
                key.attach(null);
                key.cancel();
                processor.getPerfCollection().getSocketCount().getAndDecrement();
            }
        } catch (Throwable ignore) {
        } finally {
            keyLock.unlock();
        }
    }

    private boolean closeSocket() {
        LOGGER.trace("{} close socket.", this);
        clearSelectionKey();
        final boolean socketClosed;
        final Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (Throwable e) {
            }
            socketClosed = socket.isClosed();
        } else {
            socketClosed = true;
        }
        try {
            channel.close();
        } catch (Throwable ignore) {
        }
        return socketClosed && !channel.isOpen();
    }

    private void cleanup() {
        readLock.lock();
        try {
            if (readBufferHolder != null) {
                readBufferHolder.subReference();
                readBufferHolder = null;
                readBuffer = null;
                readBufferBaseOffset = 0;
                readBufferCapacity = 0;
                readBufferConsumed = 0;
            }
        } finally {
            readLock.unlock();
        }

        writeLock.lock();
        try {
            Slice top;
            while ((top = writeQueue.poll()) != null) {
                top.close();
            }
            lastWrite = null;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            try {
                if (!tcpClosed.getPlain()) {
                    // this is idempotent
                    closeSocket(); // ignore the result and free the resource anyway
                    if (tcpClosed.compareAndSet(false, true)) {
                        // cleanup once
                        cleanup();
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("{} close failed", this, t);
            }
        }
    }

    protected abstract void onEstablished();

    // only package accessible
    synchronized void register(Selector selector) throws IOException {
        try {
            switch (state) {
            case ConnectingNotRegistered:
                processKey = channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_CONNECT, this);
                state = State.ConnectingRegistered;
                break;

            case ConnectedNotRegistered:
                processKey = channel.register(selector, SelectionKey.OP_READ, this);
                state = State.ConnectedRegistered;
                break;

            default:
                throw new IllegalStateException("invalid state: " + state);
            }
            processor.getPerfCollection().getSocketCount().getAndIncrement();
        } catch (Throwable t) {
            LOGGER.error("{} register failed, and force close it", this, t);
            // force close this if failed to register
            close();
            throw t;
        } finally {
            // full fence needed
            if (tcpClosed.get()) {
                // socket, channel and buffer already closed, so just clear selection key
                clearSelectionKey();
            } else if (state == State.ConnectedRegistered) {
                onEstablished();
            }
        }
    }

    // -1 means not enough header
    protected abstract int probeLength(ByteBuffer buf, int offset, int length);

    // Caution: Packets will be auto freed if not taken.
    protected abstract void onPacket(AutoCloseableContainer<Slice> packets);

    protected abstract void onFatalError(Throwable t);

    // resume to monitor readable state
    public void enableRead() {
        keyLock.lock();
        try {
            SelectionKey key = this.processKey;
            if ((key.interestOps() & SelectionKey.OP_READ) == 0) {
                LOGGER.debug("{} resume read.", this);
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            }
        } finally {
            keyLock.unlock();
        }
        // wakeup anyway in case we invoke from other thread
        processKey.selector().wakeup();
    }

    // disable read for flow control
    public void disableRead() {
        LOGGER.debug("{} pause read.", this);
        keyLock.lock();
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        } finally {
            keyLock.unlock();
        }
    }

    private void readByEvent() throws IOException {
        // record time use fast nanos
        lastRecv.set(System.nanoTime());

        // this always in single thread
        boolean EOF = false;
        try (final AutoCloseableContainer<Slice> batch = new AutoCloseableContainer<>(32)) {
            readLock.lock();
            try {
                // to prevent reallocate after close
                if (tcpClosed.getPlain()) { // in lock so plain read
                    throw new RuntimeException(this + " closed.");
                }

                // select one buffer for recv
                if (null == readBuffer) {
                    assert null == readBufferHolder;
                    // try fast buffer first
                    readBufferHolder = processor.getBufferPool().allocateAndAddReference();
                    if (readBufferHolder != null) {
                        readBuffer = readBufferHolder.duplicateBuffer();
                        readBufferBaseOffset = readBuffer.position();
                        readBufferCapacity = readBuffer.limit() - readBufferBaseOffset;
                    } else {
                        readBuffer = ByteBuffer.allocate(processor.getBufferPool().getBlockSize());
                        readBufferBaseOffset = 0;
                        readBufferCapacity = readBuffer.capacity();
                    }
                    // force set to little endian
                    readBuffer.order(ByteOrder.LITTLE_ENDIAN);
                }

                // recv
                final int got = channel.read(readBuffer);
                if (got < 0) {
                    EOF = true;
                } else if (got > 0) {
                    // record perf data
                    recvBytes.getAndAdd(got);

                    // probe body size
                    int packetSize, readable;
                    boolean checkRest = true;
                    while ((packetSize = probeLength(readBuffer, readBufferBaseOffset + readBufferConsumed,
                        readable = (readBuffer.position() - readBufferBaseOffset - readBufferConsumed))) > 0) {
                        if (readable >= packetSize) {
                            // full packet got
                            final Slice slice;
                            if (readBufferHolder != null) {
                                slice = new Slice(readBufferHolder, readBufferConsumed, packetSize);
                            } else {
                                assert 0 == readBufferBaseOffset;
                                slice = new Slice(readBuffer, readBufferConsumed, packetSize);
                            }
                            try {
                                batch.add(slice);
                            } catch (Throwable t) {
                                slice.close();
                                throw t;
                            }
                            readBufferConsumed += packetSize;
                            // then next
                        } else if (packetSize > readBufferCapacity - readBufferConsumed) {
                            // not enough for next packet, allocate new one
                            final FastBufferPool pool = processor.getBufferPool();
                            final FastBufferPool.BufferHolder newHolder;
                            final ByteBuffer newBuffer;
                            final int newBase;
                            final int newCapcity;
                            if (packetSize <= pool.getBlockSize()) {
                                // allocate new block
                                newHolder = pool.allocateAndAddReference();
                                if (newHolder != null) {
                                    newBuffer = newHolder.duplicateBuffer();
                                    newBase = newBuffer.position();
                                    newCapcity = newBuffer.limit() - newBase;
                                } else {
                                    newBuffer = ByteBuffer.allocate(pool.getBlockSize());
                                    newBase = 0;
                                    newCapcity = newBuffer.capacity();
                                }
                            } else {
                                // large block needed
                                newHolder = null;
                                newBuffer = ByteBuffer.allocate(Math.min(maxPacketSize, 2 * packetSize));
                                newBase = 0;
                                newCapcity = newBuffer.capacity();
                            }
                            newBuffer.put(readBuffer.duplicate().position(readBufferBaseOffset + readBufferConsumed)
                                .limit(readBuffer.position()));
                            newBuffer.order(ByteOrder.LITTLE_ENDIAN);
                            // swap buffer
                            if (readBufferHolder != null) {
                                readBufferHolder.subReference();
                            }
                            readBufferHolder = newHolder;
                            readBuffer = newBuffer;
                            readBufferBaseOffset = newBase;
                            readBufferCapacity = newCapcity;
                            readBufferConsumed = 0; // reset to head
                            checkRest = false;
                            break;
                        } else {
                            // or simply more data needed and buffer enough
                            checkRest = false;
                            break;
                        }
                    }

                    if (checkRest) {
                        // check rest buffer
                        final FastBufferPool pool = processor.getBufferPool();
                        final int blockSize = pool.getBlockSize();
                        if (readBufferCapacity - readBufferConsumed < Math.min(blockSize / 2, reservedBufferSize)) {
                            // allocate new buffer
                            final FastBufferPool.BufferHolder newHolder = pool.allocateAndAddReference();
                            final ByteBuffer newBuffer;
                            final int newBase;
                            final int newCapcity;
                            if (newHolder != null) {
                                newBuffer = newHolder.duplicateBuffer();
                                newBase = newBuffer.position();
                                newCapcity = newBuffer.limit() - newBase;
                            } else {
                                newBuffer = ByteBuffer.allocate(blockSize);
                                newBase = 0;
                                newCapcity = newBuffer.capacity();
                            }
                            newBuffer.put(readBuffer.duplicate().position(readBufferBaseOffset + readBufferConsumed)
                                .limit(readBuffer.position()));
                            newBuffer.order(ByteOrder.LITTLE_ENDIAN);
                            // swap buffer
                            if (readBufferHolder != null) {
                                readBufferHolder.subReference();
                            }
                            readBufferHolder = newHolder;
                            readBuffer = newBuffer;
                            readBufferBaseOffset = newBase;
                            readBufferCapacity = newCapcity;
                            readBufferConsumed = 0; // reset to head
                        }
                    }
                }
            } finally {
                readLock.unlock();
            }

            if (!batch.isEmpty()) {
                // with the protection of try-with-resource, slice will be freed if not taken
                onPacket(batch);
            }
        } catch (IOException e) {
            if (e.getMessage().equals("Connection reset by peer")) {
                // just ignore peer reset
                EOF = true;
                if (0 == recvBytes.getAcquire()) {
                    LOGGER.debug("Connection reset when nothing send from client, close connection", e);
                } else if (LOGGER.isDebugEnabled()) {
                    LOGGER.info("Connection reset when something send from client, close connection", e);
                } else {
                    LOGGER.info("Connection reset when something send from client, close connection {}", this);
                }
            } else {
                throw e;
            }
        } finally {
            // close connection if EOF found
            if (EOF) {
                try {
                    close();
                } catch (Throwable t) {
                    // not throw here, and keep the original exception in try block
                    LOGGER.error("Failed to close connection when EOF", t);
                }
            }
        }
    }

    // Caution: Must hold the write lock.
    private boolean write0() throws IOException {
        // record time use fast nanos
        lastSend.set(System.nanoTime());

        Slice top;
        while ((top = writeQueue.peek()) != null) {
            // gen buffer for write
            final ByteBuffer buf = top.duplicateBuffer();
            buf.position(buf.position() + top.getConsumed());
            buf.limit(buf.position() + top.getValid());
            final int written = channel.write(buf);

            // record perf data
            if (written > 0) {
                sendBytes.getAndAdd(written);
            }

            if (buf.hasRemaining()) {
                // update pos
                top.consume(written);
                return false;
            } else {
                if (lastWrite == top) {
                    // clear last
                    lastWrite = null;
                }
                final Slice removed = writeQueue.remove();
                assert removed == top;
                removed.close();
            }
        }
        return true;
    }

    private void enableWrite() {
        keyLock.lock();
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            writeBlocking.setRelease(true);
        } finally {
            keyLock.unlock();
        }
        // wakeup anyway in case we invoke from other thread
        processKey.selector().wakeup();
    }

    private void disableWrite() {
        keyLock.lock();
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            writeBlocking.setRelease(false);
        } finally {
            keyLock.unlock();
        }

        // notify
        for (final WeakReference<Runnable> ref : writeResumeListener) {
            final Runnable runnable = ref.get();
            if (runnable != null) {
                runnable.run();
            }
        }
        // clean
        writeResumeListener.removeIf(ref -> null == ref.get());
    }

    public boolean isWriteBlocking() {
        return writeBlocking.getAcquire();
    }

    public void registerWriteResumeListener(final Runnable runnable) {
        final AtomicBoolean found = new AtomicBoolean(false);
        writeResumeListener.removeIf(ref -> {
            final Runnable r = ref.get();
            if (null == r) {
                return true;
            }
            if (runnable == r) {
                found.setPlain(true);
            }
            return false;
        });
        if (!found.getPlain()) {
            writeResumeListener.add(new WeakReference<>(runnable));
        }
    }

    public void removeWriteResumeListener(final Runnable runnable) {
        writeResumeListener.removeIf(ref -> null == ref.get() || runnable == ref.get());
    }

    // Caution: Invoke with try-with-resource on packets container to ensure resource free.
    //          Packets which used will be taken from container.
    public void write(AutoCloseableContainer<Slice> packets) throws IOException {
        writeLock.lock();
        try {
            if (tcpClosed.getPlain()) { // in lock so plain read
                throw new RuntimeException(this + " closed.");
            }

            // push packets to queue
            Throwable err = null;
            for (Slice s : packets) {
                try {
                    // add to queue, or leave and free in finally
                    if (null == err && (null == lastWrite || !lastWrite.merge(s))) {
                        writeQueue.add(s);
                        lastWrite = s;
                        s = null; // consumed
                    }
                } catch (Throwable t) {
                    err = t;
                } finally {
                    if (s != null) {
                        s.close();
                    }
                }
            }
            packets.clear(); // all merged or consumed, just clear container

            // abort if any error occurs
            if (err != null) {
                throw new RuntimeException(err.toString(), err);
            }

            if ((processKey.interestOps() & SelectionKey.OP_WRITE) == 0 && !write0()) {
                // write not blocking then try direct write and has remaining
                enableWrite();
            }
        } finally {
            writeLock.unlock();
        }
    }

    // Caution: Packet will close anyway.
    public void write(Slice packet) throws IOException {
        try {
            writeLock.lock();
            try {
                if (tcpClosed.getPlain()) { // in lock so plain read
                    throw new RuntimeException(this + " closed.");
                }

                // push packets to queue
                if (null == lastWrite || !lastWrite.merge(packet)) {
                    writeQueue.add(packet);
                    lastWrite = packet;
                    packet = null; // consumed
                }

                if ((processKey.interestOps() & SelectionKey.OP_WRITE) == 0 && !write0()) {
                    // write not blocking then try direct write and has remaining
                    enableWrite();
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            if (packet != null) {
                packet.close();
            }
        }
    }

    private void writeByEvent() throws IOException {
        writeLock.lock();
        try {
            if (tcpClosed.getPlain()) { // in lock so plain read
                throw new RuntimeException(this + " closed.");
            }

            if (write0()) {
                // all data written
                disableWrite();
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void disableConnect() {
        keyLock.lock();
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
        } finally {
            keyLock.unlock();
        }
    }

    private synchronized void connectByEvent() throws IOException {
        if (state != State.ConnectingRegistered) {
            throw new RuntimeException("Unknown state: " + state + " when OP_CONNECT occurs.");
        }

        // close check(close updated in sync block, so plain read here)
        if (tcpClosed.getPlain()) {
            throw new RuntimeException(this + " closed.");
        }

        if (channel.isConnectionPending()) {
            final boolean connected = channel.finishConnect();
            if (connected) {
                // clear ON_CONNECT flag and change state
                disableConnect();
                state = State.ConnectedRegistered;
                // update socket buffer if needed
                if (FastConfig.tcpEnsureMinimumBuffer) {
                    try {
                        ensureMinimumTcpBuffer(channel.socket(), true);
                    } catch (Throwable t) {
                        LOGGER.warn("Failed to ensure minimum tcp buffer.", t);
                    }
                }
                onEstablished();
            } else {
                // force close when connect failed
                close();
            }
        }
    }

    // only package accessible
    void event(int readyOps) {
        try {
            if ((readyOps & SelectionKey.OP_READ) != 0) {
                processor.getPerfCollection().getReadCount().getAndIncrement();
                readByEvent();
            } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                processor.getPerfCollection().getWriteCount().getAndIncrement();
                writeByEvent();
            } else if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                processor.getPerfCollection().getConnectCount().getAndIncrement();
                connectByEvent();
            } else {
                throw new RuntimeException("Unknown event readyOps: " + readyOps);
            }
        } catch (Throwable t) {
            onFatalError(t);
        }
    }

    private String cachedConnectionString;

    public String connectionString() throws IOException {
        String tmp = cachedConnectionString;
        if (null == tmp) {
            synchronized (this) {
                tmp = cachedConnectionString;
                if (null == tmp) {
                    try {
                        final SocketAddress local = channel.getLocalAddress();
                        final SocketAddress remote = channel.getRemoteAddress();
                        if (local != null && remote != null) {
                            // cache only valid
                            tmp = cachedConnectionString = local + " <-> " + remote;
                        } else {
                            tmp = local + " <-> " + remote;
                        }
                    } catch (Throwable t) {
                        tmp = "unknown";
                    }
                }
            }
        }
        return tmp;
    }

    @Override
    public int compareTo(@NotNull NIOConnection o) {
        return Long.compare(internalId, o.internalId);
    }

    @Override
    public String toString() {
        try {
            return "NIO-Connection " + connectionString();
        } catch (Throwable ignore) {
            return "NIO-Connection";
        }
    }
}
