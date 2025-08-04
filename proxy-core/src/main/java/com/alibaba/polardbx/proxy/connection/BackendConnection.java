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

package com.alibaba.polardbx.proxy.connection;

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.callback.ResultCallback;
import com.alibaba.polardbx.proxy.connection.configs.ReadOnlyConfigs;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.MysqlContext;
import com.alibaba.polardbx.proxy.net.NIOConnection;
import com.alibaba.polardbx.proxy.net.NIOProcessor;
import com.alibaba.polardbx.proxy.protocol.command.ComQuery;
import com.alibaba.polardbx.proxy.protocol.common.MysqlClientState;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.protocol.handler.BackendAuthenticator;
import com.alibaba.polardbx.proxy.protocol.handler.result.OkErrResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.StmtPrepareResultHandler;
import com.alibaba.polardbx.proxy.utils.CaseInsensitiveString;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class BackendConnection extends MysqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackendConnection.class);
    public static final Set<BackendConnection> CONNECTIONS = new ConcurrentSkipListSet<>();

    private final AtomicBoolean resourceClosed = new AtomicBoolean(false);

    // connection context
    @Getter
    private final AtomicReference<BackendContext> contextReference = new AtomicReference<>(null);
    private final FutureTask<Boolean> login =
        new FutureTask<>(() -> {
            final BackendContext context = contextReference.getAcquire();
            return context != null && MysqlClientState.Authenticated == context.getState();
        });

    // now handler
    private volatile BackendAuthenticator authenticator;
    private volatile ResultHandler nowResultHandler;
    // queued handler, protected by synchronize on object login
    private final Queue<ResultHandler> resultHandlers = new LinkedList<>();

    // pending send data, protected by synchronize on object login
    private final Queue<byte[]> pendingData = new LinkedList<>();

    // slave mark
    @Getter
    private boolean slave;

    // global RO configs(ref from connection pool)
    private ReadOnlyConfigs readOnlyConfigs;
    // global variables ref
    private Map<CaseInsensitiveString, String> globalVariables;

    private BackendConnection(SocketChannel channel, NIOProcessor processor, boolean connected, String username,
                              String encryptedPassword, String database) {
        super(channel, processor, connected);
        // last line of constructor, so no need to catch and close it
        authenticator =
            new BackendAuthenticator(remoteAddress(), contextReference, username, encryptedPassword, database);

        // add to global set
        CONNECTIONS.add(this);
    }

    public void setPoolInfo(boolean slave, ReadOnlyConfigs readOnlyConfigs,
                            Map<CaseInsensitiveString, String> globalVariables) {
        this.slave = slave;
        this.readOnlyConfigs = readOnlyConfigs;
        this.globalVariables = globalVariables;
    }

    @Override
    protected void onEstablished() {
        // nothing to do
    }

    @Override
    protected boolean handleAndTakePacket(Slice packet, Decoder decoder, Encoder encoder) throws Exception {
        final boolean taken;
        final BackendAuthenticator auth = authenticator;
        if (auth != null) {
            taken = auth.handleAndTakePacket(packet, decoder, encoder);
            final BackendContext context = contextReference.getAcquire();
            if (context != null && context.getState().isAuthChecked()) {
                // set global RO configs and global variables
                context.setConfigsAndGlobalVariables(readOnlyConfigs, globalVariables);
                // notify waits
                login.run();
                // finish before clear authenticator
                auth.handleFinish();
                auth.close();

                // use resourceClosed as synchronize lock
                synchronized (resourceClosed) {
                    // modify authenticator in lock, this ensures pending data put into correct place(send or queue)
                    authenticator = null;

                    // early abort
                    if (resourceClosed.getPlain()) {
                        pendingData.clear();
                        throw new IllegalStateException("connection is closed");
                    }

                    // flush all pending sending data
                    if (MysqlClientState.Authenticated == context.getState()) {
                        byte[] data;
                        while ((data = pendingData.poll()) != null) {
                            encoder.pkt(data);
                        }
                    } else {
                        pendingData.clear();
                    }
                }
            }
        } else {
            ResultHandler handler = nowResultHandler;
            if (null == handler) {
                // use resourceClosed as synchronize lock
                synchronized (resourceClosed) {
                    handler = nowResultHandler;
                    if (null == handler) {
                        // check closed before assign
                        if (resourceClosed.getPlain()) {
                            throw new IllegalStateException("connection is closed");
                        }
                        handler = resultHandlers.poll();
                        if (null == handler) {
                            throw new RuntimeException("No more result handler. packet: " + packet.toString());
                        }
                        nowResultHandler = handler;
                    }
                }
            }
            taken = handler.handleAndTakePacket(packet, decoder);
            if (handler.isDone()) {
                // finish before clear handler
                handler.handleFinish();
                handler.close();
                // use resourceClosed as synchronize lock
                synchronized (resourceClosed) {
                    // switch if still current handler
                    if (nowResultHandler == handler) {
                        // check closed before assign
                        if (resourceClosed.getPlain()) {
                            throw new IllegalStateException("connection is closed");
                        }
                        // null or more result
                        nowResultHandler = handler.hasMore();
                    }
                }
            }
        }
        return taken;
    }

    @Override
    protected void handleFinish() {
        // invoke handle finish with optimistic read
        final BackendAuthenticator auth = authenticator;
        if (auth != null) {
            auth.handleFinish();
        }
        final ResultHandler handler = nowResultHandler;
        if (handler != null) {
            handler.handleFinish();
        }
        // close connection if any error occurs
        final BackendContext context = contextReference.getAcquire();
        if (context != null && MysqlClientState.Closed == context.getState()) {
            close();
        }
    }

    @Override
    protected void onFatalError(Throwable t) {
        LOGGER.error("fatal error on {}", this, t);
        close();
    }

    @Override
    public void close() {
        final boolean needClose;
        final BackendAuthenticator auth;
        final List<ResultHandler> handlers;
        // use resourceClosed as synchronize lock
        synchronized (resourceClosed) {
            if (resourceClosed.compareAndSet(false, true)) {
                needClose = true;
                // move all handlers and free outside
                auth = authenticator;
                authenticator = null;
                handlers = new ArrayList<>(resultHandlers);
                resultHandlers.clear();
                if (nowResultHandler != null) {
                    handlers.add(nowResultHandler);
                    nowResultHandler = null;
                }
                // free pending
                pendingData.clear();
            } else {
                needClose = false;
                assert null == authenticator;
                assert null == nowResultHandler;
                assert resultHandlers.isEmpty();
                assert pendingData.isEmpty();
                auth = null;
                handlers = null;
            }
        }

        if (needClose) {
            // close all handlers and context in async task to prevent any potential deadlock
            ProxyExecutor.getInstance().getExecutor().submit(() -> {
                try {
                    if (auth != null) {
                        auth.close();
                    }
                    for (ResultHandler handler : handlers) {
                        do {
                            handler.close();
                            handler = handler.hasMore();
                        } while (handler != null);
                    }
                    handlers.clear();
                    // when authenticator is closed, context will never rebuild
                    final BackendContext context = contextReference.getAcquire();
                    if (context != null) {
                        context.close();
                    }
                } catch (Throwable t) {
                    LOGGER.error("close connection {} free resources failed", this, t);
                }
            });
        }

        // remove from global set
        CONNECTIONS.remove(this);

        // finalize the TCP close
        super.close();
    }

    // Caution: Packet and handler will close anyway.
    public void forward(@NotNull Slice packet, @Nullable ResultHandler handler) throws IOException {
        try {
            // use resourceClosed as synchronize lock
            synchronized (resourceClosed) {
                if (resourceClosed.getPlain()) {
                    throw new IllegalStateException("connection is closed");
                }
                if (authenticator != null) {
                    // auth not finished, just push requests to pending
                    pendingData.add(packet.dump());
                } else {
                    write(packet);
                    packet = null; // now packet is consumed
                }
                if (handler != null) {
                    resultHandlers.add(handler);
                    handler = null;
                }
            }
        } catch (Throwable t) {
            // any send error may corrupt then protocol, close connection
            close();
            throw t;
        } finally {
            if (packet != null) {
                packet.close();
            }
            if (handler != null) {
                handler.close();
            }
        }
    }

    public boolean isGood() {
        final BackendContext context = contextReference.getAcquire();
        return context != null && context.getState() == MysqlClientState.Authenticated && !resourceClosed.getAcquire();
    }

    public void waitLogin(long timeout, TimeUnit unit)
        throws ExecutionException, InterruptedException, TimeoutException {
        final boolean result = login.get(timeout, unit);
        if (!result) {
            final String err = contextReference.getAcquire().getLastError();
            if (err != null) {
                throw new RuntimeException(this + " login failed, " + err + '.');
            } else {
                throw new RuntimeException(this + " login failed.");
            }
        }
    }

    private BackendContext commonInit(final String command) {
        final BackendContext context;
        if (login.isDone()) {
            // full fence needed
            context = contextReference.get();
            if (context.getState() != MysqlClientState.Authenticated || resourceClosed.getAcquire()) {
                throw new RuntimeException(this + " is not correct state for " + command + '.');
            }
        } else {
            context = null;
        }
        return context;
    }

    public QueryResultHandler sendQuery(String query) throws IOException {
        return sendQuery(query, MysqlContext.DEFAULT_CHARSET, false, null);
    }

    // will invoke callback's onDone finally
    public QueryResultHandler sendQuery(String query, Charset expectedCharset, boolean expectedHasClientQueryAttributes,
                                        ResultCallback callback) throws IOException {
        final BackendContext context;
        final ComQuery comQuery;
        QueryResultHandler handler;
        try {
            // try to get context first
            context = commonInit("query");

            // build request packet and handler first
            comQuery = new ComQuery();
            comQuery.setQuery(null == context ? query.getBytes(expectedCharset) : context.encodeStringClient(query));
            handler = new QueryResultHandler(contextReference, null, null, callback);
        } catch (Throwable t) {
            if (callback != null) {
                callback.onDone(null, null, ResultState.Abort);
            }
            throw t;
        }
        final QueryResultHandler returnHandler = handler;
        try (final Encoder encoder = Encoder.create(processor.getBufferPool(), this::write)) {
            // use resourceClosed as synchronize lock
            synchronized (resourceClosed) {
                if (resourceClosed.getPlain()) {
                    throw new IllegalStateException("connection is closed");
                }
                // reload now context
                final BackendContext nowContext = null == context ? contextReference.getAcquire() : context;
                if (nowContext != null && nowContext.isCharsetReady() && null == context) {
                    // fix query encoding
                    comQuery.setQuery(nowContext.encodeStringClient(query));
                }
                if (authenticator != null) {
                    // auth not finished, just push requests to pending
                    try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                        try (final Encoder bytesEncoder = Encoder.create(null, output)) {
                            comQuery.encode(bytesEncoder, null == nowContext ?
                                (expectedHasClientQueryAttributes ? Capabilities.CLIENT_QUERY_ATTRIBUTES : 0) :
                                nowContext.getCapabilities());
                            bytesEncoder.flush();
                        }
                        pendingData.add(output.getBytes());
                    }
                } else {
                    comQuery.encode(encoder, null == nowContext ?
                        (expectedHasClientQueryAttributes ? Capabilities.CLIENT_QUERY_ATTRIBUTES : 0) :
                        nowContext.getCapabilities());
                    encoder.flush();
                }
                resultHandlers.add(handler);
                handler = null;
            }
            return returnHandler;
        } catch (Throwable t) {
            // any send error may corrupt then protocol, close connection
            close();
            throw t;
        } finally {
            if (handler != null) {
                handler.close();
            }
        }
    }

    public void initDB(String db, Charset expectedCharset, boolean abortWhenFail) throws IOException {
        // check and then switch if context is ok, or just send pending switch.
        final BackendContext context = commonInit("init DB");

        // skip if same
        if (context != null && Objects.equals(db, context.getDatabase())) {
            return;
        }
        if (null == db) {
            // switch DB to null via proc
            sendQuery("call dbms_proxy.reset_db()", MysqlContext.DEFAULT_CHARSET, false, (h, b, s) -> {
                if (s.isDone()) {
                    if (null == h) {
                        throw new RuntimeException("Failed to reset db with early abort.");
                    }
                    final BackendContext c = h.getContextReference().getAcquire();
                    assert c != null;
                    if (s.isOK()) {
                        // mark database switched
                        c.setDatabase(null);
                    } else if (abortWhenFail) {
                        final int errCode = s.isError() ? ((QueryResultHandler) h).getErr().getErrorCode() : -1;
                        if (1305 == errCode || 7557 == errCode) {
                            // ignore not supported procedure or "The consensus follower is not allowed to to do current operation."
                            c.setDatabase(null);
                        } else {
                            final String err = s.isError() ?
                                c.decodeStringResults(((QueryResultHandler) h).getErr().getErrorMessage()) : s.name();
                            throw new RuntimeException(
                                this + ". Init DB failed, " + (null == err ? "unknown" : err) + '.');
                        }
                    }
                }
            });
            return;
        }

        // send COM_INIT_DB
        final byte[] dbBytes = null == context ? db.getBytes(expectedCharset) : context.encodeStringClient(db);
        OkErrResultHandler handler = new OkErrResultHandler(contextReference, null, null,
            (h, b, s) -> {
                if (s.isDone()) {
                    final BackendContext c = contextReference.getAcquire();
                    assert c != null;
                    if (s.isOK()) {
                        // mark database switched
                        c.setDatabase(db);
                    } else if (abortWhenFail) {
                        final String err = s.isError() ?
                            c.decodeStringResults(((OkErrResultHandler) h).getErr().getErrorMessage()) : s.name();
                        throw new RuntimeException(this + ". Init DB failed, " + (null == err ? "unknown" : err) + '.');
                    }
                }
            });
        try (final Encoder encoder = Encoder.create(processor.getBufferPool(), this::write)) {
            // use resourceClosed as synchronize lock
            synchronized (resourceClosed) {
                if (resourceClosed.getPlain()) {
                    throw new IllegalStateException("connection is closed");
                }
                // reload now context and re-encode db bytes if needed
                final BackendContext nowContext = null == context ? contextReference.getAcquire() : context;
                final byte[] nowDbBytes;
                if (nowContext != null && nowContext.isCharsetReady() && null == context) {
                    nowDbBytes = nowContext.encodeStringClient(db);
                } else {
                    nowDbBytes = dbBytes;
                }
                if (authenticator != null) {
                    // auth not finished, just push requests to pending
                    try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                        try (final Encoder bytesEncoder = Encoder.create(null, output)) {
                            bytesEncoder.begin();
                            bytesEncoder.u8(0x02);
                            bytesEncoder.str(nowDbBytes);
                            bytesEncoder.end();
                            bytesEncoder.flush();
                        }
                        pendingData.add(output.getBytes());
                    }
                } else {
                    encoder.begin();
                    encoder.u8(0x02);
                    encoder.str(nowDbBytes);
                    encoder.end();
                    encoder.flush();
                }
                resultHandlers.add(handler);
                handler = null;
            }
        } catch (Throwable t) {
            // any send error may corrupt then protocol, close connection
            close();
            throw t;
        } finally {
            if (handler != null) {
                handler.close();
            }
        }
    }

    public StmtPrepareResultHandler sendPrepare(String sql, Charset expectedCharset, ResultCallback callback)
        throws IOException {
        // try to get context first
        final BackendContext context = commonInit("prepare");

        // build handler first
        final byte[] sqlBytes = null == context ? sql.getBytes(expectedCharset) : context.encodeStringClient(sql);
        StmtPrepareResultHandler handler = new StmtPrepareResultHandler(contextReference, null, null, callback);
        final StmtPrepareResultHandler returnHandler = handler;
        try (final Encoder encoder = Encoder.create(processor.getBufferPool(), this::write)) {
            // use resourceClosed as synchronize lock
            synchronized (resourceClosed) {
                if (resourceClosed.getPlain()) {
                    throw new IllegalStateException("connection is closed");
                }
                // reload now context and re-encode sql bytes if needed
                final BackendContext nowContext = null == context ? contextReference.getAcquire() : context;
                final byte[] nowSqlBytes;
                if (nowContext != null && nowContext.isCharsetReady() && null == context) {
                    nowSqlBytes = nowContext.encodeStringClient(sql);
                } else {
                    nowSqlBytes = sqlBytes;
                }
                if (authenticator != null) {
                    // auth not finished, just push requests to pending
                    try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                        try (final Encoder bytesEncoder = Encoder.create(null, output)) {
                            bytesEncoder.begin();
                            bytesEncoder.u8(0x16);
                            bytesEncoder.str(nowSqlBytes);
                            bytesEncoder.end();
                            bytesEncoder.flush();
                        }
                        pendingData.add(output.getBytes());
                    }
                } else {
                    encoder.begin();
                    encoder.u8(0x16);
                    encoder.str(nowSqlBytes);
                    encoder.end();
                    encoder.flush();
                }
                resultHandlers.add(handler);
                handler = null;
            }
            return returnHandler;
        } catch (Throwable t) {
            // any send error may corrupt then protocol, close connection
            close();
            throw t;
        } finally {
            if (handler != null) {
                handler.close();
            }
        }
    }

    public void resetPreparedStatement(int statementId) throws IOException {
        if (!login.isDone()) {
            return; //ignore
        }

        // send COM_STMT_RESET
        OkErrResultHandler handler = new OkErrResultHandler(contextReference, null, null,
            (h, b, s) -> {
                if (s.isDone() && !s.isOK()) {
                    final BackendContext c = contextReference.getAcquire();
                    assert c != null;
                    final String err = s.isError() ?
                        c.decodeStringResults(((OkErrResultHandler) h).getErr().getErrorMessage()) : s.name();
                    throw new RuntimeException(this + ". Reset PS failed, " + (null == err ? "unknown" : err) + '.');
                }
            });
        // reset PS is always system request
        handler.setSystemRequest(true);
        try (final Encoder encoder = Encoder.create(processor.getBufferPool(), this::write)) {
            // use resourceClosed as synchronize lock
            synchronized (resourceClosed) {
                if (resourceClosed.getPlain()) {
                    throw new IllegalStateException("connection is closed");
                }
                if (authenticator != null) {
                    // auth not finished, just push requests to pending
                    try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                        try (final Encoder bytesEncoder = Encoder.create(null, output)) {
                            bytesEncoder.begin();
                            bytesEncoder.u8(0x1A);
                            bytesEncoder.u32(statementId & 0xFFFFFFFFL);
                            bytesEncoder.end();
                            bytesEncoder.flush();
                        }
                        pendingData.add(output.getBytes());
                    }
                } else {
                    encoder.begin();
                    encoder.u8(0x1A);
                    encoder.u32(statementId & 0xFFFFFFFFL);
                    encoder.end();
                    encoder.flush();
                }
                resultHandlers.add(handler);
                handler = null;
            }
        } catch (Throwable t) {
            // any send error may corrupt then protocol, close connection
            close();
            throw t;
        } finally {
            if (handler != null) {
                handler.close();
            }
        }
    }

    public void closePreparedStatement(int statementId) throws IOException {
        if (!login.isDone()) {
            return; //ignore
        }

        // send COM_STMT_CLOSE(no handler needed because no response)
        try (final Encoder encoder = Encoder.create(processor.getBufferPool(), this::write)) {
            // use resourceClosed as synchronize lock
            synchronized (resourceClosed) {
                if (resourceClosed.getPlain()) {
                    throw new IllegalStateException("connection is closed");
                }
                if (authenticator != null) {
                    // auth not finished, just push requests to pending
                    try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                        try (final Encoder bytesEncoder = Encoder.create(null, output)) {
                            bytesEncoder.begin();
                            bytesEncoder.u8(0x19);
                            bytesEncoder.u32(statementId & 0xFFFFFFFFL);
                            bytesEncoder.end();
                            bytesEncoder.flush();
                        }
                        pendingData.add(output.getBytes());
                    }
                } else {
                    encoder.begin();
                    encoder.u8(0x19);
                    encoder.u32(statementId & 0xFFFFFFFFL);
                    encoder.end();
                    encoder.flush();
                }
            }
        } catch (Throwable t) {
            // any send error may corrupt then protocol, close connection
            close();
            throw t;
        }
    }

    public synchronized boolean hasPendingUserRequests() {
        if (!resultHandlers.isEmpty()) {
            for (final ResultHandler handler : resultHandlers) {
                if (!handler.isSystemRequest()) {
                    return true;
                }
            }
        }
        final ResultHandler now = nowResultHandler;
        return now != null && ((!now.isDone() && !now.isSystemRequest()) || (now.hasMore() != null && !now.hasMore()
            .isSystemRequest()));
    }

    @Override
    public String toString() {
        try {
            return "Backend-Connection " + connectionString();
        } catch (Throwable ignore) {
            return "Backend-Connection";
        }
    }

    /**
     * Connect target MySQL server and login with block mode and timeout.
     *
     * @param address target MySQL server address
     * @param processor NIOProcessor
     * @param username username
     * @param encryptedPassword encrypted password
     * @param database database(can be null)
     * @param timeout timeout in milliseconds
     * @return BackendConnection
     * @throws IOException if any error occurs
     */
    public static BackendConnection connectBlocking(SocketAddress address, NIOProcessor processor, String username,
                                                    String encryptedPassword, String database, int timeout)
        throws IOException, ExecutionException, InterruptedException {
        if (null == username || username.isEmpty()) {
            throw new IllegalArgumentException("username is empty");
        }
        if (null == encryptedPassword || encryptedPassword.isEmpty()) {
            throw new IllegalArgumentException("encryptedPassword is empty");
        }

        final long startNanos = System.nanoTime();
        final SocketChannel channel = NIOConnection.connectBlocking(address, timeout);
        final BackendConnection connection;
        try {
            connection = new BackendConnection(channel, processor, true, username, encryptedPassword, database);
        } catch (Throwable t) {
            // close and free the channel
            try {
                final Socket socket = channel.socket();
                if (socket != null) {
                    socket.close();
                }
            } catch (Throwable ignore) {
            }
            try {
                channel.close();
            } catch (Throwable ignore) {
            }
            throw t;
        }
        try {
            processor.postRegister(connection);
            // Need wait valid.
            while (!connection.isValid()) {
                final long nowNanos = System.nanoTime();
                if (nowNanos - startNanos > TimeUnit.MILLISECONDS.toNanos(timeout)) {
                    throw new RuntimeException(
                        connection + " connect wait valid timeout, actual " + (nowNanos - startNanos) / 1000_000.f
                            + " ms.");
                }
                Thread.yield();
            }

            // wait login
            final long nowNanos = System.nanoTime();
            final long restNanos = startNanos + TimeUnit.MILLISECONDS.toNanos(timeout) - nowNanos;
            if (restNanos <= 0) {
                throw new RuntimeException(
                    connection + " login timeout, actual " + (nowNanos - startNanos) / 1000_000.f + " ms.");
            } else {
                try {
                    connection.waitLogin(restNanos, TimeUnit.NANOSECONDS);
                } catch (TimeoutException t) {
                    final long endNanos = System.nanoTime();
                    throw new RuntimeException(
                        connection + " login timeout, actual " + (endNanos - startNanos) / 1000_000.f + " ms.");
                }
            }
        } catch (Throwable t) {
            connection.close();
            throw t;
        }
        return connection;
    }

    public static BackendConnection connectNonBlocking(SocketAddress address, NIOProcessor processor, String username,
                                                       String encryptedPassword, String database) throws IOException {
        if (null == username || username.isEmpty()) {
            throw new IllegalArgumentException("username is empty");
        }
        if (null == encryptedPassword || encryptedPassword.isEmpty()) {
            throw new IllegalArgumentException("encryptedPassword is empty");
        }

        final SocketChannel channel = NIOConnection.connectNonBlocking(address);
        BackendConnection connection = null;
        try {
            connection = new BackendConnection(channel, processor, false, username, encryptedPassword, database);
            processor.postRegister(connection);
        } catch (Throwable t) {
            if (connection != null) {
                connection.close();
            } else {
                // close and free the channel
                try {
                    final Socket socket = channel.socket();
                    if (socket != null) {
                        socket.close();
                    }
                } catch (Throwable ignore) {
                }
                try {
                    channel.close();
                } catch (Throwable ignore) {
                }
            }
            throw t;
        }
        return connection;
    }
}
