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

package com.alibaba.polardbx.proxy.connection.pool;

import com.alibaba.polardbx.proxy.callback.ResultCallback;
import com.alibaba.polardbx.proxy.connection.BackendConnection;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.MysqlContext;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.StmtPrepareResultHandler;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class BackendConnectionWrapper implements AutoCloseable {
    @Getter
    private final BackendPool backendPool;
    // Note: use object synchronization to avoid deadlock, because we may close wrapper inner the invoking
    private BackendConnection backendConnection;

    public BackendConnectionWrapper(@NotNull final BackendPool backendPool,
                                    @NotNull final BackendConnection backendConnection) {
        backendPool.connectionRunning.getAndIncrement();
        this.backendPool = backendPool;
        this.backendConnection = backendConnection;
    }

    public synchronized AtomicReference<BackendContext> getContextReference() {
        return backendConnection.getContextReference();
    }

    public synchronized String probeBackendTag() {
        if (backendConnection != null) {
            final BackendContext context = backendConnection.getContextReference().getAcquire();
            if (context != null) {
                return context.getRemoteAddress().getHostString() + ':' + context.getRemoteAddress().getPort() + '-'
                    + context.getConnectionId();
            }
        }
        return null;
    }

    public synchronized Integer probeConnectionId() {
        if (backendConnection != null) {
            final BackendContext context = backendConnection.getContextReference().getAcquire();
            if (context != null) {
                return context.getConnectionId();
            }
        }
        return null;
    }

    public synchronized boolean isSlave() {
        return backendConnection.isSlave();
    }

    // Caution: Packet and handler will close anyway.
    public void forward(@NotNull Slice packet, @Nullable ResultHandler handler) throws IOException {
        boolean close = true;
        try {
            synchronized (this) {
                if (backendConnection != null) {
                    close = false; // handler will close in forward
                    backendConnection.forward(packet, handler);
                } else {
                    throw new IOException("Connection is closed");
                }
            }
        } finally {
            if (close) {
                packet.close();
                if (handler != null) {
                    handler.close();
                }
            }
        }
    }

    public synchronized void waitLogin(long timeout, TimeUnit unit)
        throws ExecutionException, InterruptedException, TimeoutException {
        backendConnection.waitLogin(timeout, unit);
    }

    public synchronized void enableRead() {
        backendConnection.enableRead();
    }

    public synchronized void disableRead() {
        backendConnection.disableRead();
    }

    public synchronized void initDB(String db, Charset expectedCharset, boolean abortWhenFail) throws IOException {
        backendConnection.initDB(db, expectedCharset, abortWhenFail);
    }

    public synchronized QueryResultHandler sendQuery(String query) throws IOException {
        return backendConnection.sendQuery(query);
    }

    // Caution: onDone in callback will be invoked anyway.
    public QueryResultHandler sendQuery(String query, Charset expectedCharset,
                                        boolean expectedHasClientQueryAttributes, ResultCallback callback)
        throws IOException {
        boolean close = true;
        try {
            synchronized (this) {
                if (backendConnection != null) {
                    close = false; // sendQuery will invoke callback
                    return backendConnection.sendQuery(
                        query, expectedCharset, expectedHasClientQueryAttributes, callback);
                } else {
                    throw new IOException("Connection is closed");
                }
            }
        } finally {
            if (close) {
                callback.onDone(null, null, ResultState.Abort);
            }
        }
    }

    public synchronized StmtPrepareResultHandler sendPrepare(String sql, Charset expectedCharset,
                                                             ResultCallback callback)
        throws IOException {
        return backendConnection.sendPrepare(sql, expectedCharset, callback);
    }

    public synchronized void resetPreparedStatement(int statementId) throws IOException {
        backendConnection.resetPreparedStatement(statementId);
    }

    public synchronized void closePreparedStatement(int statementId) throws IOException {
        backendConnection.closePreparedStatement(statementId);
    }

    public synchronized void restoreContext(FrontendContext frontendContext) throws IOException {
        final BackendContext backendContext = backendConnection.getContextReference().getAcquire();
        // check user and privileged host first if backend support user switch
        if (backendPool.getProxyToken() != null && (null == backendContext || !Objects.equals(
            frontendContext.getUsername(), backendContext.getUsername()) || !Objects.equals(
            frontendContext.getPrivilegeHost(), backendContext.getPrivilegeHost()))) {
            final String targetUser = frontendContext.getUsername();
            final String targetHost = frontendContext.getPrivilegeHost();
            backendConnection.sendQuery(
                "call dbms_proxy.switch_user('" + backendPool.getProxyToken() + "', '" + targetUser + "', '"
                    + targetHost + "')", MysqlContext.DEFAULT_CHARSET, false, (handler, before, state) -> {
                    if (state.isDone()) {
                        if (null == handler) {
                            throw new RuntimeException("Failed to switch user with early abort.");
                        }
                        final BackendContext c = handler.getContextReference().getAcquire();
                        assert c != null;
                        if (state.isOK()) {
                            // Backend context is auto updated, and update frontend warnings & status.
                            frontendContext.updateStatus(c);
                            // update backend user info
                            c.setUsername(targetUser);
                            c.setPrivilegeHost(targetHost);
                        } else {
                            final String err = state.isError() ?
                                c.decodeStringResults(((QueryResultHandler) handler).getErr().getErrorMessage()) :
                                state.name();
                            throw new RuntimeException(
                                backendConnection + ". Switch user failed, " + (null == err ? "unknown" : err) + '.');
                        }
                    }
                });
        }

        // restore schema
        backendConnection.initDB(frontendContext.getDatabase(), MysqlContext.DEFAULT_CHARSET, true);

        // restore autocommit
        final boolean autoCommit = frontendContext.isAutoCommit();
        if (null == backendContext || backendContext.isAutoCommit() != autoCommit) {
            final boolean inTrx = frontendContext.isInTransaction();
            // need set auto commit
            backendConnection.sendQuery("set autocommit=" + (frontendContext.isAutoCommit() ? '1' : '0'),
                MysqlContext.DEFAULT_CHARSET, false,
                (handler, before, state) -> {
                    if (state.isDone()) {
                        if (null == handler) {
                            throw new RuntimeException("Failed to set autocommit with early abort.");
                        }
                        final BackendContext c = handler.getContextReference().getAcquire();
                        assert c != null;
                        if (state.isOK()) {
                            // Backend context is auto updated, and update frontend warnings & status.
                            frontendContext.updateStatus(c);
                            if (frontendContext.isInTransaction() != inTrx
                                || frontendContext.isAutoCommit() != autoCommit) {
                                throw new RuntimeException(MessageFormat.format(
                                    "Trx status unexpected changed. Before inTrx {0} autoCommit {1} after inTrx {2} autoCommit {3}",
                                    inTrx, autoCommit, frontendContext.isInTransaction(),
                                    frontendContext.isAutoCommit()));
                            }
                        } else {
                            final String err = state.isError() ?
                                c.decodeStringResults(((QueryResultHandler) handler).getErr().getErrorMessage()) :
                                state.name();
                            throw new RuntimeException(
                                backendConnection + ". Restore autocommit failed, " + (null == err ? "unknown" : err)
                                    + '.');
                        }
                    }
                }
            );
        }
    }

    public void discard() {
        final BackendConnection conn;
        synchronized (this) {
            conn = backendConnection;
            backendConnection = null;
        }

        if (conn != null) {
            backendPool.connectionRunning.getAndDecrement();
            conn.close();
        }
    }

    @Override
    public void close() {
        final BackendConnection conn;
        synchronized (this) {
            conn = backendConnection;
            backendConnection = null;
        }

        if (conn != null) {
            backendPool.connectionRunning.getAndDecrement();
            backendPool.release(conn);
        }
    }

    @Override
    public synchronized String toString() {
        if (backendConnection != null) {
            return backendConnection.toString();
        }
        return "BackendConnection freed.";
    }
}
