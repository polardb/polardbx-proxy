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

package com.alibaba.polardbx.proxy.context.transaction;

import com.alibaba.polardbx.proxy.ProxyServer;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.help.ServerPreparedStatementKey;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import com.alibaba.polardbx.proxy.serverless.ReadWriteSplittingPool;
import com.alibaba.polardbx.proxy.utils.LeakChecker;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FrontendTransactionContext extends LeakChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendTransactionContext.class);

    @Getter
    private final String trxId = Long.toHexString(ProxyServer.getInstance().getTrxIdGenerator().nextId());
    @Getter
    private final boolean rwTrx; // should be determined before start

    private BackendConnectionWrapper rwConnection = null;
    private BackendConnectionWrapper roConnection = null;

    private final AtomicInteger statementCount = new AtomicInteger(0);
    @Setter
    private boolean connectionHold = false;
    @Getter
    @Setter
    private boolean transactionStarted = false;
    private boolean forceDiscard = false;

    @Getter
    public static class ActiveBackendPreparedStatement {
        private final int statementId;
        private final String schema;
        private final String prepareSql;
        private final BackendConnectionWrapper connection;

        @Setter
        private boolean cursorInUse = false;

        public ActiveBackendPreparedStatement(int statementId, String schema, String prepareSql,
                                              BackendConnectionWrapper connection) {
            this.statementId = statementId;
            this.schema = schema;
            this.prepareSql = prepareSql;
            this.connection = connection;
        }
    }

    @Getter
    private final Map<Integer, ActiveBackendPreparedStatement> activeBackendPreparedStatementMap =
        new ConcurrentHashMap<>();

    public FrontendTransactionContext(boolean rwTrx) {
        this.rwTrx = rwTrx;
        setTag("FrontendTransactionContext " + trxId);
    }

    public int getNewStmtId() {
        return statementCount.incrementAndGet();
    }

    public boolean canTrxFreeIfNoReference() {
        return !connectionHold && !transactionStarted && activeBackendPreparedStatementMap.values().stream()
            .noneMatch(i -> i.cursorInUse);
    }

    public void discard() {
        forceDiscard = true;
    }

    private BackendConnectionWrapper getConnection(boolean createIfNotExist, FrontendContext frontendContext,
                                                   boolean readOnly)
        throws IOException {
        if (forceDiscard) {
            throw new IllegalStateException("transaction is in fatal error state and discarded");
        }
        BackendConnectionWrapper conn = readOnly ? this.roConnection : this.rwConnection;
        if (null == conn && createIfNotExist) {
            synchronized (this) {
                conn = readOnly ? this.roConnection : this.rwConnection;
                if (null == conn) {
                    if (leakCheckClosed.getPlain()) { // plain read in lock
                        throw new IllegalStateException("transaction is closed");
                    }
                    final ReadWriteSplittingPool pool = HaManager.getInstance().getReadWriteSplittingPool();
                    conn = readOnly ? pool.getRoConnection() : pool.getRwConnection();
                    if (null == conn) {
                        return null;
                    }
                    LOGGER.debug("allocate {} conn for trx", readOnly ? "RO" : "RW");
                    if (readOnly) {
                        this.roConnection = conn;
                    } else {
                        this.rwConnection = conn;
                    }
                    try {
                        conn.restoreContext(frontendContext);
                    } catch (Throwable t) {
                        // force discard connection if any error occurs in context switch
                        forceDiscard = true;
                        throw t;
                    }
                }
            }
        }
        return conn;
    }

    public BackendConnectionWrapper getRwConnection(FrontendContext frontendContext) throws IOException {
        return getConnection(true, frontendContext, false);
    }

    public BackendConnectionWrapper getRoConnection(FrontendContext frontendContext) throws IOException {
        return getConnection(true, frontendContext, true);
    }

    public BackendConnectionWrapper getExistingRwConnection() throws IOException {
        return getConnection(false, null, false);
    }

    public BackendConnectionWrapper getExistingRoConnection() throws IOException {
        return getConnection(false, null, true);
    }

    @Override
    public void close() {
        final BackendConnectionWrapper rwWrapper, roWrapper;
        final boolean shouldDiscard;
        final List<ActiveBackendPreparedStatement> preparedStatements;
        synchronized (this) {
            rwWrapper = rwConnection;
            rwConnection = null;
            roWrapper = roConnection;
            roConnection = null;
            shouldDiscard = connectionHold || transactionStarted || forceDiscard;

            if (activeBackendPreparedStatementMap.isEmpty()) {
                preparedStatements = null;
            } else {
                preparedStatements = new ArrayList<>(activeBackendPreparedStatementMap.values());
                activeBackendPreparedStatementMap.clear();
            }

            // because we use leakCheckClosed to check context closed, so set it here
            leakCheckClosed.setPlain(true);
        }

        // free prepared statement back to connection if not discarded
        if (!shouldDiscard && preparedStatements != null) {
            for (final ActiveBackendPreparedStatement ps : preparedStatements) {
                try {
                    // try reset first if needed
                    if (ps.cursorInUse) {
                        ps.connection.resetPreparedStatement(ps.statementId);
                        LOGGER.debug("reset prepared statement on {} id: {} before reuse", ps.connection,
                            ps.statementId);
                    }
                    final BackendContext backendContext = ps.connection.getContextReference().getAcquire();
                    assert backendContext != null;
                    backendContext.recordPreparedStatement(ps.connection,
                        new ServerPreparedStatementKey(ps.schema, ps.prepareSql), ps.statementId, "reuse");
                } catch (Throwable t) {
                    LOGGER.error("close prepared statement error", t);
                }
            }
        }

        if (rwWrapper != null) {
            LOGGER.debug("{} RW conn for trx", shouldDiscard ? "discard" : "release");
            try {
                if (shouldDiscard) {
                    rwWrapper.discard();
                } else {
                    rwWrapper.close();
                }
            } catch (Throwable t) {
                LOGGER.error("close transaction RW connection error", t);
            }
        }
        if (roWrapper != null) {
            LOGGER.debug("{} RO conn for trx", shouldDiscard ? "discard" : "release");
            try {
                if (shouldDiscard) {
                    roWrapper.discard();
                } else {
                    roWrapper.close();
                }
            } catch (Throwable t) {
                LOGGER.error("close transaction RO connection error", t);
            }
        }

        // finalize the leak check
        super.close();
    }
}
