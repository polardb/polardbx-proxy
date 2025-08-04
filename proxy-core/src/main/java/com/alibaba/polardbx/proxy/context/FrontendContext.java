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

package com.alibaba.polardbx.proxy.context;

import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.connection.MysqlConnection;
import com.alibaba.polardbx.proxy.context.help.PreparedStatementContext;
import com.alibaba.polardbx.proxy.context.query.FrontendQueryContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.command.ErrPacket;
import com.alibaba.polardbx.proxy.protocol.command.OkPacket;
import com.alibaba.polardbx.proxy.protocol.command.StatusFlags;
import com.alibaba.polardbx.proxy.protocol.common.MysqlServerState;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.protocol.handler.MysqlForwarder;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FrontendContext extends MysqlContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendContext.class);

    @Getter
    @Setter
    private MysqlServerState state = MysqlServerState.Init;

    public FrontendContext(InetSocketAddress remoteAddress, int connectionId, int capabilities) {
        super(remoteAddress, connectionId, capabilities);
    }

    private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes(StandardCharsets.UTF_8);

    public void sendErr(Encoder encoder, int errorCode, String state, String message) throws IOException {
        final ErrPacket packet = new ErrPacket();
        packet.setErrorCode(errorCode);
        packet.setSqlStateMarker((byte) '#');
        packet.setSqlState(null == state || state.isEmpty() ? DEFAULT_SQLSTATE : encodeStringResults(state));
        packet.setErrorMessage(encodeStringResults(message));
        packet.encode(encoder, capabilities);
    }

    public void sendErr(MysqlConnection connection, int errorCode, String state, String message) {
        try (final Encoder encoder = Encoder.create(connection.getProcessor().getBufferPool(), connection::write)) {
            sendErr(encoder, errorCode, state, message);
            encoder.flush();
        } catch (Throwable t) {
            LOGGER.error("send err failed", t);
            connection.close(); // close anyway
        }
    }

    public int genStatusFlags(boolean statusChanged) {
        return (inTransaction ? StatusFlags.SERVER_STATUS_IN_TRANS : 0)
            | (isAutoCommit ? StatusFlags.SERVER_STATUS_AUTOCOMMIT : 0)
            | (statusChanged ? StatusFlags.SERVER_SESSION_STATE_CHANGED : 0);
    }

    private static final byte[] OK_SEQ1 = new byte[] {7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    private static final byte[] OK_SEQ2 = new byte[] {7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};
    private static final byte[] OK_SEQ4 = new byte[] {7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0};

    public void sendOk(Encoder encoder, boolean statusChanged) throws IOException {
        // todo more status?
        if (isAutoCommit && !inTransaction && !statusChanged && hasCapability(Capabilities.CLIENT_PROTOCOL_41)
            && !hasCapability(Capabilities.CLIENT_SESSION_TRACK)) {
            switch (encoder.getSeq()) {
            case 1:
                // fast path for simple ok
                encoder.pkt(OK_SEQ1);
                return;

            case 2:
                // fast path for auth ok
                encoder.pkt(OK_SEQ2);
                return;

            case 4:
                // fast path for auth ok after auth switch
                encoder.pkt(OK_SEQ4);
                return;

            default:
                break;
            }
        }
        final OkPacket ok = new OkPacket();
        ok.setStatusFlags(genStatusFlags(statusChanged));
        ok.encode(encoder, capabilities);
    }

    public void sendOk(MysqlConnection connection, boolean statusChanged) {
        try (final Encoder encoder = Encoder.create(connection.getProcessor().getBufferPool(), connection::write)) {
            sendOk(encoder, statusChanged);
            encoder.flush();
        } catch (Throwable t) {
            LOGGER.error("send ok failed", t);
            connection.close(); // close anyway
        }
    }

    /**
     * Extra context for proxy queries.
     */

    private MysqlForwarder forwarder = null;

    // trx context with reference count
    private int transactionRefer = 0;
    @Getter
    private volatile FrontendTransactionContext transactionContext = null;

    // query context
    @Getter
    private volatile FrontendQueryContext queryContext = null;

    // prepared statement id allocator
    @Getter
    private final AtomicInteger statementIdAllocator = new AtomicInteger(0);
    // prepared statement
    @Getter
    private final Map<Integer, PreparedStatementContext> preparedStatementContexts = new ConcurrentHashMap<>();

    public MysqlForwarder getForwarder(FrontendConnection connection) {
        MysqlForwarder forwarder = this.forwarder;
        if (null == forwarder) {
            synchronized (this) {
                forwarder = this.forwarder;
                if (null == forwarder) {
                    if (leakCheckClosed.getPlain()) { // plain read in lock
                        throw new IllegalStateException("connection is closed");
                    }
                    this.forwarder = forwarder = new MysqlForwarder(connection, this);
                }
            }
        }
        return forwarder;
    }

    public FrontendTransactionContext referenceTransaction(boolean rwTrx) {
        return referenceTransaction(true, rwTrx);
    }

    public synchronized FrontendTransactionContext referenceTransaction(boolean createIfNotExist, boolean rwTrx) {
        if (leakCheckClosed.getPlain()) { // plain read in lock
            throw new IllegalStateException("connection is closed");
        }
        if (transactionRefer < 0) {
            throw new IllegalStateException("bad transaction refer");
        }
        if (0 == transactionRefer && null == transactionContext && !createIfNotExist) {
            return null; // no existing and no create
        }
        final int before = transactionRefer++;
        if (0 == before) {
            // new one if not initialized
            if (null == transactionContext) {
                assert createIfNotExist;
                transactionContext = new FrontendTransactionContext(rwTrx);
            }
            return transactionContext;
        }
        // when context closed, will not reach here, because leakCheckClosed will be true(synchronized on this object)
        assert transactionContext != null;
        return transactionContext;
    }

    public synchronized void initNewQuery() {
        if (queryContext != null) {
            throw new IllegalStateException("query context is not cleanup");
        }
        if (leakCheckClosed.getPlain()) { // plain read in lock
            throw new IllegalStateException("connection is closed");
        }
        queryContext = new FrontendQueryContext();
    }

    // invoker will close the trx context
    public FrontendTransactionContext dereferenceTransaction() {
        final FrontendTransactionContext trx;
        synchronized (this) {
            if (transactionRefer <= 0) {
                throw new IllegalStateException("bad transaction refer");
            }
            final int after = --transactionRefer;
            if (0 == after && transactionContext != null && transactionContext.canTrxFreeIfNoReference()) {
                // close transaction context only if no reference and no extra limits
                trx = transactionContext;
                transactionContext = null;
            } else {
                trx = null;
            }
        }
        return trx;
    }

    public FrontendTransactionContext tryFreeTransaction() {
        final FrontendTransactionContext trx;
        synchronized (this) {
            if (transactionRefer < 0) {
                throw new IllegalStateException("bad transaction refer");
            }
            if (0 == transactionRefer && transactionContext != null && transactionContext.canTrxFreeIfNoReference()) {
                // close transaction context only if no reference and no extra limits
                trx = transactionContext;
                transactionContext = null;
            } else {
                trx = null;
            }
        }
        return trx;
    }

    public void cleanupQuery() {
        final FrontendQueryContext query;
        synchronized (this) {
            query = queryContext;
            queryContext = null;
        }

        if (query != null) {
            try {
                query.close();
            } catch (Throwable t) {
                LOGGER.error("close query context error", t);
            }
        }
    }

    @Override
    public void close() {
        final FrontendQueryContext query;
        final FrontendTransactionContext trx;
        final MysqlForwarder tmpForwarder;
        synchronized (this) {
            query = queryContext;
            queryContext = null;
            trx = transactionContext;
            if (transactionRefer > 0 && trx != null) {
                // force discard if any query still running
                trx.discard();
                // trx will auto discard if hold the trx or trx is started
            }
            transactionContext = null;
            tmpForwarder = forwarder;
            forwarder = null;

            // because we use leakCheckClosed to check context closed, so set it here
            leakCheckClosed.setPlain(true);
        }

        // close query context first
        if (query != null) {
            try {
                query.close();
            } catch (Throwable t) {
                LOGGER.error("close query context error", t);
            }
        }
        // then transaction context
        if (trx != null) {
            try {
                trx.close();
            } catch (Throwable t) {
                LOGGER.error("close transaction context error", t);
            }
        }
        // then connection context
        if (tmpForwarder != null) {
            try {
                tmpForwarder.close();
            } catch (Throwable t) {
                LOGGER.error("close forwarder error", t);
            }
        }

        // set state to closed
        state = MysqlServerState.Closed;

        // finalize the leak check
        super.close();
    }
}
