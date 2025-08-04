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

package com.alibaba.polardbx.proxy.callback;

import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.help.PreparedStatementContext;
import com.alibaba.polardbx.proxy.context.help.ServerPreparedStatementKey;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.StmtPrepareResultHandler;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StmtPrepareResultCallback extends ResultCallbackBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(StmtPrepareResultCallback.class);

    private BackendConnectionWrapper backend;
    private String sql;

    public StmtPrepareResultCallback(@NotNull FrontendConnection frontend, @NotNull FrontendContext context,
                                     @NotNull Scheduler scheduler, @NotNull BackendConnectionWrapper backend,
                                     @NotNull String sql) {
        super(frontend, context, scheduler);
        this.backend = backend;
        this.sql = sql;
    }

    @Override
    public void onStateChangeWithinLock(ResultHandler handler, ResultState before, ResultState state) {
        assert handler instanceof StmtPrepareResultHandler;
        if (state.isDone()) {
            final BackendContext backendContext = handler.getContextReference().getAcquire();
            assert backendContext != null;
            // Backend context is auto updated, and update frontend warnings & status.
            context.updateStatus(backendContext);

            // record stmt prepare result with in lock
            if (state.isOK()) {
                final StmtPrepareResultHandler prepareHandler = (StmtPrepareResultHandler) handler;
                // record in backend cache first
                final ServerPreparedStatementKey key =
                    new ServerPreparedStatementKey(backendContext.getDatabase(), sql);
                backendContext.recordPreparedStatement(
                    backend, key, prepareHandler.getOk().getStatementId(), "prepare");

                // record in frontend contexts
                final int stmtId = context.getStatementIdAllocator().incrementAndGet();
                final PreparedStatementContext preparedContext =
                    new PreparedStatementContext(stmtId, context.getDatabase(), sql, prepareHandler);
                context.getPreparedStatementContexts().put(stmtId, preparedContext);
                LOGGER.debug("New prepared statement proxyId: {} stmt: {}", stmtId, preparedContext);

                // patch return packet
                final boolean patched = prepareHandler.patchStatementId(stmtId);
                if (!patched) {
                    throw new IllegalStateException("Patch statement id failed");
                }
            }
        }

        // finish trx deference
        super.onStateChangeWithinLock(handler, before, state);
    }

    @Override
    public void onDone(ResultHandler handler, ResultState lastValidState, ResultState state) {
        // finish lazy trx close
        super.onDone(handler, lastValidState, state);

        // dealing retransmit or abort(retry statement prepare is idempotent)
        generalRetransmitDealing(handler, lastValidState, state);
    }
}
