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

import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.StmtFetchHandler;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StmtFetchCallback extends ResultCallbackBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(StmtFetchCallback.class);

    public StmtFetchCallback(@NotNull FrontendConnection frontend, @NotNull FrontendContext context,
                             @NotNull Scheduler scheduler) {
        super(frontend, context, scheduler);
    }

    @Override
    public void onStateChangeWithinLock(ResultHandler handler, ResultState before, ResultState state) {
        assert handler instanceof StmtFetchHandler;
        // record trx state change in lock to prevent any reorder outside the lock
        if (state.isDone()) {
            final StmtFetchHandler nowHandler = (StmtFetchHandler) handler;
            if (nowHandler.hasMore() != null) {
                assert !state.isAbort();
                return; // ignore more result, keep the trx reference(skip super invoke)
            }

            // maintain trx state before super function's dereference
            try {
                final FrontendTransactionContext trx = context.getTransactionContext();
                if (trx != null) {
                    final BackendContext backendContext = handler.getContextReference().getAcquire();
                    assert backendContext != null;
                    // Backend context is auto updated, and update frontend warnings & status.
                    context.updateStatus(backendContext);

                    // clear cursor in use when fetch done
                    if (!context.isCursorExists()) {
                        final FrontendTransactionContext.ActiveBackendPreparedStatement
                            activeBackendPreparedStatement =
                            trx.getActiveBackendPreparedStatementMap()
                                .get(scheduler.getPreparedStatement().getStatementId());
                        if (null == activeBackendPreparedStatement) {
                            throw new IllegalStateException("Active prepared statement not found");
                        }
                        activeBackendPreparedStatement.setCursorInUse(false);
                    }

                    // hold the connection if any warnings occurs or hold connection is set
                    trx.setConnectionHold(context.getWarnings() > 0 || FastConfig.enableConnectionHold);
                    trx.setTransactionStarted(context.isInTransaction());
                }
            } catch (Throwable t) {
                LOGGER.error("Failed to update trx state when query finish", t);
            }
        }

        // finish trx deference
        super.onStateChangeWithinLock(handler, before, state);
    }

    @Override
    public void onDone(ResultHandler handler, ResultState lastValidState, ResultState state) {
        assert handler instanceof StmtFetchHandler;
        if (handler.hasMore() != null) {
            assert !state.isAbort();
            return; // ignore more result, keep the trx reference(skip super invoke, because no lazy trx free)
        }

        // finish lazy trx close
        super.onDone(handler, lastValidState, state);

        // never retransmit for cursor fetch
        if (state.isAbort()) {
            sendAbortError(lastValidState);
        }
    }
}
