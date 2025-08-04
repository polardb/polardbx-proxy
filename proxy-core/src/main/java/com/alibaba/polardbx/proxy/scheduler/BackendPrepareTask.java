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

package com.alibaba.polardbx.proxy.scheduler;

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.help.PreparedStatementContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.command.ErrPacket;
import com.alibaba.polardbx.proxy.protocol.common.MysqlError;
import com.alibaba.polardbx.proxy.protocol.handler.result.StmtPrepareResultHandler;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtExecute;
import com.alibaba.polardbx.proxy.protocol.prepare.StmtUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;

public class BackendPrepareTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackendPrepareTask.class);

    private boolean prepareAndForward(Scheduler scheduler) throws IOException {
        final FrontendContext context = scheduler.getContext();
        final PreparedStatementContext preparedStatement = scheduler.getPreparedStatement();
        final BackendConnectionWrapper backend = scheduler.getBackend();

        final long prepareStartNanos = System.nanoTime();

        // switch db if needed
        final boolean switchDbForPrepare = !Objects.equals(context.getDatabase(), preparedStatement.getSchema());
        if (switchDbForPrepare) {
            LOGGER.debug("switch db to {} for prepare, now {}", preparedStatement.getSchema(), context.getDatabase());
            backend.initDB(preparedStatement.getSchema(), context.getClientJavaCharset(), true);
        }

        backend.sendPrepare(preparedStatement.getPrepareSql(), context.getClientJavaCharset(),
            (handler, before, state) -> {
                if (!state.isDone()) {
                    return;
                }

                scheduler.switchThread(); // mark thread switched

                final long prepareDoneNanos = System.nanoTime();
                ProxyExecutor.getInstance().getExecutor().submit(() -> {
                    boolean needFree = true;
                    try {
                        final long scheduleNanos = System.nanoTime();
                        scheduler.addPrepareNanos(prepareDoneNanos - prepareStartNanos);
                        scheduler.addScheduleNanos(scheduleNanos - prepareDoneNanos);

                        final BackendContext backendContext = backend.getContextReference().getAcquire();
                        assert backendContext != null;

                        final boolean taken;
                        // todo: mock failed here
                        if (state.isOK()) {
                            // prepared ok, save it to cache and restart normal routine
                            // don't check database here, because we may switch back to original DB(not PS's DB)
                            backendContext.recordPreparedStatement(backend, preparedStatement.getKey(),
                                ((StmtPrepareResultHandler) handler).getOk().getStatementId(), "setup");
                            taken = scheduler.forward();
                        } else if (state.isError()) {
                            final ErrPacket err = ((StmtPrepareResultHandler) handler).getErr();
                            taken = scheduler.errorHandle(
                                new SQLException(backendContext.decodeStringResults(err.getErrorMessage()),
                                    backendContext.decodeStringResults(err.getSqlState()), err.getErrorCode()));
                        } else {
                            taken = scheduler.errorHandle(new IOException("Failed to prepare on backend"));
                        }
                        if (taken) {
                            needFree = false;
                        }
                    } catch (Throwable t) {
                        LOGGER.error("prepare callback failed", t);
                        scheduler.getFrontend().close(); // close frontend connection
                    } finally {
                        if (needFree) {
                            scheduler.getPacket().close();
                        }
                    }
                });
            });

        // switch back
        if (switchDbForPrepare) {
            LOGGER.debug("switch db back to {} after prepare", context.getDatabase());
            backend.initDB(context.getDatabase(), context.getClientJavaCharset(), true);
        }
        return true; // take packet and free it in dealing task
    }

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        // init prepared statement context if empty
        PreparedStatementContext preparedStatement = scheduler.getPreparedStatement();
        if (null == preparedStatement) {
            final int stmtId;
            if (scheduler.getRequest() instanceof ComStmtExecute) {
                stmtId = ((ComStmtExecute) scheduler.getRequest()).getStatementId();
            } else {
                throw new RuntimeException(
                    "Unexpected request type " + scheduler.getRequest().getClass().getSimpleName());
            }
            preparedStatement = scheduler.getContext().getPreparedStatementContexts().get(stmtId);
            if (null == preparedStatement) {
                throw new SQLException("Unknown prepared statement id", MysqlError.GENERAL_STATE,
                    MysqlError.ER_UNKNOWN_STMT_HANDLER);
            }
            scheduler.setPreparedStatement(preparedStatement);
        }

        // load active backend prepare id if not initialized
        if (null == scheduler.getBackendPreparedId()) {
            // find in active map first
            final FrontendTransactionContext trx = scheduler.getContext().getTransactionContext();
            if (null == trx) {
                throw new RuntimeException("Transaction context is null when prepare");
            }

            final FrontendTransactionContext.ActiveBackendPreparedStatement activeBackendPreparedStatement =
                trx.getActiveBackendPreparedStatementMap().get(preparedStatement.getStatementId());
            if (activeBackendPreparedStatement != null) {
                scheduler.setBackendPreparedId(activeBackendPreparedStatement.getStatementId());
                // and switch backend
                scheduler.setBackend(activeBackendPreparedStatement.getConnection());
                LOGGER.debug("Use active PS frontend id: {}, backend id: {}, backend: {}.",
                    preparedStatement.getStatementId(), scheduler.getBackendPreparedId(), scheduler.getBackend());
            } else {
                final BackendConnectionWrapper backend = scheduler.getBackend();
                if (null == backend) {
                    throw new RuntimeException("Backend is null when prepare");
                } else {
                    // init in backend
                    final BackendContext backendContext = backend.getContextReference().getAcquire();
                    Integer backendPreparedId = null;
                    if (backendContext != null) {
                        backendPreparedId = backendContext.takePreparedStatement(backend, preparedStatement.getKey());
                    }
                    if (null == backendPreparedId) {
                        // prepare statement on backend and will resubmit in callback
                        return prepareAndForward(scheduler);
                    } else {
                        scheduler.setBackendPreparedId(backendPreparedId);
                        LOGGER.debug("Take backend PS frontend id: {}, backend id: {}, backend: {}.",
                            preparedStatement.getStatementId(), scheduler.getBackendPreparedId(),
                            scheduler.getBackend());
                    }
                    // put it in active map
                    trx.getActiveBackendPreparedStatementMap().put(preparedStatement.getStatementId(),
                        new FrontendTransactionContext.ActiveBackendPreparedStatement(backendPreparedId,
                            preparedStatement.getSchema(), preparedStatement.getPrepareSql(), backend));
                }
            }

            // now patch it
            StmtUtils.fastPatchStatementId(scheduler.getPacket(), scheduler.getBackendPreparedId());
            // and patch long data
            for (int i = 0; i < preparedStatement.getLongDataParams().length; i++) {
                final byte[] pkt = preparedStatement.getLongDataParams()[i];
                if (pkt != null) {
                    StmtUtils.fastPatchStatementId(pkt, scheduler.getBackendPreparedId());
                }
            }
        }

        return null;
    }
}
