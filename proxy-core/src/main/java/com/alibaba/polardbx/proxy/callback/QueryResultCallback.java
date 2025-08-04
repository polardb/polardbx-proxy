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

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.help.PreparedStatementContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.logger.ExtraLog;
import com.alibaba.polardbx.proxy.parser.recognizer.SQLParser;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.protocol.command.ComQuery;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtExecute;
import com.alibaba.polardbx.proxy.protocol.prepare.ParameterRebind;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class QueryResultCallback extends ResultCallbackBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResultCallback.class);

    private String traceId;
    private boolean rollbackDiscard = false;

    public QueryResultCallback(@NotNull FrontendConnection frontend, @NotNull FrontendContext context,
                               @NotNull Scheduler scheduler) {
        super(frontend, context, scheduler);
    }

    @Override
    public void onStateChangeWithinLock(ResultHandler handler, ResultState before, ResultState state) {
        assert handler instanceof QueryResultHandler;
        // record trx state change in lock to prevent any reorder outside the lock
        if (state.isDone()) {
            final QueryResultHandler nowHandler = (QueryResultHandler) handler;
            if (nowHandler.hasMore() != null) {
                assert !state.isAbort();
                return; // ignore more result, keep the trx reference(skip super invoke)
            }

            // maintain trx state before super function's dereference
            try {
                final FrontendTransactionContext trx = context.getTransactionContext();
                if (trx != null) {
                    final boolean beforeInTrx = trx.isTransactionStarted();
                    final BackendContext backendContext = handler.getContextReference().getAcquire();
                    assert backendContext != null;
                    // Backend context is auto updated, and update frontend warnings & status.
                    context.updateStatus(backendContext);

                    // record sql affects for each statement(multi-stmt)
                    {
                        final List<QueryResultHandler> handlers = new ArrayList<>(1);
                        handlers.add(0, nowHandler);
                        QueryResultHandler previous = nowHandler;
                        while ((previous = (QueryResultHandler) previous.getPrevious()) != null) {
                            handlers.add(0, previous);
                        }
                        context.recordSqlAffects(scheduler, handlers, backendContext);
                    }

                    if (!state.isError() && !state.isAbort()) {
                        // and affects on prepare statement context
                        if (scheduler.getRequest() instanceof ComStmtExecute) {
                            final PreparedStatementContext preparedStatementContext =
                                scheduler.getPreparedStatement();
                            final ComStmtExecute stmtExecute = (ComStmtExecute) scheduler.getRequest();
                            if (stmtExecute.isNewParamsBindFlag()) {
                                // new update frontend context
                                final ParameterRebind[] parameterRebinds =
                                    new ParameterRebind[preparedStatementContext.getOk().getNumParams()];
                                for (int i = 0; i < parameterRebinds.length; ++i) {
                                    parameterRebinds[i] = new ParameterRebind(stmtExecute.getParameters()[i].getType(),
                                        stmtExecute.getParameters()[i].getName());
                                }
                                preparedStatementContext.getRebindParameters().setRelease(parameterRebinds);
                            }

                            // clear long data params
                            preparedStatementContext.clearLongDataParams();
                            if (context.isCursorExists()) {
                                // set cursor flag if needed
                                final FrontendTransactionContext.ActiveBackendPreparedStatement
                                    activeBackendPreparedStatement =
                                    trx.getActiveBackendPreparedStatementMap()
                                        .get(preparedStatementContext.getStatementId());
                                if (null == activeBackendPreparedStatement) {
                                    throw new IllegalStateException("Active prepared statement not found");
                                }
                                activeBackendPreparedStatement.setCursorInUse(true);
                            }
                        }
                    }

                    // hold the connection if any warnings occurs or hold connection is set
                    trx.setConnectionHold(context.getWarnings() > 0 || FastConfig.enableConnectionHold);
                    trx.setTransactionStarted(context.isInTransaction());

                    // discard if rollback and abort
                    if (state.isAbort() && scheduler.getRequest() instanceof ComQuery) {
                        final ComQuery query = (ComQuery) scheduler.getRequest();
                        try {
                            final SQLParser parser =
                                new SQLParser(query.getQuery(), 0, query.getQuery().length,
                                    context.getClientJavaCharset(),
                                    context.getSqlMode(),
                                    HaManager.getInstance().getVersion());
                            // todo make sure is rollback and no other token
                            if (parser.getFirstToken() == MySQLToken.KW_ROLLBACK) {
                                rollbackDiscard = true;
                                trx.setConnectionHold(false);
                                trx.setTransactionStarted(false);
                                trx.discard();
                            }
                        } catch (Throwable ignore) {
                        }
                    }

                    final int stmtId = trx.getNewStmtId();
                    if (beforeInTrx || trx.isTransactionStarted()) {
                        traceId = trx.getTrxId() + '-' + stmtId;
                    } else if (1 == stmtId) {
                        traceId = trx.getTrxId();
                    } else {
                        assert stmtId > 0;
                        traceId = trx.getTrxId() + '?' + stmtId;
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("Failed to update trx state when query finish", t);
            }
        }

        // finish trx deference
        super.onStateChangeWithinLock(handler, before, state);
    }

    private boolean isReadOnly() {
        final byte[] sql;
        final Charset charset;
        if (scheduler.getRequest() instanceof ComQuery) {
            sql = ((ComQuery) scheduler.getRequest()).getQuery();
            charset = context.getClientJavaCharset();
        } else if (scheduler.getRequest() instanceof ComStmtExecute) {
            sql = scheduler.getPreparedStatement().getPrepareSql().getBytes(StandardCharsets.UTF_8);
            charset = StandardCharsets.UTF_8;
        } else {
            sql = null;
            charset = null;
        }
        if (null == sql || null == charset) {
            return false;
        }
        try {
            final SQLParser parser =
                new SQLParser(sql, 0, sql.length, context.getClientJavaCharset(), context.getSqlMode(),
                    HaManager.getInstance().getVersion());
            return parser.isReadOnly();
        } catch (Throwable t) {
            LOGGER.error("error when parse for system views", t);
            return false;
        }
    }

    @Override
    public void onDone(ResultHandler handler, ResultState lastValidState, ResultState state) {
        assert handler instanceof QueryResultHandler;
        if (handler.hasMore() != null) {
            assert !state.isAbort();
            return; // ignore more result, keep the trx reference(skip super invoke, because no lazy trx free)
        }

        // finish lazy trx close
        super.onDone(handler, lastValidState, state);

        // abort, set read_lsn timeout dealing
        final RetransmitDealing dealing = checkRetransmitOrAbort(handler, state);
        final boolean realRetransmit;
        // todo fixme retransmit only if read(and no trx) or first stmt in trx, check trx and is auto commit
        //      more: record all select and dml affects in trx and retry same can do retransmit
        if (dealing.needRetransmit) {
            realRetransmit = !context.isAutoCommit() // not auto commit(when no active trx means this is the first stmt)
                || isReadOnly(); // or read only
        } else {
            realRetransmit = false;
        }

        if (realRetransmit) {
            // real retransmit
            if (LOGGER.isInfoEnabled()) {
                final String sql;
                if (scheduler.getRequest() instanceof ComQuery) {
                    sql = context.decodeStringClient(((ComQuery) scheduler.getRequest()).getQuery());
                } else if (scheduler.getRequest() instanceof ComStmtExecute) {
                    sql = scheduler.getPreparedStatement().getPrepareSql();
                } else {
                    sql = null;
                }
                if (sql != null) {
                    LOGGER.info("retransmit query: {}{}", sql,
                        dealing.shouldForceLeader ? " force back to leader" : "");
                }
            }
            retransmit(dealing.shouldForceLeader);
            return; // no sql log
        } else if (dealing.needRetransmit || dealing.needSendAbortError) {
            // send abort error if needed
            if (!rollbackDiscard) {
                sendAbortError(lastValidState);
            } else {
                // send ok
                ProxyExecutor.getInstance().getExecutor().submit(new OkReporter(frontend, context));
            }
        }

        if (ExtraLog.SqlLog.isInfoEnabled()) {
            // done and do sql log
            final long currentNanos = System.nanoTime();
            MDC.put("schema", context.getDatabase());
            final String conn =
                "user=" + context.getUsername() +
                    ",host=" + context.getRemoteAddress().getHostString() +
                    ",port=" + context.getRemoteAddress().getPort() +
                    ",schema=" + context.getDatabase() +
                    (context.isAutoCommit() ? "" : ",autocommit=0") +
                    (scheduler.getIsSlaveConnection() ? ",lsn=" + scheduler.getSpecificLsn() : "");
            MDC.put("CONNECTION", conn);
            String sql;
            if (scheduler.getRequest() instanceof ComQuery) {
                sql = context.decodeStringClient(((ComQuery) scheduler.getRequest()).getQuery());
            } else if (scheduler.getPreparedStatement() != null) {
                sql = scheduler.getPreparedStatement().getPrepareSql();
            } else {
                sql = "<unknown>";
            }
            sql = sql.length() > FastConfig.logSqlMaxLength ?
                sql.substring(0, FastConfig.logSqlMaxLength) + "..." : sql;
            sql = sql.replaceAll("\\n+", " ");
            if (scheduler.getRequest() instanceof ComStmtExecute) {
                sql += ' ' + ((ComStmtExecute) scheduler.getRequest()).parametersLogString();
                // todo and show long data parameters
            }
            final StringBuilder builder = new StringBuilder();
            if (scheduler.getRetransmitDelayNanos() > 0 || scheduler.getFetchLsnNanos() > 0
                || scheduler.getScheduleNanos() > 0 || scheduler.getWaitLsnNanos() > 0) {
                builder.append(",retransmit_delay:").append(scheduler.getRetransmitDelayNanos() / 1e3)
                    .append("us,fetch_lsn:").append(scheduler.getFetchLsnNanos() / 1e3).append("us,schedule:")
                    .append(scheduler.getScheduleNanos() / 1e3).append("us,wait_lsn:")
                    .append(scheduler.getWaitLsnNanos() / 1e3).append("us");
            }
            if (scheduler.getWaitLeaderNanos() > 0) {
                builder.append(",wait_leader:").append(scheduler.getWaitLeaderNanos() / 1e3).append("us");
            }
            ExtraLog.SqlLog.info("{} # [state:{},retry:{},total_time:{}us{}] # {}",
                sql, state.name(), scheduler.getRescheduleCount(), (currentNanos - scheduler.getStartNanos()) / 1e3,
                builder, null == traceId ? "<unknown>" : traceId);
        }
    }
}
