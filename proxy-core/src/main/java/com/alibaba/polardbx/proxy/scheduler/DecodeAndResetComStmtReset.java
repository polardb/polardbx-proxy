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

import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.help.PreparedStatementContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.common.MysqlError;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtReset;
import com.alibaba.polardbx.proxy.protocol.prepare.StmtUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecodeAndResetComStmtReset implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(DecodeAndResetComStmtReset.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        final FrontendContext context = scheduler.getContext();
        if (null == scheduler.getRequest() && scheduler.getDecoder() != null) {
            // pre-check statement existence
            final int stmtId = StmtUtils.fastGetStatementId(scheduler.getPacket());
            final PreparedStatementContext preparedStatementContext =
                context.getPreparedStatementContexts().get(stmtId);
            if (null == preparedStatementContext) {
                if (null == scheduler.getEncoder()) {
                    context.sendErr(scheduler.getFrontend(), MysqlError.ER_UNKNOWN_STMT_HANDLER,
                        MysqlError.GENERAL_STATE,
                        "Unknown prepared statement handler (" + stmtId + ") given to EXECUTE");
                } else {
                    context.sendErr(scheduler.getEncoder(), MysqlError.ER_UNKNOWN_STMT_HANDLER,
                        MysqlError.GENERAL_STATE,
                        "Unknown prepared statement handler (" + stmtId + ") given to EXECUTE");
                }
                return false; // not taken
            }
            // fill PS context
            scheduler.setPreparedStatement(preparedStatementContext);

            final ComStmtReset stmtReset = new ComStmtReset();
            stmtReset.decode(scheduler.getDecoder(), scheduler.getContext().getCapabilities());
            scheduler.setRequest(stmtReset);
        }

        final PreparedStatementContext preparedStatementContext = scheduler.getPreparedStatement();
        final int stmtId = preparedStatementContext.getStatementId();

        // clear in PS context
        preparedStatementContext.clearLongDataParams();

        // reset any cursor in backend PS
        final FrontendTransactionContext trx = context.getTransactionContext();
        if (trx != null) {
            final FrontendTransactionContext.ActiveBackendPreparedStatement activeBackendPreparedStatement =
                trx.getActiveBackendPreparedStatementMap().get(stmtId);
            if (activeBackendPreparedStatement != null) {
                if (activeBackendPreparedStatement.isCursorInUse()) {
                    FrontendTransactionContext deleting = null;
                    try {
                        activeBackendPreparedStatement.getConnection().resetPreparedStatement(stmtId);
                        activeBackendPreparedStatement.setCursorInUse(false);

                        LOGGER.debug("reset stmt active cursor: {} stmt {}",
                            activeBackendPreparedStatement.getConnection(), stmtId);

                        // try release trx if no active stmt
                        deleting = context.tryFreeTransaction();
                    } catch (Throwable t) {
                        LOGGER.error("reset stmt: {} stmt {} failed",
                            activeBackendPreparedStatement.getConnection(), stmtId, t);
                        // close backend anyway
                        activeBackendPreparedStatement.getConnection().close();
                    }

                    // free trx if needed
                    if (deleting != null) {
                        deleting.close();
                    }
                }
            }
        }

        // send ok
        if (null == scheduler.getEncoder()) {
            context.sendOk(scheduler.getFrontend(), false);
        } else {
            context.sendOk(scheduler.getEncoder(), false);
        }

        LOGGER.debug("reset stmt: {} done", stmtId);
        return false; // not taken
    }
}
