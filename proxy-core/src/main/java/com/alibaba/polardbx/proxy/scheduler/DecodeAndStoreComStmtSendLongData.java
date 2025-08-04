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
import com.alibaba.polardbx.proxy.protocol.common.MysqlError;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtSendLongData;
import com.alibaba.polardbx.proxy.protocol.prepare.StmtUtils;
import com.alibaba.polardbx.proxy.utils.BytesTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecodeAndStoreComStmtSendLongData implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(DecodeAndStoreComStmtSendLongData.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        if (null == scheduler.getRequest() && scheduler.getDecoder() != null) {
            // pre-check statement existence
            final FrontendContext context = scheduler.getContext();
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

            final ComStmtSendLongData stmtSendLongData = new ComStmtSendLongData();
            stmtSendLongData.decode(scheduler.getDecoder(), scheduler.getContext().getCapabilities());
            scheduler.setRequest(stmtSendLongData);
        }

        final PreparedStatementContext preparedStatementContext = scheduler.getPreparedStatement();
        final ComStmtSendLongData stmtSendLongData = (ComStmtSendLongData) scheduler.getRequest();

        // just store in PS context
        preparedStatementContext.getLongDataParams()[stmtSendLongData.getParamId()] = scheduler.getPacket().dump();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("store long data on stmt: {} idx: {} data:\n{}",
                stmtSendLongData.getStatementId(), stmtSendLongData.getParamId(),
                BytesTools.beautifulHex(stmtSendLongData.getData(), 0, stmtSendLongData.getData().length));
        }
        return false; // not taken
    }
}
