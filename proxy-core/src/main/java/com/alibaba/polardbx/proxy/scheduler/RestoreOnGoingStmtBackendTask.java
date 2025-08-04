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
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtFetch;
import com.alibaba.polardbx.proxy.protocol.prepare.StmtUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RestoreOnGoingStmtBackendTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreOnGoingStmtBackendTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        // restore existing backend and stmt id if needed
        if (null == scheduler.getBackend()) {
            final FrontendContext context = scheduler.getContext();
            final boolean goSlave = scheduler.getSlaveRead() != null && scheduler.getSlaveRead();
            final FrontendTransactionContext trx = context.referenceTransaction(false, !goSlave);
            if (null == trx) {
                throw new IOException("no ongoing transaction");
            }
            scheduler.setDereference(true);

            // restore backend via active PS
            final int stmtId;
            if (scheduler.getRequest() instanceof ComStmtFetch) {
                stmtId = ((ComStmtFetch) scheduler.getRequest()).getStatementId();
            } else {
                throw new IOException("unexpected request type " + scheduler.getRequest().getClass().getSimpleName());
            }

            final FrontendTransactionContext.ActiveBackendPreparedStatement activeBackendPreparedStatement =
                trx.getActiveBackendPreparedStatementMap().get(stmtId);
            if (null == activeBackendPreparedStatement) {
                throw new IOException("no ongoing prepared statement");
            }
            scheduler.setBackendPreparedId(activeBackendPreparedStatement.getStatementId());
            scheduler.setBackend(activeBackendPreparedStatement.getConnection());
            LOGGER.debug("Fetch stmt use active PS frontend id: {}, backend id: {}, backend: {}.",
                stmtId, scheduler.getBackendPreparedId(), scheduler.getBackend());

            // now patch it
            StmtUtils.fastPatchStatementId(scheduler.getPacket(), scheduler.getBackendPreparedId());
        }
        return null;
    }
}
