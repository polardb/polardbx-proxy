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
import com.alibaba.polardbx.proxy.parser.recognizer.SQLParser;
import com.alibaba.polardbx.proxy.protocol.command.ComQuery;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtExecute;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class CheckQuerySlaveReadTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckQuerySlaveReadTask.class);

    private boolean canQuerySlave(Scheduler scheduler) {
        final FrontendContext context = scheduler.getContext();
        final MysqlPacket request = scheduler.getRequest();
        try {
            final SQLParser parser;
            if (request instanceof ComQuery) {
                final byte[] query = ((ComQuery) request).getQuery();
                parser = new SQLParser(query, 0, query.length, context.getClientJavaCharset(), context.getSqlMode(),
                    HaManager.getInstance().getVersion());
            } else if (request instanceof ComStmtExecute) {
                final Charset defaultCharset = Charset.defaultCharset();
                final byte[] bytes = scheduler.getPreparedStatement().getPrepareSql().getBytes(defaultCharset);
                parser = new SQLParser(bytes, 0, bytes.length, defaultCharset, context.getSqlMode(),
                    HaManager.getInstance().getVersion());
            } else {
                return false;
            }

            return parser.canSlaveRead();
        } catch (Throwable t) {
            LOGGER.error("error when parse for slave check", t);
            return false;
        }
    }

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        if (null == scheduler.getSlaveRead()) {
            final FrontendContext context = scheduler.getContext();
            // todo check START TRANSACTION READ ONLY and trx is RO trx
            // only check when not in trx(include trx hold) or auto commit
            if (context.getTransactionContext() != null || !context.isAutoCommit()) {
                scheduler.setSlaveRead(false);
            } else {
                scheduler.setSlaveRead(canQuerySlave(scheduler));
            }
        }
        return null;
    }
}
