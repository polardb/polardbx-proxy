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

import com.alibaba.polardbx.proxy.callback.QueryResultCallback;
import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ComQuery;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardComQueryTask extends ForwardTaskBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ForwardComQueryTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        pre(scheduler);

        final FrontendContext context = scheduler.getContext();
        assert scheduler.getRequest() instanceof ComQuery;
        if (LOGGER.isDebugEnabled()) {
            final ComQuery comQuery = (ComQuery) scheduler.getRequest();
            String sql = context.decodeStringClient(comQuery.getQuery());
            sql = sql.length() > FastConfig.logSqlMaxLength ?
                sql.substring(0, FastConfig.logSqlMaxLength) + "..." : sql;
            LOGGER.debug("query: {} want_slave: {} go_slave: {}",
                sql, scheduler.getSlaveRead(), scheduler.getIsSlaveConnection());
        }

        // build result handler
        final ResultHandler handler = new QueryResultHandler(scheduler.getBackend().getContextReference(), scheduler,
            context.getForwarder(scheduler.getFrontend()),
            new QueryResultCallback(scheduler.getFrontend(), context, scheduler));
        return post(scheduler, scheduler.getPacket(), handler);
    }
}
