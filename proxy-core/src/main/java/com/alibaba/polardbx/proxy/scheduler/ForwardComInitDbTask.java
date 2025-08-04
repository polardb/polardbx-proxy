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

import com.alibaba.polardbx.proxy.callback.InitDbResultCallback;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ComInitDb;
import com.alibaba.polardbx.proxy.protocol.handler.result.OkErrResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardComInitDbTask extends ForwardTaskBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ForwardComInitDbTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        pre(scheduler);

        final FrontendContext context = scheduler.getContext();
        assert scheduler.getRequest() instanceof ComInitDb;
        final String db = context.decodeStringClient(((ComInitDb) scheduler.getRequest()).getSchemaName());
        LOGGER.debug("init db: {}", db);

        // build result handler
        final ResultHandler handler = new OkErrResultHandler(scheduler.getBackend().getContextReference(), scheduler,
            context.getForwarder(scheduler.getFrontend()),
            new InitDbResultCallback(scheduler.getFrontend(), context, scheduler, db));
        return post(scheduler, scheduler.getPacket(), handler);
    }
}
