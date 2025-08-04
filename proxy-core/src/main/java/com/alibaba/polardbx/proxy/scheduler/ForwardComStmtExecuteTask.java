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
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtExecute;
import com.alibaba.polardbx.proxy.utils.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ForwardComStmtExecuteTask extends ForwardTaskBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ForwardComStmtExecuteTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        pre(scheduler);

        final FrontendContext context = scheduler.getContext();
        assert scheduler.getRequest() instanceof ComStmtExecute;
        final ComStmtExecute stmtExecute = (ComStmtExecute) scheduler.getRequest();
        if (LOGGER.isDebugEnabled()) {
            // todo show long data parameters
            LOGGER.debug("stmt execute: {} {}", scheduler.getPreparedStatement().getPrepareSql(),
                stmtExecute.parametersLogString());
        }

        // push long data parameters
        try {
            for (int i = 0; i < scheduler.getPreparedStatement().getLongDataParams().length; i++) {
                final byte[] data = scheduler.getPreparedStatement().getLongDataParams()[i];
                if (null == data) {
                    continue;
                }
                scheduler.getBackend().forward(new Slice(ByteBuffer.wrap(data), 0, data.length), null);
            }
        } catch (Throwable t) {
            LOGGER.error("push long data parameters failed", t);
            // close backend anyway
            scheduler.getBackend().close();
            throw t;
        }

        // simply forward request if not rebind or packet will rebind
        if (null == scheduler.getPreparedStatement().getRebindParameters().getAcquire()
            || stmtExecute.isNewParamsBindFlag()) {
            // build result handler(with no extra code between new and post)
            final ResultHandler handler =
                new QueryResultHandler(scheduler.getBackend().getContextReference(), scheduler,
                    context.getForwarder(scheduler.getFrontend()),
                    new QueryResultCallback(scheduler.getFrontend(), context, scheduler), true);
            return post(scheduler, scheduler.getPacket(), handler);
        }

        // or we need to rebind the request
        stmtExecute.setStatementId(scheduler.getBackendPreparedId());
        stmtExecute.setNewParamsBindFlag(true);
        final Slice newPacket;
        try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
            try (final Encoder bytesEncoder = Encoder.create(null, output)) {
                stmtExecute.encode(bytesEncoder, scheduler.getContext().getCapabilities());
                bytesEncoder.flush();
            }
            final byte[] newPacketData = output.getBytes();
            newPacket = new Slice(ByteBuffer.wrap(newPacketData), 0, newPacketData.length);
        }

        // build result handler(with no extra code between new and post)
        final ResultHandler handler = new QueryResultHandler(scheduler.getBackend().getContextReference(), scheduler,
            context.getForwarder(scheduler.getFrontend()),
            new QueryResultCallback(scheduler.getFrontend(), context, scheduler), true);
        // reuse post routine but not taken
        post(scheduler, newPacket, handler);
        return false; // not taken
    }
}
