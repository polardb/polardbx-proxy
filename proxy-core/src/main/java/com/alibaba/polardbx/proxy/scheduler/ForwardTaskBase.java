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

import com.alibaba.polardbx.proxy.cluster.GlobalMock;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.utils.Slice;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class ForwardTaskBase implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ForwardTaskBase.class);

    protected void pre(@NotNull Scheduler scheduler) {
        if (null == scheduler.getBackend()) {
            throw new IllegalStateException("backend is null when forward request");
        }
        // mock fail
        GlobalMock.randomThrowWhenForwardPacket(scheduler.getContext());
    }

    protected boolean post(@NotNull Scheduler scheduler, @NotNull Slice packet, @Nullable ResultHandler handler)
        throws IOException {
        final BackendConnectionWrapper backend;
        final String postOperationSql;
        final FrontendContext context;
        try {
            // assume backend is ok
            backend = scheduler.getBackend();
            if (null == backend) {
                throw new IllegalStateException("Backend is null when forward request.");
            }

            postOperationSql = scheduler.getPostOperationSql();
            context = scheduler.getContext();
            if (postOperationSql != null) {
                // add extra trx ref for post ops
                final boolean goSlave = scheduler.getSlaveRead() != null && scheduler.getSlaveRead();
                final FrontendTransactionContext trx = context.referenceTransaction(false, !goSlave);
                if (null == trx) {
                    throw new IllegalStateException(
                        "Transaction is null when forward request and need post operation.");
                }
            }
        } catch (Throwable t) {
            if (handler != null) {
                handler.close();
            }
            throw t;
        }

        boolean derefer = postOperationSql != null;
        try {
            // forward will take packet, dereference, or send error finally
            scheduler.setDereference(false);
            scheduler.setSendError(false);
            backend.forward(packet, handler);

            // schedule post operation
            if (postOperationSql != null) {
                // sendQuery with callback will invoke onDone(dereference trx) in callback finally
                derefer = false;
                backend.sendQuery(postOperationSql, context.getClientJavaCharset(),
                    context.hasCapability(Capabilities.CLIENT_QUERY_ATTRIBUTES), scheduler.getPostOperationCallback());
            }
        } catch (Throwable t) {
            if (derefer) {
                final FrontendTransactionContext trx = context.dereferenceTransaction();
                if (trx != null) {
                    try {
                        trx.close();
                    } catch (Throwable t1) {
                        LOGGER.error("free transaction failed", t1);
                    }
                }
            }
            throw t;
        }

        // when forwarded, response will be scheduled in another thread
        scheduler.switchThread();
        return true; // taken and forward
    }
}
