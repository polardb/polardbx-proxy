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
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.common.MysqlServerState;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.Slice;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class ResultCallbackBase implements ResultCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultCallbackBase.class);

    protected final FrontendConnection frontend;
    protected final FrontendContext context;
    protected final Scheduler scheduler;

    private FrontendTransactionContext lazyCloseTransaction = null;

    public ResultCallbackBase(@NotNull FrontendConnection frontend, @NotNull FrontendContext context,
                              @NotNull Scheduler scheduler) {
        this.frontend = frontend;
        this.context = context;
        this.scheduler = scheduler;
    }

    public void onStateChangeWithinLock(ResultHandler handler, ResultState before, ResultState state) {
        if (state.isDone()) {
            // dereference transaction
            final FrontendTransactionContext trx = context.dereferenceTransaction();
            if (trx != null) {
                assert null == lazyCloseTransaction;
                lazyCloseTransaction = trx;
            }
        }
    }

    public void onDone(ResultHandler handler, ResultState lastValidState, ResultState state) {
        // lazy free transaction
        if (lazyCloseTransaction != null) {
            lazyCloseTransaction.close();
            lazyCloseTransaction = null;
        }
    }

    protected static class RetransmitDealing {
        public final boolean shouldForceLeader;
        public final boolean needRetransmit;
        public final boolean needSendAbortError;

        public RetransmitDealing(boolean shouldForceLeader, boolean needRetransmit, boolean needSendAbortError) {
            this.shouldForceLeader = shouldForceLeader;
            this.needRetransmit = needRetransmit;
            this.needSendAbortError = needSendAbortError;
        }
    }

    protected RetransmitDealing checkRetransmitOrAbort(ResultHandler handler, ResultState state) {
        // abort, set read_lsn timeout dealing
        final boolean outdatedAbort;
        if (state.isAbort()) {
            final BackendContext backendContext = handler.getContextReference().getAcquire();
            outdatedAbort = backendContext != null && !backendContext.isUpToDate();
        } else {
            outdatedAbort = false;
        }
        final boolean forceLeader = handler.isPacketDroppedByLsn() || outdatedAbort;
        final boolean needDealing = state.isAbort() || forceLeader;
        final boolean needRetransmit;
        if (needDealing) {
            if (forceLeader && handler.isPacketForwarded()) {
                // impossible to get here
                frontend.close();
                throw new RuntimeException("packet dropped by lsn, but forwarded, kill frontend");
            }

            final long nowNanos = System.nanoTime();
            needRetransmit = scheduler.getRetransmitData() != null // have retransmit packet
                && null == context.getTransactionContext() // not trx(or we will get same conn and get abort again)
                && (scheduler.getRetransmitLimitNanos() - nowNanos) > 0 // still in time
                && !handler.isPacketForwarded() // no extra pkt forward to frontend
                && MysqlServerState.Authenticated == context.getState(); // frontend still valid
        } else {
            needRetransmit = false;
        }
        return new RetransmitDealing(forceLeader, needRetransmit, needDealing && !needRetransmit);
    }

    protected void retransmit(boolean forceLeader) {
        // do retry
        final long beforeRetransmitNanos = System.nanoTime();
        ProxyExecutor.getInstance().getExecutor().schedule(() -> {
                try {
                    scheduler.switchThread();
                    final byte[] retransmit = scheduler.getRetransmitData();
                    // heap buffer packet and no need to close
                    final Slice packet = new Slice(ByteBuffer.wrap(retransmit), 0, retransmit.length);
                    scheduler.setSlaveRead(!forceLeader && scheduler.getSlaveRead());
                    if (forceLeader) {
                        scheduler.setSpecificLsn(null);
                    }
                    scheduler.addRetransmitDelayNanos(System.nanoTime() - beforeRetransmitNanos);
                    final Scheduler newScheduler = new Scheduler(scheduler, packet);
                    newScheduler.forward();
                } catch (Throwable t) {
                    LOGGER.error("Failed to retransmit packet", t);
                    frontend.close(); // close frontend connection
                }
            }, scheduler.getRescheduleCount() < FastConfig.queryRetransmitFastRetries ?
                FastConfig.queryRetransmitFastRetryDelay : FastConfig.queryRetransmitSlowRetryDelay,
            TimeUnit.MILLISECONDS);
    }

    protected void sendAbortError(ResultState lastValidState) {
        ProxyExecutor.getInstance().getExecutor().submit(new AbortReporter(frontend, context, lastValidState));
    }

    protected void generalRetransmitDealing(ResultHandler handler, ResultState lastValidState, ResultState state) {
        final RetransmitDealing dealing = checkRetransmitOrAbort(handler, state);
        if (dealing.needRetransmit) {
            retransmit(dealing.shouldForceLeader);
        } else if (dealing.needSendAbortError) {
            sendAbortError(lastValidState);
        }
    }
}
