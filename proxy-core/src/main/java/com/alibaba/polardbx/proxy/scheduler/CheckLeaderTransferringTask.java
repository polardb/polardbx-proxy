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

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class CheckLeaderTransferringTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckLeaderTransferringTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        if (null == scheduler.getBackend()) {
            final FrontendContext context = scheduler.getContext();
            final boolean goSlave = scheduler.getSlaveRead() != null && scheduler.getSlaveRead();

            // check leader transferring if not go slave and no ongoing transaction
            if (!goSlave && null == context.getTransactionContext() && HaManager.getInstance().isLeaderTransferring()) {
                // do reschedule
                final long startNanos = System.nanoTime();
                final AtomicBoolean runOnce = new AtomicBoolean(false);
                final Runnable runnable = () -> {
                    if (!runOnce.compareAndSet(false, true)) {
                        return;
                    }

                    scheduler.switchThread(); // mark thread switched

                    // do reschedule
                    boolean needFree = true;
                    try {
                        LOGGER.info("leader transferred reschedule");

                        final long restartNanos = System.nanoTime();
                        scheduler.addWaitLeaderNanos(restartNanos - startNanos);
                        if (scheduler.forward()) {
                            needFree = false;
                        }
                    } catch (Throwable t) {
                        LOGGER.error("leader transferred reschedule failed", t);
                        scheduler.getFrontend().close(); // close frontend connection
                    } finally {
                        if (needFree) {
                            scheduler.getPacket().close();
                        }
                    }
                };

                // queue in pending tasks
                HaManager.getInstance().getLeaderTransferredTask().add(runnable);
                if (!HaManager.getInstance().isLeaderTransferring()) {
                    // recheck again and submit immediately if no leader transferring
                    ProxyExecutor.getInstance().getExecutor().submit(runnable);
                }

                return true; // take packet and free it in dealing task
            }
        }
        return null;
    }
}
