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
import com.alibaba.polardbx.proxy.cluster.GlobalMock;
import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.serverless.GroupingLsnFetcher;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FetchLsnTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchLsnTask.class);

    private boolean fetchLsnAndForward(Scheduler scheduler) {
        final long fetchStartNanos = System.nanoTime();
        GroupingLsnFetcher.fetchLsn(lsn -> {
            scheduler.switchThread(); // mark thread switched

            final long fetchDoneNanos = System.nanoTime();
            ProxyExecutor.getInstance().getExecutor().submit(() -> {
                boolean needFree = true;
                try {
                    final long scheduleNanos = System.nanoTime();
                    scheduler.addFetchLsnNanos(fetchDoneNanos - fetchStartNanos);
                    scheduler.addScheduleNanos(scheduleNanos - fetchDoneNanos);

                    final boolean taken;
                    if (null == lsn || GlobalMock.mockFailedToFetchLsn(scheduler.getContext())) {
                        // force go leader
                        scheduler.setSlaveRead(false);
                        taken = scheduler.errorHandle(new IOException("Failed to fetch LSN from leader"));
                    } else {
                        // got LSN, go normal routine with specificLsn
                        scheduler.setSpecificLsn(lsn);
                        taken = scheduler.forward();
                    }
                    if (taken) {
                        needFree = false;
                    }
                } catch (Throwable t) {
                    LOGGER.error("fetch lsn callback failed", t);
                    scheduler.getFrontend().close(); // close frontend connection
                } finally {
                    if (needFree) {
                        scheduler.getPacket().close();
                    }
                }
            });
        });
        return true; // take packet and free it in dealing task
    }

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        if (null == scheduler.getSlaveRead() || !scheduler.getSlaveRead()) {
            return null; // go leader
        }
        if (null != scheduler.getSpecificLsn()) {
            return null; // LSN got
        }
        if (FastConfig.enableStaleRead) {
            return null; // stale read
        }
        if (scheduler.getBackend() != null && !scheduler.getBackend().isSlave()) {
            return null; // go leader
        }

        final boolean roAvailable = HaManager.getInstance().getReadWriteSplittingPool().isRoAvailable();
        if (!roAvailable) {
            scheduler.setSlaveRead(false);
            return null; // no slave
        }

        return fetchLsnAndForward(scheduler);
    }
}
