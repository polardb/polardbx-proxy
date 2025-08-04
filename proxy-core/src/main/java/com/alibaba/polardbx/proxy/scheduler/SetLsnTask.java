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
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetLsnTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(SetLsnTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        final BackendConnectionWrapper backend = scheduler.getBackend();
        if (null == backend || !backend.isSlave()) {
            return null; // ignore if not slave
        }

        Long specificLsn = scheduler.getSpecificLsn();
        if (null == specificLsn) {
            return null; // no LSN
        }

        final FrontendContext context = scheduler.getContext();
        if (GlobalMock.mockSetReadLsnTimeout(context)) {
            // mock timeout when wait LSN
            specificLsn = 9999999999L;
        }

        // enqueue wait LSN before request
        LOGGER.debug("get request with LSN {}", specificLsn);

        // set LSN and record slave state in backend context
        final long waitLsnStartNanos = System.nanoTime();
        backend.sendQuery("set read_lsn=" + specificLsn, context.getClientJavaCharset(),
            context.hasCapability(Capabilities.CLIENT_QUERY_ATTRIBUTES), (handler, before, state) -> {
                if (null == handler) {
                    throw new RuntimeException("failed to set read_lsn with early abort");
                }
                // now within lock and when error occurs, following read is not up-to-date
                final BackendContext backendContext = handler.getContextReference().getAcquire();
                assert backendContext != null;
                backendContext.setUpToDate(ResultState.OK == state);
                scheduler.addWaitLsnNanos(System.nanoTime() - waitLsnStartNanos);
                if (state != ResultState.OK) {
                    // throw and do fast abort and back to leader
                    throw new RuntimeException("failed to set read_lsn");
                }
            });

        return null;
    }
}
