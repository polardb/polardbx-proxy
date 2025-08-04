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

package com.alibaba.polardbx.proxy.serverless;

import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SmoothSwitchoverMonitor extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(SmoothSwitchoverMonitor.class);

    private static final String FETCH_SQL =
        "/* PolarDB-X-Proxy SmoothSwitchoverMonitor */ show global status like 'consensus_in_leader_transfer'";

    private SmoothSwitchoverMonitor() {
        super(ThreadNames.SMOOTH_SWITCHOVER_MONITOR);

        // set thread and start
        setDaemon(true);
        start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (FastConfig.enableSmoothSwitchover) {
                    final int timeout = Math.min(FastConfig.smoothSwitchoverCheckInterval, 3000);
                    final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
                    try (final BackendConnectionWrapper connection = HaManager.getInstance().getAdminConnection()) {
                        final QueryResultHandler leaderTransferResult = connection.sendQuery(FETCH_SQL);
                        final AtomicBoolean leaderTransfer = new AtomicBoolean(false);
                        leaderTransferResult.consume(row -> {
                            final String val = new String(row[1]);
                            if (val.equalsIgnoreCase("1")) {
                                leaderTransfer.setPlain(true);
                            }
                        }, limitTimeNs);
                        if (leaderTransfer.getPlain()) {
                            HaManager.getInstance().markLeaderTransferring(connection.getBackendPool().getAddress());
                        }
                    } catch (RuntimeException e) {
                        // notify fast HA
                        HaManager.getInstance().refresh();
                        throw e;
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("SmoothSwitchoverMonitor error", t);
            }

            try {
                Thread.sleep(FastConfig.smoothSwitchoverCheckInterval);
            } catch (Throwable t) {
                LOGGER.error("SmoothSwitchoverMonitor sleep error", t);
            }
        }
    }

    private static SmoothSwitchoverMonitor INSTANCE;

    public static void init() throws InterruptedException {
        if (null == INSTANCE) {
            synchronized (HaManager.class) {
                if (null == INSTANCE) {
                    INSTANCE = new SmoothSwitchoverMonitor();
                    LOGGER.info("SmoothSwitchoverMonitor started.");
                }
            }
        }
    }
}
