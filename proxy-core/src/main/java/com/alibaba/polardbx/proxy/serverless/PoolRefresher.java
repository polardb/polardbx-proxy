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

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.pool.BackendPool;
import com.alibaba.polardbx.proxy.utils.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PoolRefresher {
    private static final Logger LOGGER = LoggerFactory.getLogger(PoolRefresher.class);

    private static ThreadPoolExecutor checkerThreads = null;

    // package private
    static void startBackendPoolRefresh() {
        final int taskInterval =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_POOL_REFRESH_TASK_INTERVAL));

        ProxyExecutor.getInstance().getTimer().scheduleAtFixedRate(
            () -> {
                try {
                    synchronized (PoolRefresher.class) {
                        final long startTimeNanos = System.nanoTime();

                        // start thread pool first
                        if (null == checkerThreads) {
                            final int threads = Integer.parseInt(
                                ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_POOL_REFRESH_THREADS));
                            checkerThreads = new ThreadPoolExecutor(threads,
                                threads,
                                0L,
                                TimeUnit.MILLISECONDS,
                                new LinkedBlockingQueue<>(),
                                new NamedThreadFactory(ThreadNames.BACKEND_POOL_REFRESHER_EXECUTOR));
                        }

                        final int interval = Integer.parseInt(
                            ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_POOL_REFRESH_INTERVAL));
                        final int timeout = Integer.parseInt(
                            ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_POOL_REFRESH_TIMEOUT));
                        int poolNumber = 0;
                        try {
                            // get admin, RW, RO pool
                            final HaManager haManager = HaManager.getInstance();
                            final ArrayList<BackendPool> pools = new ArrayList<>();
                            final BackendPool adminPool = haManager.getAdminPool();
                            if (adminPool != null) {
                                pools.add(adminPool);
                            }
                            final BackendPool rwPool = haManager.getReadWriteSplittingPool().getRwPool();
                            if (rwPool != null) {
                                pools.add(rwPool);
                            }
                            pools.addAll(haManager.getReadWriteSplittingPool().getRoPoolMap().values());
                            poolNumber = pools.size();

                            final String sql =
                                ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_POOL_REFRESH_SQL);

                            // refresh all pools
                            for (final BackendPool pool : pools) {
                                final long intervalNanos = TimeUnit.MILLISECONDS.toNanos(interval);
                                pool.refreshPool(((float) taskInterval) / interval, intervalNanos,
                                    checkerThreads, sql, TimeUnit.MILLISECONDS.toNanos(timeout));
                            }
                        } catch (Throwable t) {
                            LOGGER.error("Failed to enum and refresh backend pool", t);
                        }

                        // wait all worker done
                        long nowNanos;
                        while (
                            (nowNanos = System.nanoTime()) - startTimeNanos < 2 * TimeUnit.MILLISECONDS.toNanos(timeout)
                                && checkerThreads.getCompletedTaskCount() < checkerThreads.getTaskCount()) {
                            // still tasks pending
                            Thread.sleep(100); // 100ms
                        }
                        LOGGER.debug("Refresh backend {} pool done, {} ms", poolNumber,
                            (nowNanos - startTimeNanos) / 1000_000L);

                        if (checkerThreads.getCompletedTaskCount() < checkerThreads.getTaskCount()) {
                            // still task running, force shutdown
                            try {
                                checkerThreads.shutdown();
                                checkerThreads.awaitTermination(timeout, TimeUnit.MILLISECONDS);
                            } catch (Throwable t) {
                                LOGGER.error("Failed to shutdown checker threads", t);
                            } finally {
                                try {
                                    checkerThreads.shutdownNow();
                                } catch (Throwable t) {
                                    LOGGER.error("Failed to shutdown checker threads", t);
                                }
                            }

                            // free thread pool
                            checkerThreads = null;
                        }
                    }
                } catch (Throwable t0) {
                    LOGGER.error("Failed to refresh backend pool", t0);

                    // force shutdown if error occurs
                    if (checkerThreads != null) {
                        try {
                            checkerThreads.shutdownNow();
                        } catch (Throwable t1) {
                            LOGGER.error("Failed to shutdown checker threads", t1);
                            // never throw in schedule task
                        }
                        checkerThreads = null;
                    }
                }
            },
            taskInterval,
            taskInterval,
            java.util.concurrent.TimeUnit.MILLISECONDS
        );
    }
}
