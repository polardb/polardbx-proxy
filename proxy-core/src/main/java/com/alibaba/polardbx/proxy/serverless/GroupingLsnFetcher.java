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

import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class GroupingLsnFetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(GroupingLsnFetcher.class);

    private static final String FETCH_LSN_QUERY =
        "/* PolarDB-X-Proxy GroupingLsnFetcher */ select COMMIT_INDEX from information_schema.alisql_cluster_local limit 1";

    private static class LsnCallback {
        private final Consumer<Long> callback;
        private final AtomicBoolean invoked = new AtomicBoolean(false);

        public LsnCallback(Consumer<Long> callback) {
            this.callback = callback;
        }

        public void invoke(long lsn) {
            if (invoked.compareAndSet(false, true)) {
                try {
                    callback.accept(lsn);
                } catch (Throwable t) {
                    LOGGER.warn("invoke lsn callback failed", t);
                }
            }
        }

        public void cancel() {
            if (invoked.compareAndSet(false, true)) {
                try {
                    callback.accept(null);
                } catch (Throwable t) {
                    LOGGER.warn("invoke lsn callback failed", t);
                }
            }
        }
    }

    private static final List<LsnCallback> tasks = new ArrayList<>();

    private static void fetchLsnTask() {
        List<LsnCallback> pending;
        synchronized (tasks) {
            while (tasks.isEmpty()) {
                try {
                    tasks.wait();
                } catch (InterruptedException ignore) {
                }
            }
            pending = new ArrayList<>(tasks);
            tasks.clear();
        }

        // get LSN
        final AtomicLong commitIndex = new AtomicLong(0);
        int retryCount = 0;

        do {
            // add new waited
            synchronized (tasks) {
                if (!tasks.isEmpty()) {
                    pending.addAll(tasks);
                    tasks.clear();
                }
            }

            final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(FastConfig.fetchLsnTimeout);
            try (final BackendConnectionWrapper connection = HaManager.getInstance().getAdminConnection()) {
                final QueryResultHandler result = connection.sendQuery(FETCH_LSN_QUERY);
                result.consume(row -> {
                    if (row[0] != null && row[0].length > 0) {
                        commitIndex.setPlain(Long.parseLong(new String(row[0])));
                    }
                }, limitTimeNs);
            } catch (Throwable t) {
                LOGGER.warn("fetch lsn failed", t);
            }
        } while (0 == commitIndex.getPlain() && ++retryCount < FastConfig.fetchLsnRetryTimes);

        // report success
        final long index = commitIndex.getPlain();
        if (index != 0) {
            for (LsnCallback task : pending) {
                task.invoke(index);
            }
        } else {
            for (LsnCallback task : pending) {
                task.cancel();
            }
        }
    }

    static {
        final Runnable lsnAsyncTask = () -> {
            while (true) {
                try {
                    fetchLsnTask();
                } catch (Throwable t) {
                    LOGGER.error("fetch lsn failed", t);
                }
            }
        };

        final Thread thread = new Thread(lsnAsyncTask, "LsnFetcher");
        thread.setDaemon(true);
        thread.start();
    }

    public static void fetchLsn(Consumer<Long> callback) {
        final LsnCallback task = new LsnCallback(callback);
        synchronized (tasks) {
            tasks.add(task);
            tasks.notify();
        }
    }
}
