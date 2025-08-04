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

package com.alibaba.polardbx.proxy.connection.pool;

import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.BackendConnection;
import com.alibaba.polardbx.proxy.connection.configs.ReadOnlyConfigs;
import com.alibaba.polardbx.proxy.net.NIOWorker;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.utils.BytesTools;
import com.alibaba.polardbx.proxy.utils.CaseInsensitiveString;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BackendPool implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackendPool.class);

    private final NIOWorker nioWorker;
    @Getter
    private final SocketAddress address;
    @Getter
    private final String proxyToken;
    private final String username;
    private final String encryptedPassword;
    private final String defaultDatabase;

    private final Queue<BackendConnection> connections = new ConcurrentLinkedQueue<>();
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    // package private
    final AtomicInteger connectionRunning = new AtomicInteger(0);

    @Getter
    private volatile int maxPooled;

    // slave mark
    @Getter
    private final boolean slave;

    @Getter
    private final ReadOnlyConfigs readOnlyConfigs = new ReadOnlyConfigs();
    @Getter
    private final Map<CaseInsensitiveString, String> globalVariables = new ConcurrentHashMap<>();
    private final AtomicLong globalVariablesRefreshTime = new AtomicLong(0);

    /**
     * Connection pool to backend.
     *
     * @param nioWorker NIO worker.
     * @param address IP of backend.
     * @param proxyToken Proxy token of backend.
     * @param username Username of backend.
     * @param encryptedPassword Encrypted password of backend.
     * @param defaultDatabase Default database of backend(can be null).
     * @param maxPooled Maximum number of connections in pool.
     * @param slave Slave mark.
     */
    public BackendPool(NIOWorker nioWorker, SocketAddress address, String proxyToken, String username,
                       String encryptedPassword, String defaultDatabase, int maxPooled, boolean slave) {
        this.nioWorker = nioWorker;
        this.address = address;
        this.proxyToken = proxyToken;
        this.username = username;
        this.encryptedPassword = encryptedPassword;
        this.defaultDatabase = defaultDatabase;
        this.maxPooled = Math.max(maxPooled, 0); // never less than 0 unless closed
        this.slave = slave;
    }

    public synchronized void setMaxPooled(int maxPooled) {
        if (this.maxPooled < 0) {
            return; // pool closed
        }
        this.maxPooled = Math.max(maxPooled, 0); // never less than 0 unless closed
    }

    public int getNowIdleConnectionCount() {
        return connectionCount.getAcquire();
    }

    public int getNowRunningConnectionCount() {
        return connectionRunning.getAcquire();
    }

    public BackendConnectionWrapper getConnection() throws IOException {
        while (true) {
            BackendConnection connection = connections.poll();
            if (connection != null) {
                connectionCount.getAndDecrement();
                if (!connection.isGood()) {
                    connection.close();
                    continue;
                }
            } else {
                connection =
                    BackendConnection.connectNonBlocking(address, nioWorker.getProcessor(), username, encryptedPassword,
                        defaultDatabase);
                connection.setPoolInfo(slave, readOnlyConfigs, globalVariables);
            }
            return new BackendConnectionWrapper(this, connection);
        }
    }

    // package invoke only
    void release(BackendConnection connection) {
        if (null == connection) {
            return;
        }
        try {
            // resume read monitor
            connection.enableRead();
        } catch (Throwable ignore) {
            LOGGER.info("Error when resume read monitor on {}, just close it.", connection);
            connection.close();
            return;
        }
        if (!connection.isGood() || connection.hasPendingUserRequests()) {
            LOGGER.info("Close bad connection which is not good. {}", connection);
            connection.close();
            return;
        }

        final boolean reuse;
        synchronized (this) {
            // this synchronized is to avoid leak when pool closed
            final int before = connectionCount.getAndIncrement();
            reuse = before < maxPooled;
        }
        if (reuse) {
            connections.offer(connection);
            return;
        }
        connectionCount.getAndDecrement();
        connection.close();
    }

    public void refreshPool(float ratio, long checkIdleThreshNanos, Executor executor, String sql, long timeoutNanos) {
        final int maxLoop = connectionCount.getAcquire();
        final int maxRefresh = (int) Math.ceil(maxLoop * ratio);
        int refreshed = 0;
        for (int i = 0; i < maxLoop; ++i) {
            final BackendConnection connection = connections.poll();
            if (null == connection) {
                break;
            }
            connectionCount.getAndDecrement();
            if (connection.idleTime() <= checkIdleThreshNanos) {
                // ignore and put it back to pool
                release(connection); // check good internal
                // assume next is newer than this, just break
                break;
            } else if (connection.isGood()) {
                // do refresh
                executor.execute(() -> {
                    try {
                        final long limitNanos = System.nanoTime() + timeoutNanos;
                        final QueryResultHandler handler = connection.sendQuery(sql);
                        handler.consume(row -> {
                        }, limitNanos);
                        // done and put it back to pool
                        release(connection);
                    } catch (Throwable t0) {
                        LOGGER.error("refresh pool error", t0);
                        try {
                            // close it anyway
                            connection.close();
                        } catch (Throwable t1) {
                            LOGGER.error("refresh pool close error", t1);
                        }
                    }
                });
                if (++refreshed >= maxRefresh) {
                    break;
                }
            } else {
                connection.close();
            }
        }

        // and global variables
        final long nowNanos = System.nanoTime();
        final long lastRefreshNanos = globalVariablesRefreshTime.getAcquire();
        final int interval =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.GLOBAL_VARIABLES_REFRESH_INTERVAL));
        if (0 == lastRefreshNanos || nowNanos - lastRefreshNanos >= TimeUnit.MILLISECONDS.toNanos(interval)) {
            if (globalVariablesRefreshTime.compareAndSet(lastRefreshNanos, nowNanos)) {
                executor.execute(() -> {
                    final int timeout =
                        Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_CONNECT_TIMEOUT));
                    final long limitTimeNs = nowNanos + TimeUnit.MILLISECONDS.toNanos(timeout);
                    try (final BackendConnectionWrapper connection = getConnection()) {
                        final QueryResultHandler resultHandler =
                            connection.sendQuery("/* PolarDB-X-Proxy BackendPool */ show global variables");
                        resultHandler.consume(row -> {
                                try {
                                    final String key = new String(row[0],
                                        CharsetMapping.getStaticJavaEncodingForCollationIndex(
                                            resultHandler.getFields().get(0).getCharacterSet()));
                                    final String rawValue = new String(row[1],
                                        CharsetMapping.getStaticJavaEncodingForCollationIndex(
                                            resultHandler.getFields().get(1).getCharacterSet()));
                                    final String numberRegex = "^[+-]?\\d*(\\.\\d+)?([eE][+-]?\\d+)?$";
                                    final String value = null == row[1] ? "null" :
                                        (!rawValue.isEmpty() && rawValue.matches(numberRegex) ? rawValue :
                                            '\'' + rawValue.replaceAll("'", "''") + '\'');
                                    globalVariables.put(new CaseInsensitiveString(key), value);
                                } catch (Throwable t) {
                                    LOGGER.error("parse global variable error, [{}, {}]",
                                        null == row[0] ? null : BytesTools.bytes2Hex(row[0]),
                                        null == row[1] ? null : BytesTools.bytes2Hex(row[1]), t);
                                }
                            },
                            limitTimeNs);
                    } catch (Throwable t) {
                        LOGGER.error("refresh global variables error", t);
                    }
                });
            }
        }
    }

    // Caution: This is a blocking function, and should never be invoked in reactor thread.
    public void loadDbConfigs() {
        // check thread first
        if (ThreadNames.isThread(ThreadNames.NIO_PROCESSOR)) {
            throw new IllegalStateException("loadDbConfigs should never be invoked in reactor thread");
        }

        final int timeout =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_CONNECT_TIMEOUT));
        final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        try (final BackendConnectionWrapper connection = getConnection()) {
            // refresh token first
            final QueryResultHandler resultHandler =
                connection.sendQuery("/* PolarDB-X-Proxy BackendPool */ show variables like 'lower_case_table_names'");
            resultHandler.consume(row -> readOnlyConfigs.setLowerCaseTableNames("1".equals(new String(row[1]))),
                limitTimeNs);
        } catch (Throwable t) {
            LOGGER.error("load db configs error", t);
        }
    }

    @Override
    public synchronized void close() {
        // synchronized and set max to 0
        maxPooled = Integer.MIN_VALUE;
        BackendConnection connection;
        while ((connection = connections.poll()) != null) {
            connection.close();
        }
        connectionCount.setRelease(0);
    }
}
