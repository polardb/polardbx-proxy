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

import com.alibaba.polardbx.proxy.common.AddressDecoder;
import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.common.XClusterNodeHealth;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.BackendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.connection.pool.BackendPool;
import com.alibaba.polardbx.proxy.net.NIOWorker;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LatencyChecker extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LatencyChecker.class);

    private static final String CLUSTER_LOCAL_QUERY =
        "/* PolarDB-X-Proxy LatencyChecker */ select ROLE, COMMIT_INDEX, LAST_APPLY_INDEX from information_schema.alisql_cluster_local limit 1";
    private static final String PROXY_TOKEN_QUERY = "/* PolarDB-X-Proxy HaManager */ call dbms_proxy.get_token()";

    private final NIOWorker nioWorker;
    private final Executor executor;
    private final HaManager haManager;

    private final ConcurrentSkipListMap<Long, Long> latencyHistoryNanos = new ConcurrentSkipListMap<>();
    private final Map<String, Long> latencyMapNanos = new ConcurrentHashMap<>();

    public LatencyChecker(@NotNull final NIOWorker nioWorker, @NotNull final Executor executor,
                          @NotNull final HaManager haManager) {
        super(ThreadNames.LATENCY_CHECKER);
        this.nioWorker = nioWorker;
        this.executor = executor;
        this.haManager = haManager;

        // set thread and start
        setDaemon(true);
        start();
    }

    public Long getLatencyNanos(final String address) {
        return latencyMapNanos.get(address);
    }

    private XClusterNodeHealth recordLeaderIndexes(@NotNull final String expectedTag) {
        final BackendPool pool = haManager.getAdminPool();
        if (null == pool) {
            return null;
        }
        if (!AddressDecoder.decode(expectedTag).equals(pool.getAddress())) {
            return null; // abort if not equal leader
        }

        // try query with admin pool
        final AtomicReference<XClusterNodeHealth> newNodeHealthRef = new AtomicReference<>();
        final int timeout =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.LATENCY_CHECK_TIMEOUT));
        final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        try (final BackendConnectionWrapper connection = pool.getConnection()) {
            // refresh token first
            final QueryResultHandler proxyTokenResult = connection.sendQuery(PROXY_TOKEN_QUERY);
            final AtomicReference<String> proxyTokenRef = new AtomicReference<>();
            try {
                proxyTokenResult.consume(row -> proxyTokenRef.setPlain(new String(row[0])), limitTimeNs);
            } catch (SQLException ignore) {
                // may no such variable
            }
            final String proxyToken = proxyTokenRef.getPlain();

            // record actual query send time
            final long queryNanos = System.nanoTime();
            final QueryResultHandler localResult = connection.sendQuery(CLUSTER_LOCAL_QUERY);
            localResult.consume(row -> {
                if (null == row[0] || 0 == row[0].length || !new String(row[0]).equalsIgnoreCase("Leader")) {
                    return; // ignore if not leader
                }
                if (row[1] != null && row[1].length > 0 && row[2] != null && row[2].length > 0) {
                    final long commitIndex = Long.parseLong(new String(row[1]));
                    final long applyIndex = Long.parseLong(new String(row[2]));
                    final long nowNanos = System.nanoTime();
                    final long rttNanos = nowNanos - queryNanos;
                    final long indexNanos = queryNanos + rttNanos / 2;
                    // record history
                    latencyHistoryNanos.put(commitIndex, indexNanos);
                    // gen new health info
                    newNodeHealthRef.setPlain(new XClusterNodeHealth(
                        expectedTag, "Leader", proxyToken, commitIndex, applyIndex, rttNanos, indexNanos));
                }
            }, limitTimeNs);
        } catch (Throwable t) {
            LOGGER.error("Failed to record leader indexes", t);
        }

        // keep at most LATENCY_RECORD_COUNT indexes
        final int count =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.LATENCY_RECORD_COUNT));
        while (latencyHistoryNanos.size() > count) {
            latencyHistoryNanos.pollFirstEntry();
        }
        return newNodeHealthRef.getPlain();
    }

    private XClusterNodeHealth updateLatency(final String address) {
        final InetSocketAddress socketAddress = AddressDecoder.decode(address);
        final String username = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_USERNAME);
        final String encryptedPassword = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_PASSWORD);
        final int timeout =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.LATENCY_CHECK_TIMEOUT));
        final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);

        final AtomicReference<XClusterNodeHealth> newNodeHealthRef = new AtomicReference<>();
        try (final BackendConnection connection = BackendConnection.connectBlocking(socketAddress,
            nioWorker.getProcessor(), username, encryptedPassword, null, timeout)) {
            // refresh token first
            final QueryResultHandler proxyTokenResult = connection.sendQuery(PROXY_TOKEN_QUERY);
            final AtomicReference<String> proxyTokenRef = new AtomicReference<>();
            try {
                proxyTokenResult.consume(row -> proxyTokenRef.setPlain(new String(row[0])), limitTimeNs);
            } catch (SQLException ignore) {
                // may no such variable
            }
            final String proxyToken = proxyTokenRef.getPlain();

            // record actual query send time
            final long queryNanos = System.nanoTime();
            final QueryResultHandler localResult = connection.sendQuery(CLUSTER_LOCAL_QUERY);
            localResult.consume(row -> {
                final String role;
                if (row[0] != null && row[0].length > 0) {
                    role = new String(row[0]);
                } else {
                    role = null;
                }
                if (role != null && row[1] != null && row[1].length > 0 && row[2] != null && row[2].length > 0) {
                    final long commitIndex = Long.parseLong(new String(row[1]));
                    final long applyIndex = Long.parseLong(new String(row[2]));
                    final long nowNanos = System.nanoTime();
                    final long rttNanos = nowNanos - queryNanos;
                    final long indexNanos = queryNanos + rttNanos / 2;
                    // calculate latency
                    final Map.Entry<Long, Long> floor = latencyHistoryNanos.floorEntry(applyIndex);
                    if (null == floor) {
                        latencyMapNanos.put(address, Long.MAX_VALUE);
                    } else {
                        if (applyIndex > floor.getKey()) {
                            final Map.Entry<Long, Long> higher = latencyHistoryNanos.higherEntry(applyIndex);
                            if (null == higher) {
                                latencyMapNanos.put(address, 0L);
                            } else {
                                final long calculatedNanos = floor.getValue()
                                    + (higher.getValue() - floor.getValue()) / (higher.getKey() - floor.getKey())
                                    * (applyIndex - floor.getKey());
                                latencyMapNanos.put(address, Math.max(0, indexNanos - calculatedNanos));
                            }
                        } else {
                            latencyMapNanos.put(address, Math.max(0, indexNanos - floor.getValue()));
                        }
                    }
                    // gen new health info
                    newNodeHealthRef.setPlain(new XClusterNodeHealth(
                        address, role, proxyToken, commitIndex, applyIndex, rttNanos, indexNanos));
                }
            }, limitTimeNs);
        } catch (Throwable t) {
            LOGGER.error("Failed to update latency", t);
        }
        return newNodeHealthRef.getPlain();
    }

    @Override
    public void run() {
        while (true) {
            try {
                final HaManager.XClusterServerless serverless = haManager.getClusterServerlessRef().getAcquire();
                if (serverless != null && serverless.getLeader() != null) {
                    // update leader latency map first
                    final XClusterNodeHealth newLeaderHealth = recordLeaderIndexes(serverless.getLeader().getTag());
                    if (newLeaderHealth != null) {
                        // only update when leader refreshed
                        final List<CompletableFuture<Void>> futures =
                            new ArrayList<>(serverless.getFollowers().size() + serverless.getLearners().size());
                        final List<XClusterNodeHealth> followers = new ArrayList<>(serverless.getFollowers().size());
                        final List<XClusterNodeHealth> learners = new ArrayList<>(serverless.getLearners().size());
                        for (final XClusterNodeHealth follower : serverless.getFollowers()) {
                            futures.add(CompletableFuture.runAsync(() -> {
                                final XClusterNodeHealth newFollowerHealth = updateLatency(follower.getTag());
                                if (newFollowerHealth != null) {
                                    synchronized (followers) {
                                        followers.add(newFollowerHealth);
                                    }
                                }
                            }, executor));
                        }
                        for (final XClusterNodeHealth learner : serverless.getLearners()) {
                            futures.add(CompletableFuture.runAsync(() -> {
                                final XClusterNodeHealth newLearnerHealth = updateLatency(learner.getTag());
                                if (newLearnerHealth != null) {
                                    synchronized (learners) {
                                        learners.add(newLearnerHealth);
                                    }
                                }
                            }, executor));
                        }
                        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

                        // update all cluster serverless if same count and not changed
                        if (serverless.getFollowers().size() == followers.size()
                            && serverless.getLearners().size() == learners.size()) {
                            final HaManager.XClusterServerless newServerless =
                                new HaManager.XClusterServerless(newLeaderHealth, followers, learners);
                            haManager.getClusterServerlessRef().compareAndSet(serverless, newServerless);
                        }

                        // remove out-dated latency
                        final Set<String> validSet = new HashSet<>(followers.size() + learners.size());
                        for (final XClusterNodeHealth follower : followers) {
                            validSet.add(follower.getTag());
                        }
                        for (final XClusterNodeHealth learner : learners) {
                            validSet.add(learner.getTag());
                        }
                        latencyMapNanos.keySet().removeIf(address -> !validSet.contains(address));
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("Failed to update latency", t);
            }

            LOGGER.debug("serverless: {}", haManager.getClusterServerlessRef().getAcquire());
            LOGGER.debug("Latency history: {}", latencyHistoryNanos);
            LOGGER.debug("Latency map: {} in ns", latencyMapNanos);

            try {
                final int interval =
                    Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.LATENCY_CHECK_INTERVAL));
                Thread.sleep(interval);
            } catch (Throwable t) {
                LOGGER.error("Latency updater sleep error", t);
            }
        }
    }
}
