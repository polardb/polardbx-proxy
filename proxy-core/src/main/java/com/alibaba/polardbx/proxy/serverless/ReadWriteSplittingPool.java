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
import com.alibaba.polardbx.proxy.common.XClusterNodeHealth;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.connection.pool.BackendPool;
import com.alibaba.polardbx.proxy.net.NIOWorker;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ReadWriteSplittingPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadWriteSplittingPool.class);

    @Getter
    private static class WeightTable {
        private final String tag;
        private final int weight;

        public WeightTable(String tag, int weight) {
            this.tag = tag;
            this.weight = weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof WeightTable)) {
                return false;
            }
            final WeightTable that = (WeightTable) o;
            return weight == that.weight && Objects.equals(tag, that.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tag, weight);
        }

        @Override
        public String toString() {
            return '{' + tag + '@' + weight + '}';
        }
    }

    private final HaManager haManager;
    private final NIOWorker nioWorker;
    private final AtomicReference<BackendPool> rwPoolRef = new AtomicReference<>();
    private final AtomicReference<WeightTable[]> weightTableRef = new AtomicReference<>();
    @Getter
    private final Map<String, BackendPool> roPoolMap = new ConcurrentHashMap<>();

    // protected by this synchronized block
    private List<WeightTable> lastWeightTableList = null;

    public ReadWriteSplittingPool(@NotNull final HaManager haManager, @NotNull final NIOWorker nioWorker) {
        this.haManager = haManager;
        this.nioWorker = nioWorker;
    }

    private void updateRwPool(@NotNull final XClusterNodeHealth leader) {
        final BackendPool rw = rwPoolRef.getAcquire();
        final SocketAddress address = AddressDecoder.decode(leader.getTag());
        if (null == rw || !rw.getAddress().equals(address) || !Objects.equals(rw.getProxyToken(),
            leader.getProxyToken())) {
            // update rw pool
            final String username = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_USERNAME);
            final String encryptedPassword = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_PASSWORD);
            final int maxPooled = Integer.parseInt(
                ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_RW_MAX_POOLED_SIZE));

            final long startNanos = System.nanoTime();
            final BackendPool newRw = new BackendPool(
                nioWorker, address, leader.getProxyToken(), username, encryptedPassword, null, maxPooled, false);
            newRw.loadDbConfigs();
            final BackendPool original = rwPoolRef.getAndSet(newRw);
            if (original != null) {
                original.close();
            }
            LOGGER.info("Backend cluster RW pool changed to: {}, cost {} ms", leader.getTag(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        }
    }

    private synchronized void updateRoPools(@NotNull final HaManager.XClusterServerless serverless) {
        // update slaves
        final boolean rwSplitting =
            Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.ENABLE_READ_WRITE_SPLITTING));
        final boolean followerRead =
            Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.ENABLE_FOLLOWER_READ));
        final boolean leaderRead =
            Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.ENABLE_LEADER_IN_RO_POOLS));
        final String username = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_USERNAME);
        final String encryptedPassword = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_PASSWORD);
        final int maxPooled = Integer.parseInt(
            ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_RO_MAX_POOLED_SIZE));
        final int latencyThreshold = Integer.parseInt(
            ConfigLoader.PROPERTIES.getProperty(ConfigProps.SLAVE_READ_LATENCY_THRESHOLD));
        final String readWeights = ConfigLoader.PROPERTIES.getProperty(ConfigProps.READ_WEIGHTS);
        final Map<String, Integer> weightMap = new HashMap<>();
        final String[] splits = readWeights.split(",");
        for (final String item : splits) {
            final String trim = item.trim();
            if (trim.isEmpty()) {
                continue;
            }
            final String[] pair = trim.split("@");
            if (pair.length != 2) {
                continue;
            }
            final String address = pair[0].trim();
            final int weight = Integer.parseInt(pair[1]);
            if (weight > 100) {
                throw new IllegalArgumentException("weight must be between 0 and 100");
            }
            weightMap.put(address, weight);
        }

        final Set<String> validNodes =
            new HashSet<>(
                serverless.getLearners().size() + (followerRead ? 0 : serverless.getFollowers().size()) + (leaderRead ?
                    1 : 0));

        // add if not exist
        final XClusterNodeHealth leader = serverless.getLeader();
        final String leaderTag = null == leader ? null : leader.getTag();
        if (rwSplitting) {
            for (final XClusterNodeHealth learner : serverless.getLearners()) {
                validNodes.add(learner.getTag());
                // check exists and token
                final BackendPool existing = roPoolMap.get(learner.getTag());
                if (null == existing || !Objects.equals(existing.getProxyToken(), learner.getProxyToken())) {
                    final long startNanos = System.nanoTime();
                    final BackendPool pool =
                        new BackendPool(nioWorker, AddressDecoder.decode(learner.getTag()), learner.getProxyToken(),
                            username, encryptedPassword, null, maxPooled, true);
                    pool.loadDbConfigs();
                    final BackendPool replaced = roPoolMap.put(learner.getTag(), pool);
                    if (null == replaced) {
                        LOGGER.info("Backend cluster RO pool learner: {} with token {} added, cost {} ms",
                            learner.getTag(),
                            learner.getProxyToken(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                    } else {
                        try {
                            replaced.close();
                        } catch (Throwable t) {
                            LOGGER.error("Failed to close backend pool: {}", replaced, t);
                        }
                        LOGGER.info("Backend cluster RO pool learner: {} replace for new token {}, cost {} ms",
                            learner.getTag(),
                            learner.getProxyToken(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                    }
                }
            }
            if (followerRead) {
                for (final XClusterNodeHealth follower : serverless.getFollowers()) {
                    validNodes.add(follower.getTag());
                    // check exists and token
                    final BackendPool existing = roPoolMap.get(follower.getTag());
                    if (null == existing || !Objects.equals(existing.getProxyToken(), follower.getProxyToken())) {
                        final long startNanos = System.nanoTime();
                        final BackendPool pool =
                            new BackendPool(nioWorker, AddressDecoder.decode(follower.getTag()),
                                follower.getProxyToken(), username, encryptedPassword, null, maxPooled, true);
                        pool.loadDbConfigs();
                        final BackendPool replaced = roPoolMap.put(follower.getTag(), pool);
                        if (null == replaced) {
                            LOGGER.info("Backend cluster RO pool follower: {} with token {} added, cost {} ms",
                                follower.getTag(),
                                follower.getProxyToken(),
                                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                        } else {
                            try {
                                replaced.close();
                            } catch (Throwable t) {
                                LOGGER.error("Failed to close backend pool: {}", replaced, t);
                            }
                            LOGGER.info("Backend cluster RO pool follower: {} replace for new token {}, cost {} ms",
                                follower.getTag(), follower.getProxyToken(),
                                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                        }
                    }
                }
            }
            if (leaderRead && leaderTag != null) {
                validNodes.add(leaderTag);
                // check exists and token
                final BackendPool existing = roPoolMap.get(leaderTag);
                if (null == existing || !Objects.equals(existing.getProxyToken(), leader.getProxyToken())) {
                    final long startNanos = System.nanoTime();
                    final BackendPool pool =
                        new BackendPool(nioWorker, AddressDecoder.decode(leaderTag), leader.getProxyToken(), username,
                            encryptedPassword, null, maxPooled, false);
                    pool.loadDbConfigs();
                    final BackendPool replaced = roPoolMap.put(leaderTag, pool);
                    if (null == replaced) {
                        LOGGER.info("Backend cluster RO pool leader: {} with token {} added, cost {} ms", leaderTag,
                            leader.getProxyToken(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                    } else {
                        try {
                            replaced.close();
                        } catch (Throwable t) {
                            LOGGER.error("Failed to close backend pool: {}", replaced, t);
                        }
                        LOGGER.info("Backend cluster RO pool leader: {} replace for new token {}, cost {} ms",
                            leaderTag, leader.getProxyToken(),
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                    }
                }
            }
        }

        final List<WeightTable> weights = new ArrayList<>();
        if (weightMap.isEmpty()) {
            // check latency
            for (final String node : validNodes) {
                final Long latencyNanos = haManager.getLatencyChecker().getLatencyNanos(node);
                if (!node.equals(leaderTag) && (null == latencyNanos
                    || latencyNanos > TimeUnit.MILLISECONDS.toNanos(latencyThreshold))) {
                    if (null == latencyNanos) {
                        LOGGER.info("Kick customized RO node {} caused by unknown latency.", node);
                    } else {
                        LOGGER.info("Kick customized RO node {} caused by latency {}ms > {}ms.", node,
                            latencyNanos / 1000000.f, latencyThreshold);
                    }
                    continue;
                }
                weights.add(new WeightTable(node, 1));
            }
        } else {
            for (final Map.Entry<String, Integer> entry : weightMap.entrySet()) {
                final String address = entry.getKey();
                final Long latencyNanos = haManager.getLatencyChecker().getLatencyNanos(address);
                if (!address.equals(leaderTag) && (null == latencyNanos
                    || latencyNanos > TimeUnit.MILLISECONDS.toNanos(latencyThreshold))) {
                    if (null == latencyNanos) {
                        LOGGER.info("Kick RO node {} caused by unknown latency.", address);
                    } else {
                        LOGGER.info("Kick RO node {} caused by latency {}ms > {}ms.", address, latencyNanos / 1000000.f,
                            latencyThreshold);
                    }
                    continue;
                }
                final int weight = entry.getValue();
                if (validNodes.contains(address)) {
                    weights.add(new WeightTable(address, weight));
                }
            }
        }
        if (weights.isEmpty()) {
            if (null == lastWeightTableList || !lastWeightTableList.isEmpty()) {
                LOGGER.info("Backend cluster RO pool select table update to empty.");
                lastWeightTableList = weights;
                weightTableRef.setRelease(null);
            } else {
                assert null == weightTableRef.getAcquire();
                assert lastWeightTableList.isEmpty();
            }
        } else {
            weights.sort(Comparator.comparing(o -> o.tag));
            if (null == lastWeightTableList || !lastWeightTableList.equals(weights)) {
                LOGGER.info("Backend cluster RO pool select table update to: {}", weights);
                lastWeightTableList = new ArrayList<>(weights); // store the sorted copy
                Collections.shuffle(weights);
                weightTableRef.setRelease(weights.toArray(WeightTable[]::new));
            } else {
                assert weightTableRef.getAcquire() != null;
                assert lastWeightTableList.equals(weights);
            }
        }

        // free which not available
        Iterator<Map.Entry<String, BackendPool>> iterator = roPoolMap.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<String, BackendPool> entry = iterator.next();
            if (!validNodes.contains(entry.getKey())) {
                try {
                    entry.getValue().close();
                    LOGGER.info("Backend cluster RO pool: {} removed", entry.getKey());
                } catch (Throwable t) {
                    LOGGER.error("Failed to close backend pool: {}", entry.getKey(), t);
                }
                iterator.remove();
            }
        }
    }

    // package private
    void update() {
        final HaManager.XClusterServerless serverless = haManager.getClusterServerless();
        if (null == serverless) {
            return;
        }

        // update leader
        final XClusterNodeHealth leader = serverless.getLeader();
        if (leader != null) {
            updateRwPool(leader);
        }

        // and RO
        updateRoPools(serverless);
    }

    public BackendConnectionWrapper getRwConnection() {
        final BackendPool pool = rwPoolRef.getAcquire();
        if (null == pool) {
            throw new IllegalStateException("Backend RW pool is not initialized.");
        }
        try {
            return pool.getConnection();
        } catch (Exception e) {
            LOGGER.error("Failed to get RW connection.", e);
            throw new RuntimeException(e);
        }
    }

    public BackendPool getRwPool() {
        return rwPoolRef.getAcquire();
    }

    public boolean isRoAvailable() {
        final WeightTable[] weightTable = weightTableRef.getAcquire();
        return null != weightTable && weightTable.length > 0;
    }

    public BackendConnectionWrapper getRoConnection() {
        final WeightTable[] weightTable = weightTableRef.getAcquire();
        if (null == weightTable || 0 == weightTable.length) {
            return null; // no available RO
        }
        float smallest = Float.MAX_VALUE;
        BackendPool pool = null;
        for (final WeightTable w : weightTable) {
            final BackendPool p = roPoolMap.get(w.tag);
            if (null == p) {
                continue;
            }
            // prefer slave node if no active connection
            final float load = ((float) p.getNowRunningConnectionCount() + (p.isSlave() ? 0 : 1)) / w.weight;
            if (load < smallest) {
                smallest = load;
                pool = p;
            }
        }
        if (null == pool) {
            return null; // pool not found
        }
        try {
            return pool.getConnection();
        } catch (Exception e) {
            LOGGER.error("Failed to get RO connection.", e);
            throw new RuntimeException(e);
        }
    }

    public Map<String, Integer> getNowRoWeights() {
        final WeightTable[] weightTable = weightTableRef.getAcquire();
        if (null == weightTable || 0 == weightTable.length) {
            return Collections.emptyMap();
        }
        final Map<String, Integer> weights = new HashMap<>();
        for (final WeightTable w : weightTable) {
            weights.put(w.tag, w.weight);
        }
        return Collections.unmodifiableMap(weights);
    }
}
