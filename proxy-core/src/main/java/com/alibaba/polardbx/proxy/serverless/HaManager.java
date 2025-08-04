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
import com.alibaba.polardbx.proxy.common.AddressDecoder;
import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.common.XClusterNodeBasic;
import com.alibaba.polardbx.proxy.common.XClusterNodeHealth;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.connection.BackendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.connection.pool.BackendPool;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.dynamic.DynamicConfig;
import com.alibaba.polardbx.proxy.net.NIOWorker;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Check leader of XCluster and do failover if necessary.
 * Persist leader info in local file.
 */
public class HaManager extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(HaManager.class);

    private static final String BASIC_INFO_QUERY =
        "/* PolarDB-X-Proxy HaManager */ select @@cluster_id, @@port";
    private static final String LEGACY_XPORT_QUERY =
        "/* PolarDB-X-Proxy HaManager */ select @@rpc_use_legacy_port, @@polarx_port";
    private static final String RPC_PORT_QUERY = "/* PolarDB-X-Proxy HaManager */ select @@rpc_port";
    private static final String CLUSTER_LOCAL_QUERY =
        "/* PolarDB-X-Proxy HaManager */ select CURRENT_LEADER, ROLE, COMMIT_INDEX, LAST_APPLY_INDEX from information_schema.alisql_cluster_local limit 1";
    private static final String CLUSTER_GLOBAL_QUERY =
        "/* PolarDB-X-Proxy HaManager */ select IP_PORT, ROLE from information_schema.alisql_cluster_global";
    private static final String PROXY_TOKEN_QUERY = "/* PolarDB-X-Proxy HaManager */ call dbms_proxy.get_token()";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss ZZZZ");

    // context
    private final NIOWorker nioWorker;
    private final ThreadPoolExecutor executor;

    // HA context
    private final AtomicLong clusterIdRef = new AtomicLong(-1);
    private final AtomicInteger globalPortGapRef = new AtomicInteger(-8000); // default gap of AliCloud
    private final AtomicReference<List<XClusterNodeBasic>> clusterPeersRef = new AtomicReference<>();

    @Getter
    public static class XClusterServerless {
        private final XClusterNodeHealth leader;
        private final List<XClusterNodeHealth> followers;
        private final List<XClusterNodeHealth> learners;

        public XClusterServerless(XClusterNodeHealth leader, List<XClusterNodeHealth> followers,
                                  List<XClusterNodeHealth> learners) {
            this.leader = leader;
            this.followers = followers;
            this.learners = learners;
        }

        @Override
        public String toString() {
            return "XClusterServerless{" +
                "leader=" + leader +
                ", followers=" + followers +
                ", learners=" + learners +
                '}';
        }
    }

    // cluster serverless and health context
    private final AtomicReference<XClusterServerless> clusterServerlessRef = new AtomicReference<>();
    @Getter
    private final LatencyChecker latencyChecker;

    // pools
    @Getter
    private final ReadWriteSplittingPool readWriteSplittingPool;
    private final AtomicReference<BackendPool> adminPoolRef = new AtomicReference<>();

    // version info
    private final AtomicInteger version = new AtomicInteger(0);

    static class LeaderTransferInfo {
        public final SocketAddress address;
        public final long timeoutNanos;

        public LeaderTransferInfo(SocketAddress address, long timeoutNanos) {
            this.address = address;
            this.timeoutNanos = timeoutNanos;
        }
    }

    // for leader transfer info
    private final AtomicReference<LeaderTransferInfo> leaderTransferInfoRef = new AtomicReference<>();
    @Getter
    private final ConcurrentLinkedQueue<Runnable> leaderTransferredTask = new ConcurrentLinkedQueue<>();

    public HaManager(NIOWorker nioWorker, ThreadPoolExecutor executor) {
        super(ThreadNames.HA_MANAGER);
        this.nioWorker = nioWorker;
        this.executor = executor;

        // init pool context before thread start
        this.readWriteSplittingPool = new ReadWriteSplittingPool(this, nioWorker);

        // set thread and start
        setDaemon(true);
        start();

        // latency updater after thread start
        this.latencyChecker = new LatencyChecker(nioWorker, executor, this);
    }

    /**
     * Get X-Cluster addresses from dynamic config file.
     * Load static addresses only if static addresses is given.
     *
     * @return socket address set(from tag to prevent address/port forwarding in k8s)
     */
    private Set<String> getProbeAddresses(final List<XClusterNodeBasic> dynamic) {
        final Set<String> probeAddresses = new HashSet<>(4);
        // get existing dynamic config first
        final DynamicConfig nowConfig = DynamicConfig.getNowConfig();
        synchronized (nowConfig) {
            final List<XClusterNodeBasic> cluster = nowConfig.getXCluster();
            if (cluster != null) {
                dynamic.addAll(cluster);
                for (final XClusterNodeBasic node : cluster) {
                    probeAddresses.add(node.getTag());
                }
            }
        }
        // load port gap and cluster id
        dynamic.stream().filter(node -> node.getRole().equalsIgnoreCase("Leader")).findAny().ifPresent(node -> {
            // calc port gap and cluster id
            if (node.getPort() != -1 && node.getPaxosPort() != -1) {
                globalPortGapRef.setPlain(node.getPort() - node.getPaxosPort());
            }
            clusterIdRef.compareAndExchange(-1, node.getClusterId());
        });
        // and static config
        final AddressDecoder addresses =
            new AddressDecoder(ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_ADDRESS));
        probeAddresses.addAll(addresses.getAddressMap().keySet());
        return probeAddresses;
    }

    @Getter
    private static class XClusterNodeInfo {
        private final XClusterNodeBasic basic;
        private final XClusterNodeHealth health;

        public XClusterNodeInfo(XClusterNodeBasic basic, XClusterNodeHealth health) {
            this.basic = basic;
            this.health = health;
        }
    }

    /**
     * Probe X-Cluster node info.
     *
     * @param address target probe address
     * @return X-Cluster node info, with peers if leader, null if failed to connect and fetch
     */
    private XClusterNodeInfo getNodeInfo(final String address) {
        // load configs
        final InetSocketAddress socketAddress = AddressDecoder.decode(address);
        final String username = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_USERNAME);
        final String encryptedPassword = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_PASSWORD);
        final int timeout =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_HA_CHECK_TIMEOUT));
        final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);

        try (final BackendConnection connection = BackendConnection.connectBlocking(socketAddress,
            nioWorker.getProcessor(), username, encryptedPassword, null, timeout)) {
            // now is connected
            final BackendContext backendContext = connection.getContextReference().getAcquire();
            assert backendContext != null;
            final String version = backendContext.decodeStringResults(backendContext.getVersion());

            // check cluster id
            final AtomicLong myClusterIdRef = new AtomicLong(-1);
            final AtomicInteger serverPortRef = new AtomicInteger(-1);
            final QueryResultHandler basicInfoResult = connection.sendQuery(BASIC_INFO_QUERY);
            basicInfoResult.consume(row -> {
                myClusterIdRef.setPlain(Long.parseLong(new String(row[0])));
                serverPortRef.setPlain(Integer.parseInt(new String(row[1])));
            }, limitTimeNs);
            final long myClusterId = myClusterIdRef.getPlain();
            final long expectedClusterId = clusterIdRef.getPlain();
            if (expectedClusterId != -1 && myClusterId != expectedClusterId) {
                return null; // bad cluster id and should ignore it
            }
            final int serverPort = serverPortRef.getPlain();
            if (-1 == serverPort) {
                throw new IllegalStateException("Failed to fetch server port");
            }

            // probe proxy token
            final QueryResultHandler proxyTokenResult = connection.sendQuery(PROXY_TOKEN_QUERY);
            final AtomicReference<String> proxyTokenRef = new AtomicReference<>();
            try {
                proxyTokenResult.consume(row -> proxyTokenRef.setPlain(new String(row[0])), limitTimeNs);
            } catch (SQLException ignore) {
                // may no such variable
            }
            final String proxyToken = proxyTokenRef.getPlain();

            // probe my role and leader address first
            final long queryNanos = System.nanoTime();
            final QueryResultHandler localResult = connection.sendQuery(CLUSTER_LOCAL_QUERY);
            final AtomicReference<String> leaderRef = new AtomicReference<>();
            final AtomicReference<String> roleRef = new AtomicReference<>();
            final AtomicReference<XClusterNodeHealth> healthRef = new AtomicReference<>();
            localResult.consume(row -> {
                if (row[0] != null && row[0].length > 0) {
                    leaderRef.setPlain(new String(row[0]));
                }
                final String role;
                if (row[1] != null && row[1].length > 0) {
                    roleRef.setPlain(role = new String(row[1]));
                } else {
                    role = null;
                }
                if (role != null && row[2] != null && row[2].length > 0 && row[3] != null && row[3].length > 0) {
                    try {
                        final long commitIndex = Long.parseLong(new String(row[2]));
                        final long applyIndex = Long.parseLong(new String(row[3]));
                        final long nowNanos = System.nanoTime();
                        final long rttNanos = nowNanos - queryNanos;
                        healthRef.setPlain(new XClusterNodeHealth(
                            address, role, proxyToken, commitIndex, applyIndex, rttNanos, queryNanos + rttNanos / 2));
                    } catch (Throwable t) {
                        LOGGER.warn("Can't parse commit/apply index from local cluster system table.", t);
                    }
                }
            }, limitTimeNs);
            final String leader = leaderRef.getPlain();
            final String role = roleRef.getPlain();
            final XClusterNodeHealth health = healthRef.getPlain();
            if (null == role) {
                throw new IllegalStateException("Can't get role from local cluster system table.");
            }

            // probe xport
            final QueryResultHandler legacyXportResult = connection.sendQuery(LEGACY_XPORT_QUERY);
            final AtomicInteger xportRef = new AtomicInteger(-1);
            try {
                legacyXportResult.consume(row -> {
                    final String useLegacyPort = new String(row[0]);
                    if (useLegacyPort.equalsIgnoreCase("true") || Integer.parseInt(useLegacyPort) != 0) {
                        xportRef.setPlain(Integer.parseInt(new String(row[1])));
                    }
                }, limitTimeNs);
            } catch (NumberFormatException | SQLException ignore) {
                // may throw in 8032 because no variables like 'polarx_xxx'
                xportRef.setPlain(-1);
            }
            if (-1 == xportRef.getPlain()) {
                // try new rpc port
                final QueryResultHandler rpcPortResult = connection.sendQuery(RPC_PORT_QUERY);
                try {
                    rpcPortResult.consume(row -> xportRef.setPlain(Integer.parseInt(new String(row[0]))), limitTimeNs);
                } catch (NumberFormatException | SQLException ignore) {
                    xportRef.setPlain(-1);
                }
            }
            final int xport = xportRef.getPlain();

            // record update time
            final String updateTime = ZonedDateTime.now().format(DATE_FORMAT);

            if (!role.equalsIgnoreCase("Leader")) {
                // no more info and just return
                final List<XClusterNodeBasic> peers;
                if (leader != null) {
                    final InetSocketAddress paxosAddr = AddressDecoder.decode(leader);
                    // guess the leader address
                    final String tag =
                        paxosAddr.getHostString() + ":" + (paxosAddr.getPort() + globalPortGapRef.getPlain());
                    peers = Collections.singletonList(
                        new XClusterNodeBasic(tag, paxosAddr.getHostString(), -1, -1, paxosAddr.getPort(), "Leader",
                            null, version, myClusterId, updateTime));
                } else {
                    peers = null;
                }
                return new XClusterNodeInfo(
                    new XClusterNodeBasic(address, socketAddress.getHostString(), socketAddress.getPort(), xport, -1,
                        role, peers, version, myClusterId, updateTime), health);
            }

            if (null == leader) {
                throw new IllegalStateException(
                    "Can't get leader from local cluster system table while myself id leader.");
            }

            // record cluster id if not initialized
            clusterIdRef.compareAndExchange(-1, myClusterId);

            // calc port gap
            final InetSocketAddress paxosAddr = AddressDecoder.decode(leader);
            final int paxosPort = paxosAddr.getPort();
            final int portGap = serverPort - paxosPort;
            // record port gap once we get one
            globalPortGapRef.setPlain(portGap);

            // get peer nodes
            final QueryResultHandler nodesResult = connection.sendQuery(CLUSTER_GLOBAL_QUERY);
            final List<byte[][]> rows = new ArrayList<>(3);
            nodesResult.consume(rows::add, limitTimeNs);

            // gather all other info
            String leaderRealIp = null;
            final List<XClusterNodeBasic> peers = new ArrayList<>(rows.size());
            for (final byte[][] row : rows) {
                final String peerPaxos = new String(row[0]);
                final InetSocketAddress peerPaxosAddr = AddressDecoder.decode(peerPaxos);
                final String peerHost = peerPaxosAddr.getHostString();
                final int peerPaxosPort = peerPaxosAddr.getPort();
                final int peerPort = peerPaxosPort + portGap;
                final String peerRole = new String(row[1]);
                final boolean peerIsLeader = peerRole.equalsIgnoreCase("Leader");
                peers.add(new XClusterNodeBasic(peerHost + ':' + (peerIsLeader ? serverPort : peerPort), peerHost,
                    peerIsLeader ? serverPort : peerPort, peerIsLeader ? xport : -1, peerPaxosPort, peerRole, null,
                    version, myClusterId, updateTime));
                if (peerIsLeader) {
                    leaderRealIp = peerHost;
                }
            }
            if (null == leaderRealIp) {
                throw new IllegalStateException("Can't get real leader ip from global cluster system table.");
            }

            // sort it to makes compare stable
            peers.sort(Comparator.comparing(XClusterNodeBasic::getTag));

            return new XClusterNodeInfo(
                new XClusterNodeBasic(address, leaderRealIp, serverPort, xport, paxosPort, role,
                    Collections.unmodifiableList(peers), version, myClusterId, updateTime), health);
        } catch (Throwable t) {
            // Ignore which failed to login or access denied(can't login logger)
            final String message = t.getMessage();
            if (null == message || (!message.contains("Auth switch needed.")
                && !message.contains("Access denied for user")
                && !message.contains("is not allowed to connect to this MySQL server")
                // Special case for mysql 8.0.32(more auth packet needed)
                && !message.contains("Unexpected server state when auth more data"))) {
                if (message != null && message.contains("Connection refused")) {
                    // less error log when server is down
                    LOGGER.error("HaManager probe nodes on {} connection refused", address);
                } else {
                    LOGGER.error("HaManager probe nodes on {} error", address, t);
                }
            }
        }
        return null;
    }

    private void reloadPool(SocketAddress socketAddress) {
        final String username = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_USERNAME);
        final String encryptedPassword = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_PASSWORD);
        final int maxPooled = Integer.parseInt(
            ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_ADMIN_MAX_POOLED_SIZE));

        final long startNanos = System.nanoTime();
        final BackendPool newPool = new BackendPool(
            nioWorker, socketAddress, null, username, encryptedPassword, null, maxPooled, false);
        newPool.loadDbConfigs();
        final BackendPool original;
        synchronized (adminPoolRef) {
            original = adminPoolRef.getAndSet(newPool);
            adminPoolRef.notifyAll();
        }
        if (original != null) {
            original.close();
        }
        LOGGER.info("Backend cluster admin pool changed to: {}, cost {} ms", socketAddress.toString(),
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
    }

    private static boolean mayBecomeOrKnowLeader(String role) {
        return role.equalsIgnoreCase("Leader") || role.equalsIgnoreCase("Follower")
            || role.equalsIgnoreCase("Candidate") || role.equalsIgnoreCase("Learner");
    }

    @Override
    public void run() {
        while (true) {
            boolean unknownLeaderExists = false;
            boolean noLeaderExists = false;
            boolean leaderTransferring = false;
            try {
                final List<XClusterNodeBasic> lastNodes = new ArrayList<>(3);
                final Set<String> probeAddresses = getProbeAddresses(lastNodes);
                final Map<String, XClusterNodeBasic> nodes = new HashMap<>(probeAddresses.size());
                final Map<String, XClusterNodeHealth> healths = new HashMap<>(probeAddresses.size());
                CompletableFuture.allOf(probeAddresses.stream().map(address -> CompletableFuture.runAsync(() -> {
                    try {
                        final XClusterNodeInfo info = getNodeInfo(address);
                        if (info != null) {
                            synchronized (nodes) {
                                nodes.put(info.getBasic().getTag(), info.getBasic());
                                healths.put(info.getBasic().getTag(), info.getHealth());
                            }
                        }
                    } catch (Throwable t) {
                        LOGGER.error(t.getMessage(), t);
                    }
                }, executor)).toArray(CompletableFuture[]::new)).join();

                // generate cluster peers from all results
                XClusterNodeBasic probeLeader = null;
                // try fetch leader with real ip first
                for (final XClusterNodeBasic node : nodes.values()) {
                    if (node.getRole().equalsIgnoreCase("Leader") && node.getTag()
                        .equals(node.getHost() + ':' + node.getPort())) {
                        if (null == probeLeader) {
                            probeLeader = node;
                        } else {
                            throw new IllegalStateException("More than one leader found.");
                        }
                    }
                }
                if (null == probeLeader) {
                    // use vip leader
                    for (final XClusterNodeBasic node : nodes.values()) {
                        if (node.getRole().equalsIgnoreCase("Leader")) {
                            probeLeader = node;
                            break;
                        }
                    }
                }
                final XClusterNodeBasic leader = probeLeader;

                // try to add any peers that are not in nodes map
                if (leader != null) {
                    // add leader's peer to nodes map
                    final List<XClusterNodeBasic> peers = leader.getPeers();
                    if (peers != null) {
                        peers.forEach(x -> {
                            if (mayBecomeOrKnowLeader(x.getRole())) {
                                final XClusterNodeBasic existing = nodes.putIfAbsent(x.getTag(), x);
                                // fix paxos port if needed
                                if (existing != null && -1 == existing.getPaxosPort()) {
                                    nodes.put(x.getTag(),
                                        new XClusterNodeBasic(existing.getTag(), existing.getHost(),
                                            existing.getPort(), existing.getXport(), x.getPaxosPort(),
                                            existing.getRole(), existing.getPeers(), existing.getVersion(),
                                            existing.getClusterId(), existing.getUpdateTime()));
                                }
                            }
                        });
                    }
                } else {
                    // try to put follower's leader into probe list
                    // copy one to prevent concurrent modification
                    final List<XClusterNodeBasic> visit = new ArrayList<>(nodes.values());
                    for (final XClusterNodeBasic node : visit) {
                        if (node.getPeers() != null) {
                            unknownLeaderExists = true;
                            node.getPeers().forEach(x -> nodes.putIfAbsent(x.getTag(), x));
                        }
                    }
                    if (!unknownLeaderExists) {
                        // no leader found in this round, just add leader in last round to list
                        XClusterNodeBasic lastLeader = null;
                        for (final XClusterNodeBasic node : lastNodes) {
                            if (node.getRole().equalsIgnoreCase("Leader") ||
                                node.getRole().equalsIgnoreCase("LastLeader")) {
                                lastLeader = node;
                                break;
                            }
                        }
                        // push it into list and rename to candidate
                        if (lastLeader != null) {
                            final List<XClusterNodeBasic> peers = lastLeader.getPeers();
                            if (!nodes.containsKey(lastLeader.getTag())) {
                                nodes.put(lastLeader.getTag(),
                                    new XClusterNodeBasic(lastLeader.getTag(), lastLeader.getHost(),
                                        lastLeader.getPort(),
                                        lastLeader.getXport(), lastLeader.getPaxosPort(), "LastLeader",
                                        peers, lastLeader.getVersion(), lastLeader.getClusterId(),
                                        lastLeader.getUpdateTime()));
                            }
                            // and it's peers
                            if (peers != null) {
                                peers.forEach(x -> {
                                    final String role = x.getRole();
                                    if (mayBecomeOrKnowLeader(role)) {
                                        nodes.putIfAbsent(x.getTag(), new XClusterNodeBasic(
                                            x.getTag(), x.getHost(), x.getPort(), x.getXport(), x.getPaxosPort(),
                                            role.startsWith("Last") ? role : "Last" + role,
                                            x.getPeers(), x.getVersion(), x.getClusterId(), x.getUpdateTime()));
                                    }
                                });
                            }
                        }
                        noLeaderExists = true;
                    }
                }

                // generate new node list
                final List<XClusterNodeBasic> peers =
                    nodes.values().stream().sorted(Comparator.comparing(XClusterNodeBasic::getTag))
                        .collect(Collectors.toUnmodifiableList());
                clusterPeersRef.setRelease(peers);

                // generate cluster health
                final XClusterNodeHealth leaderHealth = healths.values().stream()
                    .filter(x -> leader != null && x.getTag().equals(leader.getTag())).findFirst().orElse(null);
                final List<XClusterNodeHealth> followersHealth = healths.values().stream()
                    .filter(x -> x.getRole().equalsIgnoreCase("Follower") || x.getRole().equalsIgnoreCase("Candidate"))
                    .collect(Collectors.toUnmodifiableList());
                final List<XClusterNodeHealth> learnersHealth = healths.values().stream()
                    .filter(x -> x.getRole().equalsIgnoreCase("Learner")).collect(Collectors.toUnmodifiableList());
                clusterServerlessRef.setRelease(new XClusterServerless(leaderHealth, followersHealth, learnersHealth));

                // compare with dynamic config and update if needed
                final DynamicConfig nowConfig = DynamicConfig.getNowConfig();
                synchronized (nowConfig) {
                    if (!peers.equals(nowConfig.getXCluster())) {
                        nowConfig.setXCluster(peers);
                        LOGGER.info("Backend cluster state changed to: {}", DynamicConfig.GSON.toJson(peers));
                        nowConfig.save();
                    }
                }

                // update RW/RO pool before init admin pool(inst init will busy wait on admin pool ok)
                readWriteSplittingPool.update();

                // record version before init admin pool(inst init will busy wait on admin pool ok)
                if (leader != null) {
                    final String version = leader.getVersion();
                    if (version != null) {
                        int limit = 0;
                        while (limit < version.length()) {
                            final char ch = version.charAt(limit);
                            if ((ch < '0' || ch > '9') && ch != '.') {
                                break;
                            }
                            ++limit;
                        }
                        final String[] split = version.substring(0, limit).split("\\.");
                        if (split.length >= 3) {
                            this.version.setRelease(
                                10000 * Integer.parseInt(split[0]) + 100 * Integer.parseInt(split[1])
                                    + Integer.parseInt(split[2]));
                        }
                    }
                }

                // update pool if leader changed
                final BackendPool existing = adminPoolRef.getAcquire();
                if (leader != null && (null == existing || !AddressDecoder.decode(leader.getTag())
                    .equals(existing.getAddress()))) {
                    // HA needed
                    reloadPool(AddressDecoder.decode(leader.getTag()));
                }

                // check leader transferring
                final LeaderTransferInfo transferInfo = leaderTransferInfoRef.getAcquire();
                if (transferInfo != null) {
                    final long nowNanos = System.nanoTime();
                    if (nowNanos - transferInfo.timeoutNanos > 0) {
                        // clear which is timeout
                        leaderTransferInfoRef.compareAndSet(transferInfo, null);
                    } else if (leader != null) {
                        if (AddressDecoder.decode(leader.getTag()).equals(transferInfo.address)) {
                            leaderTransferring = true;
                            LOGGER.debug("Fast HA check for leader transferring.");
                        } else {
                            // clear it because we already switched
                            leaderTransferInfoRef.compareAndSet(transferInfo, null);
                        }
                    }
                }

                // after leader successfully transferred, invoke tasks
                if (!leaderTransferring && !leaderTransferredTask.isEmpty()) {
                    final ScheduledThreadPoolExecutor exec = ProxyExecutor.getInstance().getExecutor();
                    Runnable task;
                    while ((task = leaderTransferredTask.poll()) != null) {
                        exec.execute(task);
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("HaManager error", t);
            }

            // sleep and recheck
            try {
                final int interval =
                    Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_HA_CHECK_INTERVAL));
                final int sleepMs = leaderTransferring ? 100 : (unknownLeaderExists ? Math.min(500, interval) :
                    (noLeaderExists ? Math.min(1000, interval) : interval));
                synchronized (clusterServerlessRef) {
                    clusterServerlessRef.wait(sleepMs);
                }
            } catch (Throwable t) {
                LOGGER.error("HaManager sleep error", t);
            }
        }
    }

    // package private
    AtomicReference<XClusterServerless> getClusterServerlessRef() {
        return clusterServerlessRef;
    }

    // package private
    BackendPool getAdminPool() {
        return adminPoolRef.getAcquire();
    }

    // package private
    void markLeaderTransferring(SocketAddress targetLeader) {
        final LeaderTransferInfo expected = leaderTransferInfoRef.getAcquire();
        if (null == expected || !expected.address.equals(targetLeader)) {
            leaderTransferInfoRef.compareAndSet(expected, new LeaderTransferInfo(targetLeader,
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(FastConfig.smoothSwitchoverWaitTimeout)));
            refresh();
        }
    }

    public void refresh() {
        LOGGER.info("Notify refreshing HA info.");
        synchronized (clusterServerlessRef) {
            clusterServerlessRef.notifyAll();
        }
    }

    public boolean isLeaderTransferring() {
        final LeaderTransferInfo info = leaderTransferInfoRef.getAcquire();
        final BackendPool admin = adminPoolRef.getAcquire();
        return info != null && admin != null && admin.getAddress().equals(info.address);
    }

    public List<XClusterNodeBasic> getClusterPeers() {
        return clusterPeersRef.getAcquire();
    }

    public XClusterServerless getClusterServerless() {
        return clusterServerlessRef.getAcquire();
    }

    public BackendConnectionWrapper getAdminConnection() {
        final BackendPool pool = adminPoolRef.getAcquire();
        if (null == pool) {
            throw new IllegalStateException("Backend admin pool is not initialized.");
        }
        try {
            return pool.getConnection();
        } catch (Exception e) {
            LOGGER.error("Failed to get admin connection.", e);
            throw new RuntimeException(e);
        }
    }

    public int getVersion() {
        return version.getAcquire();
    }

    private static HaManager INSTANCE;

    public static void init(NIOWorker nioWorker, ThreadPoolExecutor executor) throws InterruptedException {
        if (null == INSTANCE) {
            final boolean first;
            synchronized (HaManager.class) {
                if (null == INSTANCE) {
                    INSTANCE = new HaManager(nioWorker, executor);
                    first = true;
                } else {
                    first = false;
                }
            }

            if (first) {
                // wait admin pool ready
                LOGGER.info("HA manager admin pool initializing...");
                synchronized (INSTANCE.adminPoolRef) {
                    while (null == INSTANCE.adminPoolRef.getAcquire()) {
                        INSTANCE.adminPoolRef.wait();
                    }
                }
                LOGGER.info("HA manager admin pool initialized.");

                // when ready, start background refresher
                PoolRefresher.startBackendPoolRefresh();
            }
        }
    }

    public static HaManager getInstance() {
        if (null == INSTANCE) {
            throw new IllegalStateException("HaManager is not initialized.");
        }
        return INSTANCE;
    }
}
