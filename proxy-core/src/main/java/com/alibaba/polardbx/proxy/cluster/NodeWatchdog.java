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

package com.alibaba.polardbx.proxy.cluster;

import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.protocol.common.MysqlError;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import com.alibaba.polardbx.proxy.utils.AddressUtils;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.ref.WeakReference;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class NodeWatchdog {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeWatchdog.class);

    private static final String PROXY_LEASE_TABLE_WRAPPED = "`mysql`.`proxy_lease`";
    private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS " + PROXY_LEASE_TABLE_WRAPPED + " (\n"
        + "  `id` bigint NOT NULL AUTO_INCREMENT,\n"
        + "  `owner` varchar(255) NOT NULL,\n"
        + "  `lease` bigint unsigned NOT NULL COMMENT 'the expiration time of this lease in UTC',\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  UNIQUE KEY `owner` (`owner`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 AUTO_INCREMENT=2";

    // leadership
    private static final String INIT_LEADER = "insert into " + PROXY_LEASE_TABLE_WRAPPED
        + " (`id`, `owner`, `lease`) values (1, '%s', %d)";
    private static final String ELECT_LEADER = "update " + PROXY_LEASE_TABLE_WRAPPED
        + " set `owner` = '%s', `lease` = %d"
        + " where `id` = 1 and (`lease` + 500 < %d or `owner` = '%s')";
    private static final String RENEW_LEADER = "update " + PROXY_LEASE_TABLE_WRAPPED
        + " set `lease` = %d"
        + " where `id` = 1 and `owner` = '%s' and `lease` < %d";

    // nodes manager
    private static final String UPDATE_NODE_TEMPLATE = "update " + PROXY_LEASE_TABLE_WRAPPED
        + " set `lease` = %d"
        + " where `id` != 1 and `owner` = '%s'";
    private static final String REPLACE_NODE_TEMPLATE = "replace into " + PROXY_LEASE_TABLE_WRAPPED
        + " (`lease`, `owner`) values (%d, '%s')";
    private static final String CLEANUP_TEMPLATE = "delete from " + PROXY_LEASE_TABLE_WRAPPED
        + " where `id` != 1 and `lease` < %d";
    private static final String GET_NODES_TEMPLATE = "select `id`, `owner`, `lease` from " + PROXY_LEASE_TABLE_WRAPPED
        + " where `id` != 1 and `lease` >= %d order by `id`";

    private final String leaderTag;
    private final String rpcTag;

    // leadership
    private long lastRefreshLeaderTimeUTC = 0;
    private final AtomicBoolean myselfLeader = new AtomicBoolean(false);
    private final AtomicLong expireTime = new AtomicLong(0);
    private final List<WeakReference<Consumer<Boolean>>> leadershipListener = new CopyOnWriteArrayList<>();

    // nodes info
    private final AtomicReference<String[]> nodes = new AtomicReference<>(null);

    @Setter
    private boolean mockLeader = false;

    public NodeWatchdog() {
        final String ip = Optional
            .ofNullable(ConfigLoader.PROPERTIES.getProperty(ConfigProps.NODE_IP))
            .filter(s -> !s.isBlank())
            .orElseGet(AddressUtils::getHostIp);
        final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        final String processName = runtimeMXBean.getName();
        final long pid = Long.parseLong(processName.split("@")[0]);
        final long startTime = runtimeMXBean.getStartTime();

        this.leaderTag = ip + '@' + pid + '@' + startTime;
        this.rpcTag =
            ip + ':' + Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.GENERAL_SERVICE_PORT));

        // new threads and start
        final Thread leaderRun = new Thread(this::leaderWatchdog, ThreadNames.LEADER_WATCHDOG);
        leaderRun.setDaemon(true);
        leaderRun.start();
        final Thread nodeRun = new Thread(this::nodeWatchdog, ThreadNames.NODE_WATCHDOG);
        nodeRun.setDaemon(true);
        nodeRun.start();
    }

    private void nodeWatchdog() {
        while (true) {
            long sleepMillis;

            try {
                // refresh latest lease
                final long leaseMillis = Long.parseLong(ConfigLoader.PROPERTIES.getProperty(ConfigProps.NODE_LEASE));
                final int timeout =
                    Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.UPDATE_LEASE_TIMEOUT));
                final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);

                // register myself and get all nodes
                try (final BackendConnectionWrapper connection = HaManager.getInstance().getAdminConnection()) {
                    final long nowUTC = System.currentTimeMillis();
                    final long leaseUTC = nowUTC + leaseMillis;

                    while (true) {
                        try {
                            final QueryResultHandler updateResult =
                                connection.sendQuery(String.format(UPDATE_NODE_TEMPLATE, leaseUTC, rpcTag));
                            final long updateCount = updateResult.update(limitTimeNs);
                            if (0 == updateCount) {
                                // try insert myself
                                final QueryResultHandler insertResult =
                                    connection.sendQuery(String.format(REPLACE_NODE_TEMPLATE, leaseUTC, rpcTag));
                                insertResult.update(limitTimeNs); // ignore result, always success or throw
                            }

                            // clear old nodes
                            final QueryResultHandler cleanupResult =
                                connection.sendQuery(String.format(CLEANUP_TEMPLATE, nowUTC));
                            cleanupResult.update(limitTimeNs); // ignore result

                            // get all nodes(order by id to makes existing one stable order)
                            final QueryResultHandler getNodesResult =
                                connection.sendQuery(String.format(GET_NODES_TEMPLATE, nowUTC));
                            final List<String> newNodes = new ArrayList<>();
                            getNodesResult.consume(row -> newNodes.add(new String(row[1])), limitTimeNs);
                            final String[] array = newNodes.toArray(new String[0]);

                            synchronized (this.nodes) {
                                this.nodes.setRelease(array);
                                this.nodes.notifyAll();
                            }
                        } catch (SQLException e) {
                            // deal table not exist
                            if (MysqlError.ER_NO_SUCH_TABLE == e.getErrorCode()) {
                                final QueryResultHandler createResult = connection.sendQuery(CREATE_TABLE_SQL);
                                createResult.update(limitTimeNs); // ignore result
                                continue; // retry update
                            }
                            throw e; // unknown error
                        }
                        break;
                    }
                }

                sleepMillis = leaseMillis / 2;
            } catch (Throwable t) {
                LOGGER.error("NodeWatchdog nodes update lease error", t);
                // set sleep to 1s when error occurs
                sleepMillis = 1000;
            }

            try {
                if (sleepMillis > 0) {
                    // check lease while sleep
                    final long startSleepMillis = System.currentTimeMillis();
                    while (System.currentTimeMillis() - startSleepMillis + 100 < sleepMillis) {
                        Thread.sleep(100); // sleep 100ms
                        final long leaseMillis =
                            Long.parseLong(ConfigLoader.PROPERTIES.getProperty(ConfigProps.NODE_LEASE));
                        if (sleepMillis > leaseMillis / 2) {
                            sleepMillis = leaseMillis / 2; // dealing whit lease changed
                        }
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("NodeWatchdog nodes sleep error", t);
                try {
                    // prevent infinite loop and log when bug occurs
                    Thread.sleep(1000);
                } catch (Throwable ignore) {
                }
            }
        }
    }

    private void logLeaseTime(long lastTime_ms, long startTime_ms, long connTime_ms, boolean renew, boolean hold) {
        if (LOGGER.isDebugEnabled()) {
            final long now = System.currentTimeMillis();
            LOGGER.debug(MessageFormat.format(
                "[lease] register after {0,number,#.###} s, conn cost {1,number,#.###} s, exec cost {2,number,#.###} s, {3} and {4}",
                (startTime_ms - lastTime_ms) / 1e3f, (connTime_ms - startTime_ms) / 1e3f, (now - connTime_ms) / 1e3f,
                renew ? "renew" : "elect", hold ? "success" : "fail"));
        }
    }

    private void refreshLeadership(final long nowUTC, final long leaseUTC) {
        // refresh lease
        expireTime.setRelease(leaseUTC);

        // notify if needed
        final boolean before = myselfLeader.compareAndExchange(false, true);
        if (!before) {
            LOGGER.info("Gain leadership. owner: {} nowUTC: {}", leaderTag, nowUTC);

            // invoke callback
            for (WeakReference<Consumer<Boolean>> ref : leadershipListener) {
                final Consumer<Boolean> consumer = ref.get();
                if (consumer != null) {
                    consumer.accept(true);
                }
            }
            // clean
            leadershipListener.removeIf(ref -> null == ref.get());
        }
    }

    private void lostLeadership(final long nowUTC) {
        // notify if needed
        final boolean before = myselfLeader.compareAndExchange(true, false);
        if (before) {
            LOGGER.info("Lost leadership. owner: {} nowUTC: {}", leaderTag, nowUTC);

            // invoke callback
            for (WeakReference<Consumer<Boolean>> ref : leadershipListener) {
                final Consumer<Boolean> consumer = ref.get();
                if (consumer != null) {
                    consumer.accept(false);
                }
            }
            // clean
            leadershipListener.removeIf(ref -> null == ref.get());
        }
    }

    private void leaderWatchdog() {
        while (true) {
            long sleepMillis;

            try {
                // refresh latest lease
                final long leaseMillis = Long.parseLong(ConfigLoader.PROPERTIES.getProperty(ConfigProps.NODE_LEASE));
                final int timeout =
                    Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.UPDATE_LEASE_TIMEOUT));
                final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);

                // do leader election
                final long startTimeUTC = System.currentTimeMillis();
                try (final BackendConnectionWrapper connection = HaManager.getInstance().getAdminConnection()) {
                    final long nowUTC = System.currentTimeMillis();
                    final long leaseUTC = nowUTC + leaseMillis;

                    while (true) {
                        try {
                            // try refresh leadership first
                            if (myselfLeader.getAcquire()) {
                                // try to renew if we own the leadership
                                final boolean success;
                                if (mockLeader) {
                                    success = true;
                                } else {
                                    final QueryResultHandler renewResult = connection.sendQuery(
                                        String.format(RENEW_LEADER, leaseUTC, leaderTag, leaseUTC));
                                    success = 1 == renewResult.update(limitTimeNs);
                                }

                                if (success) {
                                    // renew success
                                    logLeaseTime(lastRefreshLeaderTimeUTC, startTimeUTC, nowUTC, true, true);
                                    refreshLeadership(nowUTC, leaseUTC);
                                    // sleep half lease
                                    sleepMillis = leaseMillis / 2;
                                } else {
                                    // renew failed
                                    logLeaseTime(lastRefreshLeaderTimeUTC, startTimeUTC, nowUTC, true, false);
                                    lostLeadership(nowUTC);
                                    // fast retry
                                    sleepMillis = 0;
                                }
                            } else {
                                // or do elect
                                boolean success;
                                if (mockLeader) {
                                    success = true;
                                } else {
                                    try {
                                        final QueryResultHandler initResult = connection.sendQuery(
                                            String.format(INIT_LEADER, leaderTag, leaseUTC));
                                        success = 1 == initResult.update(limitTimeNs);
                                    } catch (SQLException e) {
                                        if (e.getErrorCode() != MysqlError.ER_DUP_ENTRY) {
                                            throw e;
                                        }
                                        success = false;
                                    }
                                    if (!success) {
                                        final QueryResultHandler electResult = connection.sendQuery(
                                            String.format(ELECT_LEADER, leaderTag, leaseUTC, nowUTC, leaderTag));
                                        success = 1 == electResult.update(limitTimeNs);
                                    }
                                }

                                if (success) {
                                    logLeaseTime(lastRefreshLeaderTimeUTC, startTimeUTC, nowUTC, false, true);
                                    refreshLeadership(nowUTC, leaseUTC);
                                    // sleep half lease
                                    sleepMillis = leaseMillis / 2;
                                } else {
                                    logLeaseTime(lastRefreshLeaderTimeUTC, startTimeUTC, nowUTC, false, false);
                                    // sleep 1s and retry
                                    sleepMillis = 1000;
                                }
                            }
                        } catch (SQLException e) {
                            // deal table not exist
                            if (MysqlError.ER_NO_SUCH_TABLE == e.getErrorCode()) {
                                final QueryResultHandler createResult = connection.sendQuery(CREATE_TABLE_SQL);
                                createResult.update(limitTimeNs); // ignore result
                                continue; // retry update
                            }
                            throw e; // unknown error
                        }
                        break;
                    }

                    // update last time to now
                    lastRefreshLeaderTimeUTC = nowUTC;
                }
            } catch (Throwable t) {
                LOGGER.error("NodeWatchdog leader update lease error", t);
                // set sleep to 1s when error occurs
                sleepMillis = 1000;
            }

            try {
                if (sleepMillis > 0) {
                    // check lease while sleep
                    while (System.currentTimeMillis() - lastRefreshLeaderTimeUTC + 100 < sleepMillis) {
                        Thread.sleep(100); // sleep 100ms
                        final long leaseMillis =
                            Long.parseLong(ConfigLoader.PROPERTIES.getProperty(ConfigProps.NODE_LEASE));
                        if (sleepMillis > leaseMillis / 2) {
                            sleepMillis = leaseMillis / 2; // dealing whit lease changed
                        }
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("NodeWatchdog leader sleep error", t);
                try {
                    // prevent infinite loop and log when bug occurs
                    Thread.sleep(1000);
                } catch (Throwable ignore) {
                }
            }
        }
    }

    public String[] getNodes() {
        return nodes.getAcquire();
    }

    public boolean isLeader() {
        if (this.myselfLeader.getAcquire()) {
            // lease recheck
            final long nowUTC = System.currentTimeMillis();
            if (nowUTC <= expireTime.getAcquire()) {
                return true;
            }
            // expired
            lostLeadership(nowUTC);
        }
        return false;
    }

    public void registerLeadershipListener(final Consumer<Boolean> consumer) {
        leadershipListener.add(new WeakReference<>(consumer));
        // clean
        leadershipListener.removeIf(ref -> null == ref.get());
    }

    private static NodeWatchdog INSTANCE;

    public static void init() throws InterruptedException {
        if (null == INSTANCE) {
            final boolean first;
            synchronized (NodeWatchdog.class) {
                if (null == INSTANCE) {
                    INSTANCE = new NodeWatchdog();
                    first = true;
                } else {
                    first = false;
                }
            }

            if (first) {
                // wait node watchdog ready
                LOGGER.info("Node watchdog registering...");
                synchronized (INSTANCE.nodes) {
                    while (null == INSTANCE.nodes.getAcquire()) {
                        INSTANCE.nodes.wait();
                    }
                }
                LOGGER.info("Node watchdog registered.");
            }
        }
    }

    public static NodeWatchdog getInstance() {
        return INSTANCE;
    }
}
