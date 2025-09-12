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

package com.alibaba.polardbx.proxy.config;

import java.util.Properties;

public class ConfigProps {
    // base info
    public static final String WORKER_THREADS = "worker_threads";
    public static final String TIMER_THREADS = "timer_threads";
    public static final String CLUSTER_NODE_ID = "cluster_node_id";

    // threads for reactor framework
    public static final String CPUS = "cpus";
    public static final String REACTOR_FACTOR = "reactor_factor";

    // setting for TCP
    public static final String TCP_ENSURE_MINIMUM_BUFFER = "tcp_ensure_minimum_buffer";

    // port for service
    public static final String FRONTEND_PORT = "frontend_port";

    // backend info
    public static final String BACKEND_ADDRESS = "backend_address";
    public static final String BACKEND_USERNAME = "backend_username";
    public static final String BACKEND_PASSWORD = "backend_password";
    public static final String BACKEND_CONNECT_TIMEOUT = "backend_connect_timeout";

    // backend pool
    public static final String BACKEND_ADMIN_MAX_POOLED_SIZE = "backend_admin_max_pooled_size";
    public static final String BACKEND_RW_MAX_POOLED_SIZE = "backend_rw_max_pooled_size";
    public static final String BACKEND_RO_MAX_POOLED_SIZE = "backend_ro_max_pooled_size";

    // HA
    public static final String BACKEND_HA_WORKER_THREADS = "backend_ha_worker_threads";
    public static final String BACKEND_HA_CHECK_INTERVAL = "backend_ha_check_interval";
    public static final String BACKEND_HA_CHECK_TIMEOUT = "backend_ha_check_timeout";

    // dynamic config
    public static final String DYNAMIC_CONFIG_FILE = "dynamic_config_file";

    // frontend connection keep and error handle
    public static final String ENABLE_CONNECTION_HOLD = "enable_connection_hold";
    public static final String QUERY_RETRANSMIT_TIMEOUT = "query_retransmit_timeout";
    public static final String QUERY_RETRANSMIT_FAST_RETRIES = "query_retransmit_fast_retries";
    public static final String QUERY_RETRANSMIT_FAST_RETRY_DELAY = "query_retransmit_fast_retry_delay";
    public static final String QUERY_RETRANSMIT_SLOW_RETRY_DELAY = "query_retransmit_slow_retry_delay";

    // Read-write splitting
    public static final String ENABLE_READ_WRITE_SPLITTING = "enable_read_write_splitting";
    public static final String ENABLE_FOLLOWER_READ = "enable_follower_read";
    public static final String ENABLE_LEADER_IN_RO_POOLS = "enable_leader_in_ro_pools";
    public static final String READ_WEIGHTS = "read_weights";
    public static final String LATENCY_CHECK_TIMEOUT = "latency_check_timeout";
    public static final String LATENCY_CHECK_INTERVAL = "latency_check_interval";
    public static final String LATENCY_RECORD_COUNT = "latency_record_count";
    public static final String SLAVE_READ_LATENCY_THRESHOLD = "slave_read_latency_threshold";
    public static final String FETCH_LSN_TIMEOUT = "fetch_lsn_timeout";
    public static final String FETCH_LSN_RETRY_TIMES = "fetch_lsn_retry_times";
    public static final String ENABLE_STALE_READ = "enable_stale_read";

    // backend pool refresh
    public static final String BACKEND_POOL_REFRESH_THREADS = "backend_pool_refresh_threads";
    public static final String BACKEND_POOL_REFRESH_TASK_INTERVAL = "backend_pool_refresh_task_interval";
    public static final String BACKEND_POOL_REFRESH_INTERVAL = "backend_pool_refresh_interval";
    public static final String BACKEND_POOL_REFRESH_SQL = "backend_pool_refresh_sql";
    public static final String BACKEND_POOL_REFRESH_TIMEOUT = "backend_pool_refresh_timeout";

    // privilege
    public static final String PRIVILEGE_REFRESH_TIMEOUT = "privilege_refresh_timeout";
    public static final String PRIVILEGE_REFRESH_INTERVAL = "privilege_refresh_interval";

    // prepared statement cache
    public static final String PREPARED_STATEMENT_CACHE_SIZE = "prepared_statement_cache_size";

    // log size
    public static final String LOG_SQL_MAX_LENGTH = "log_sql_max_length";
    public static final String LOG_SQL_PARAM_MAX_LENGTH = "log_sql_param_max_length";

    // global variables refresh interval
    public static final String GLOBAL_VARIABLES_REFRESH_INTERVAL = "global_variables_refresh_interval";

    // general service and lease
    public static final String NODE_IP = "node_ip";
    public static final String GENERAL_SERVICE_PORT = "general_service_port";
    public static final String GENERAL_SERVICE_TIMEOUT = "general_service_timeout";
    public static final String NODE_LEASE = "node_lease";
    public static final String UPDATE_LEASE_TIMEOUT = "update_lease_timeout";

    // max_allowed_packet
    public static final String MAX_ALLOWED_PACKET = "max_allowed_packet";

    // smooth switchover checker
    public static final String SMOOTH_SWITCHOVER_ENABLED = "smooth_switchover_enabled";
    public static final String SMOOTH_SWITCHOVER_CHECK_INTERVAL = "smooth_switchover_check_interval";
    public static final String SMOOTH_SWITCHOVER_WAIT_TIMEOUT = "smooth_switchover_wait_timeout";

    // dnPasswordKey
    public static final String DN_PASSWORD_KEY = "dn_password_key";

    // log
    public static final String ENABLE_SQL_LOG = "enable_sql_log";

    // extreme performance
    public static final String ENABLE_LEAK_CHECK = "enable_leak_check";

    /**
     * Default properties.
     */

    public static final Properties DEFAULT_PROPS = new Properties();

    static {
        DEFAULT_PROPS.setProperty(WORKER_THREADS, "4");
        DEFAULT_PROPS.setProperty(TIMER_THREADS, "1");
        DEFAULT_PROPS.setProperty(CLUSTER_NODE_ID, "0");

        DEFAULT_PROPS.setProperty(CPUS, "0"); // auto check
        DEFAULT_PROPS.setProperty(REACTOR_FACTOR, "1");

        DEFAULT_PROPS.setProperty(TCP_ENSURE_MINIMUM_BUFFER, "false");

        DEFAULT_PROPS.setProperty(FRONTEND_PORT, "3307");

        DEFAULT_PROPS.setProperty(BACKEND_ADDRESS, "127.0.0.1:3306");
        DEFAULT_PROPS.setProperty(BACKEND_USERNAME, "root");
        DEFAULT_PROPS.setProperty(BACKEND_PASSWORD, "123456");
        DEFAULT_PROPS.setProperty(BACKEND_CONNECT_TIMEOUT, "3000");

        DEFAULT_PROPS.setProperty(BACKEND_ADMIN_MAX_POOLED_SIZE, "2");
        DEFAULT_PROPS.setProperty(BACKEND_RW_MAX_POOLED_SIZE, "600");
        DEFAULT_PROPS.setProperty(BACKEND_RO_MAX_POOLED_SIZE, "600");

        DEFAULT_PROPS.setProperty(BACKEND_HA_WORKER_THREADS, "8"); // (vip + 3 nodes) * 2(now and last)
        DEFAULT_PROPS.setProperty(BACKEND_HA_CHECK_INTERVAL, "5000");
        DEFAULT_PROPS.setProperty(BACKEND_HA_CHECK_TIMEOUT, "3000");

        DEFAULT_PROPS.setProperty(DYNAMIC_CONFIG_FILE, "dynamic.json");

        DEFAULT_PROPS.getProperty(ENABLE_CONNECTION_HOLD, "false"); // default trx level connection pool
        DEFAULT_PROPS.setProperty(QUERY_RETRANSMIT_TIMEOUT, "20000"); // 20s for retry
        DEFAULT_PROPS.setProperty(QUERY_RETRANSMIT_FAST_RETRIES, "10");
        DEFAULT_PROPS.setProperty(QUERY_RETRANSMIT_FAST_RETRY_DELAY, "100");
        DEFAULT_PROPS.setProperty(QUERY_RETRANSMIT_SLOW_RETRY_DELAY, "1000");

        DEFAULT_PROPS.setProperty(ENABLE_READ_WRITE_SPLITTING, "true");
        DEFAULT_PROPS.setProperty(ENABLE_FOLLOWER_READ, "true");
        DEFAULT_PROPS.setProperty(ENABLE_LEADER_IN_RO_POOLS, "true");
        DEFAULT_PROPS.setProperty(READ_WEIGHTS, ""); // ip:port@1, ip:port@1, ... default all 1
        DEFAULT_PROPS.setProperty(LATENCY_CHECK_TIMEOUT, "3000");
        DEFAULT_PROPS.setProperty(LATENCY_CHECK_INTERVAL, "1000");
        DEFAULT_PROPS.setProperty(LATENCY_RECORD_COUNT, "100");
        DEFAULT_PROPS.setProperty(SLAVE_READ_LATENCY_THRESHOLD, "3000");
        DEFAULT_PROPS.setProperty(FETCH_LSN_TIMEOUT, "1000");
        DEFAULT_PROPS.setProperty(FETCH_LSN_RETRY_TIMES, "3");
        DEFAULT_PROPS.setProperty(ENABLE_STALE_READ, "false");

        DEFAULT_PROPS.setProperty(BACKEND_POOL_REFRESH_THREADS, "4");
        DEFAULT_PROPS.setProperty(BACKEND_POOL_REFRESH_TASK_INTERVAL, "1000");
        DEFAULT_PROPS.setProperty(BACKEND_POOL_REFRESH_INTERVAL, "49000");
        DEFAULT_PROPS.setProperty(BACKEND_POOL_REFRESH_SQL, "/* PolarDB-X-Proxy Refresh */ select 1");
        DEFAULT_PROPS.setProperty(BACKEND_POOL_REFRESH_TIMEOUT, "3000");

        DEFAULT_PROPS.setProperty(PRIVILEGE_REFRESH_TIMEOUT, "10000");
        DEFAULT_PROPS.setProperty(PRIVILEGE_REFRESH_INTERVAL, "10000"); // per 10s

        DEFAULT_PROPS.setProperty(PREPARED_STATEMENT_CACHE_SIZE, "100");

        DEFAULT_PROPS.setProperty(LOG_SQL_MAX_LENGTH, "4096");
        DEFAULT_PROPS.setProperty(LOG_SQL_PARAM_MAX_LENGTH, "4096");

        DEFAULT_PROPS.setProperty(GLOBAL_VARIABLES_REFRESH_INTERVAL, "60000");

        DEFAULT_PROPS.setProperty(NODE_IP, ""); // empty means get IP from OS
        DEFAULT_PROPS.setProperty(GENERAL_SERVICE_PORT, "8083");
        DEFAULT_PROPS.setProperty(GENERAL_SERVICE_TIMEOUT, "3000");
        DEFAULT_PROPS.setProperty(NODE_LEASE, "10000"); // 10s
        DEFAULT_PROPS.setProperty(UPDATE_LEASE_TIMEOUT, "3000");

        DEFAULT_PROPS.setProperty(MAX_ALLOWED_PACKET, "1073741824"); // 1GB

        DEFAULT_PROPS.setProperty(SMOOTH_SWITCHOVER_ENABLED, "true");
        DEFAULT_PROPS.setProperty(SMOOTH_SWITCHOVER_CHECK_INTERVAL, "100");
        DEFAULT_PROPS.setProperty(SMOOTH_SWITCHOVER_WAIT_TIMEOUT, "10000");

        DEFAULT_PROPS.setProperty(DN_PASSWORD_KEY, "");

        DEFAULT_PROPS.setProperty(ENABLE_SQL_LOG, "true");

        DEFAULT_PROPS.setProperty(ENABLE_LEAK_CHECK, "false"); // no leak check for extreme performance
    }
}
