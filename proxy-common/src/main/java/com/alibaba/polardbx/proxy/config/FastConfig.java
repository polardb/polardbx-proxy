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

public class FastConfig {
    public static volatile boolean enableConnectionHold;
    public static volatile int queryRetransmitTimeout;
    public static volatile int queryRetransmitFastRetries;
    public static volatile int queryRetransmitFastRetryDelay;
    public static volatile int queryRetransmitSlowRetryDelay;
    public static volatile int fetchLsnRetryTimes;
    public static volatile int fetchLsnTimeout;
    public static volatile boolean enableStaleRead;
    public static volatile boolean tcpEnsureMinimumBuffer;
    public static volatile int logSqlMaxLength;
    public static volatile int logSqlParamMaxLength;
    public static volatile int maxAllowedPacket;
    public static volatile boolean enableSmoothSwitchover;
    public static volatile int smoothSwitchoverCheckInterval;
    public static volatile int smoothSwitchoverWaitTimeout;
    public static volatile boolean enableSqlLog;
    public static volatile boolean enableLeakCheck;

    static {
        // refresh to default
        refresh();
    }

    public static void refresh() {
        enableConnectionHold =
            Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.ENABLE_CONNECTION_HOLD));
        queryRetransmitTimeout =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.QUERY_RETRANSMIT_TIMEOUT));
        queryRetransmitFastRetries =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.QUERY_RETRANSMIT_FAST_RETRIES));
        queryRetransmitFastRetryDelay =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.QUERY_RETRANSMIT_FAST_RETRY_DELAY));
        queryRetransmitSlowRetryDelay =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.QUERY_RETRANSMIT_SLOW_RETRY_DELAY));
        fetchLsnRetryTimes = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.FETCH_LSN_RETRY_TIMES));
        fetchLsnTimeout = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.FETCH_LSN_TIMEOUT));
        enableStaleRead = Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.ENABLE_STALE_READ));
        tcpEnsureMinimumBuffer =
            Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.TCP_ENSURE_MINIMUM_BUFFER));
        logSqlMaxLength = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.LOG_SQL_MAX_LENGTH));
        logSqlParamMaxLength =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.LOG_SQL_PARAM_MAX_LENGTH));
        maxAllowedPacket = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.MAX_ALLOWED_PACKET));
        enableSmoothSwitchover =
            Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.SMOOTH_SWITCHOVER_ENABLED));
        smoothSwitchoverCheckInterval =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.SMOOTH_SWITCHOVER_CHECK_INTERVAL));
        smoothSwitchoverWaitTimeout =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.SMOOTH_SWITCHOVER_WAIT_TIMEOUT));
        enableSqlLog = Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.ENABLE_SQL_LOG));
        enableLeakCheck = Boolean.parseBoolean(ConfigLoader.PROPERTIES.getProperty(ConfigProps.ENABLE_LEAK_CHECK));
    }
}
