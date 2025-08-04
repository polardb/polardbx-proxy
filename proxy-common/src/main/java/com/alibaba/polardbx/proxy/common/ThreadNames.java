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

package com.alibaba.polardbx.proxy.common;

public class ThreadNames {
    public static final String NIO_ACCEPTOR = "NIO-Acceptor";
    public static final String NIO_PROCESSOR = "NIO-Processor";

    public static final String PROXY_EXECUTOR = "Proxy-Executor";
    public static final String PROXY_TIMER = "Proxy-Timer";

    public static final String HA_MANAGER = "HA-Manager";
    public static final String HA_EXECUTOR = "HA-Executor";
    public static final String LATENCY_CHECKER = "LatencyChecker";

    public static final String BACKEND_POOL_REFRESHER_EXECUTOR = "BackendPoolRefresher-Executor";
    public static final String PRIVILEGE_REFRESHER = "PrivilegeRefresher";

    public static final String LEADER_WATCHDOG = "LeaderWatchdog";
    public static final String NODE_WATCHDOG = "NodeWatchdog";

    public static final String SMOOTH_SWITCHOVER_MONITOR = "SmoothSwitchoverMonitor";

    // for test
    public static final String TEST_POOL = "TestThreadPool";

    public static boolean isThread(final String name) {
        return Thread.currentThread().getName().startsWith(name);
    }
}
