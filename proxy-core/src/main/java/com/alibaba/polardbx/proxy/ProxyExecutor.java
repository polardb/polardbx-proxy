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

package com.alibaba.polardbx.proxy;

import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.utils.NamedThreadFactory;
import lombok.Getter;

import java.util.concurrent.ScheduledThreadPoolExecutor;

@Getter
public class ProxyExecutor {
    private final ScheduledThreadPoolExecutor executor;
    private final ScheduledThreadPoolExecutor timer;

    public ProxyExecutor() {
        final int workers = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.WORKER_THREADS));
        this.executor = new ScheduledThreadPoolExecutor(workers, new NamedThreadFactory(ThreadNames.PROXY_EXECUTOR));
        final int timers = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.TIMER_THREADS));
        this.timer = new ScheduledThreadPoolExecutor(timers, new NamedThreadFactory(ThreadNames.PROXY_TIMER));
    }

    private static ProxyExecutor INSTANCE = null;

    public static ProxyExecutor getInstance() {
        final ProxyExecutor inst = INSTANCE;
        if (null == inst) {
            throw new IllegalStateException("ProxyExecutor is not initialized.");
        }
        return inst;
    }

    public synchronized static void init() {
        if (null == INSTANCE) {
            INSTANCE = new ProxyExecutor();
        }
    }
}
