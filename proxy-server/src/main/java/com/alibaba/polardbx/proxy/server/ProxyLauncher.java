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

package com.alibaba.polardbx.proxy.server;

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.ProxyServer;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.utils.Kill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyLauncher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyLauncher.class);

    public static void main(String[] args) {
        LOGGER.info("## start the proxy server");

        try {
            ConfigLoader.loadConfig();
            FastConfig.refresh();
            ProxyExecutor.init();
            ProxyServer.init();
        } catch (Exception e) {
            LOGGER.error("Failed to start proxy server.", e);
            Kill.kill9();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOGGER.info("## stop the proxy server with kill9");
                Kill.kill9();
            } catch (Throwable e) {
                LOGGER.warn("## something goes wrong when stopping proxy server", e);
            } finally {
                LOGGER.info("## proxy server is down");
            }
        }));
    }
}
