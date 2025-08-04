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

package com.alibaba.polardbx.proxy.sync;

import com.alibaba.polardbx.proxy.GeneralService;
import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.cluster.NodeWatchdog;
import com.alibaba.polardbx.proxy.common.AddressDecoder;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class SyncService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncService.class);

    public static final Gson GSON = new GsonBuilder().create();

    public static void kill(final int processId, final boolean connection) {
        final KillMessage killMessage = new KillMessage(processId, connection);
        final int timeout = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.GENERAL_SERVICE_TIMEOUT));

        final String[] nodes = NodeWatchdog.getInstance().getNodes();
        for (String node : nodes) {
            ProxyExecutor.getInstance().getExecutor().submit(() -> {
                try {
                    final InetSocketAddress address = AddressDecoder.decode(node);
                    GeneralService.invoke(
                        address.getHostString(), address.getPort(), "kill", GSON.toJson(killMessage), timeout);
                } catch (Throwable t) {
                    LOGGER.error("Failed to send kill message to {}", node, t);
                }
            });
        }
    }

    public static void init() {
        GeneralService.registerHandler("kill", new KillService());
    }
}
