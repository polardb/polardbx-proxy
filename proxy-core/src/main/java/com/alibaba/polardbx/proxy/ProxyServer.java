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

import com.alibaba.polardbx.proxy.cluster.AcceptIdGenerator;
import com.alibaba.polardbx.proxy.cluster.GeneralIdGenerator;
import com.alibaba.polardbx.proxy.cluster.NodeWatchdog;
import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.net.NIOAcceptor;
import com.alibaba.polardbx.proxy.net.NIOConnection;
import com.alibaba.polardbx.proxy.net.NIOConnectionFactory;
import com.alibaba.polardbx.proxy.net.NIOProcessor;
import com.alibaba.polardbx.proxy.net.NIOWorker;
import com.alibaba.polardbx.proxy.privilege.PrivilegeRefresher;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import com.alibaba.polardbx.proxy.serverless.SmoothSwitchoverMonitor;
import com.alibaba.polardbx.proxy.sync.SyncService;
import com.alibaba.polardbx.proxy.utils.NamedThreadFactory;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Getter
public class ProxyServer implements NIOConnectionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyServer.class);

    private final AcceptIdGenerator acceptIdGenerator;
    private final GeneralIdGenerator trxIdGenerator;
    private final NIOWorker worker;

    private ProxyServer() throws Exception {
        final int clusterNodeId = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.CLUSTER_NODE_ID));
        acceptIdGenerator = new AcceptIdGenerator(clusterNodeId);
        trxIdGenerator = new GeneralIdGenerator(clusterNodeId);

        final int cpus = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.CPUS));
        final int factor = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.REACTOR_FACTOR));
        this.worker = new NIOWorker((cpus <= 0 ? Runtime.getRuntime().availableProcessors() : cpus) * factor);

        // init HA manager
        final int haWorkerThreads =
            Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_HA_WORKER_THREADS));
        final ThreadPoolExecutor haExecutor = new ThreadPoolExecutor(haWorkerThreads,
            haWorkerThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(ThreadNames.HA_EXECUTOR));
        HaManager.init(worker, haExecutor);

        // init leader transfer monitor
        SmoothSwitchoverMonitor.init();

        // init privilege refresher
        PrivilegeRefresher.init();

        // init service registration and start server
        SyncService.init();
        final int servicePort = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.GENERAL_SERVICE_PORT));
        GeneralService.startServer(servicePort);

        // register myself to cluster nodes table
        NodeWatchdog.init();

        // init acceptor
        final int proxyPort = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.FRONTEND_PORT));
        NIOAcceptor acceptor = new NIOAcceptor(ThreadNames.NIO_ACCEPTOR, proxyPort, worker, this);
        acceptor.start();

        LOGGER.info("==================== Proxy started.");
    }

    @Override
    public NIOConnection accept(SocketChannel channel, NIOProcessor processor) {
        return new FrontendConnection(channel, processor);
    }

    private static ProxyServer INSTANCE = null;

    public static ProxyServer getInstance() {
        final ProxyServer inst = INSTANCE;
        if (null == inst) {
            throw new IllegalStateException("ProxyServer is not initialized.");
        }
        return inst;
    }

    public synchronized static void init() throws Exception {
        if (null == INSTANCE) {
            INSTANCE = new ProxyServer();
        }
    }
}
