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

package com.alibaba.polardbx.proxy.client;

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.connection.BackendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.connection.pool.BackendPool;
import com.alibaba.polardbx.proxy.net.NIOWorker;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

@Ignore("manual test only")
public class BackendConnectionTest {
    private static final String IP = "";
    private static final int PORT = 0;
    private static final String USER = "";
    private static final String PSW = "";

    @BeforeClass
    public static void beforeClass() {
        ProxyExecutor.init();
    }

    @Test
    public void connectTest() throws Exception {
        final NIOWorker nioWorker = new NIOWorker(4);
        final SocketAddress address = new InetSocketAddress(IP, PORT);
        try (final BackendConnection connection = BackendConnection.connectBlocking(
            address, nioWorker.getProcessor(), USER, PSW, null, 3000)) {
            QueryResultHandler result = connection.sendQuery("set read_lsn = 99999999999; select 2");
            byte[][] row;
            do {
                System.out.println("--------------------------------------------------------------------------------");
                try {
                    while ((row = result.next(10, TimeUnit.SECONDS)) != null) {
                        for (byte[] bytes : row) {
                            System.out.print(new String(bytes) + ", ");
                        }
                        System.out.println();
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            } while ((result = (QueryResultHandler) result.hasMore()) != null);

            // again other request
            result = connection.sendQuery("select 1,2,3; select 2");
            do {
                System.out.println("--------------------------------------------------------------------------------");
                try {
                    while ((row = result.next(1, TimeUnit.SECONDS)) != null) {
                        for (byte[] bytes : row) {
                            System.out.print(new String(bytes) + ", ");
                        }
                        System.out.println();
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            } while ((result = (QueryResultHandler) result.hasMore()) != null);
        }
    }

    @Test
    public void poolTest() throws Exception {
        final NIOWorker nioWorker = new NIOWorker(4);
        final SocketAddress address = new InetSocketAddress(IP, PORT);
        try (final BackendPool backendPool = new BackendPool(nioWorker, address, null, USER, PSW, null, 5, false)) {
            backendPool.loadDbConfigs();
            try (final BackendConnectionWrapper wrapper = backendPool.getConnection()) {
                wrapper.waitLogin(3, TimeUnit.SECONDS);
                final QueryResultHandler res0 = wrapper.sendQuery("select 1,2,3,sleep(0.5)");
                final QueryResultHandler res1 = wrapper.sendQuery("select 4,5,6,sleep(0.5)");
                byte[][] row;
                while ((row = res0.next(1, TimeUnit.SECONDS)) != null) {
                    for (byte[] bytes : row) {
                        System.out.print(new String(bytes) + ", ");
                    }
                    System.out.println();
                }
                while ((row = res1.next(1, TimeUnit.SECONDS)) != null) {
                    for (byte[] bytes : row) {
                        System.out.print(new String(bytes) + ", ");
                    }
                    System.out.println();
                }
            }
        }
    }
}
