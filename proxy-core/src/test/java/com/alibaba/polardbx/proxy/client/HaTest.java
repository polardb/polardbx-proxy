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
import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.net.NIOWorker;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import com.alibaba.polardbx.proxy.utils.NamedThreadFactory;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Ignore("manual test only")
public class HaTest {
    @BeforeClass
    public static void beforeClass() {
        ProxyExecutor.init();
    }

    private final NIOWorker nioWorker = new NIOWorker(2);
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(4,
        4,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new NamedThreadFactory(ThreadNames.TEST_POOL));

    @Test
    public void simpleTest() throws Exception {
        HaManager.init(nioWorker, executor);
        try (final BackendConnectionWrapper conn = HaManager.getInstance().getAdminConnection()) {
            conn.waitLogin(3, TimeUnit.SECONDS);
            final QueryResultHandler result = conn.sendQuery("select 1,2,3");
            byte[][] row;
            while ((row = result.next(1, TimeUnit.SECONDS)) != null) {
                for (byte[] bytes : row) {
                    System.out.print(new String(bytes) + ", ");
                }
                System.out.println();
            }
        }
    }
}
