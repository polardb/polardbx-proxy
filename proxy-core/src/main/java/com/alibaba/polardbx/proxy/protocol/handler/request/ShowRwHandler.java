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

package com.alibaba.polardbx.proxy.protocol.handler.request;

import com.alibaba.polardbx.proxy.common.XClusterNodeHealth;
import com.alibaba.polardbx.proxy.connection.pool.BackendPool;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ShowRwHandler extends SystemTableRequestHandler {
    public ShowRwHandler(FrontendContext context) {
        super(context);
        setTag("ShowRwHandler");
    }

    public static final ColumnDefinition41[] fields = new ColumnDefinition41[] {
        new ColumnDefinition41().fieldVarchar("address".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 32).setNotNull(true),
        new ColumnDefinition41().fieldLong("weight".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("running".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("idle".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("max pooled".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldVarchar("role".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 32).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("token".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 32).setNotNull(true),
        new ColumnDefinition41().fieldFloat("rtt(ms)".getBytes(StandardCharsets.UTF_8)).setBinary(true),
        new ColumnDefinition41().fieldFloat("delay(ms)".getBytes(StandardCharsets.UTF_8)).setBinary(true),
        new ColumnDefinition41().fieldVarchar("update time".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 32)
    };

    @Override
    protected ColumnDefinition41[] getFields() {
        return fields;
    }

    @Override
    protected void emitRows(RowConsumer consumer) throws IOException {
        final XClusterNodeHealth leader = HaManager.getInstance().getClusterServerless().getLeader();
        if (leader != null) {
            // build row
            final byte[][] row = new byte[fields.length][];
            row[0] = leader.getTag().getBytes(StandardCharsets.UTF_8);
            row[1] = "1".getBytes(StandardCharsets.UTF_8);
            final BackendPool pool = HaManager.getInstance().getReadWriteSplittingPool().getRwPool();
            if (null == pool) {
                row[2] = row[3] = row[4] = null;
            } else {
                row[2] = String.valueOf(pool.getNowRunningConnectionCount()).getBytes(StandardCharsets.UTF_8);
                row[3] = String.valueOf(pool.getNowIdleConnectionCount()).getBytes(StandardCharsets.UTF_8);
                row[4] = String.valueOf(pool.getMaxPooled()).getBytes(StandardCharsets.UTF_8);
            }
            row[5] = leader.getRole().getBytes(StandardCharsets.UTF_8);
            row[6] = null == leader.getProxyToken() ? null :
                (leader.getProxyToken().substring(0, 4) + "***").getBytes(StandardCharsets.UTF_8);
            row[7] = String.valueOf(leader.getRttNanos() / 1000000.f).getBytes(StandardCharsets.UTF_8);
            row[8] = "0".getBytes(StandardCharsets.UTF_8);
            row[9] =
                ((System.nanoTime() - leader.getUpdateNanos()) / 1000000 + " ms ago").getBytes(StandardCharsets.UTF_8);

            consumer.accept(row);
        }
    }
}
