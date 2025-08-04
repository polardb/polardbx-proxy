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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ShowRoHandler extends SystemTableRequestHandler {
    public ShowRoHandler(FrontendContext context) {
        super(context);
        setTag("ShowRoHandler");
    }

    @Override
    protected ColumnDefinition41[] getFields() {
        return ShowRwHandler.fields;
    }

    @Override
    protected void emitRows(RowConsumer consumer) throws IOException {
        final HaManager.XClusterServerless clusterServerless = HaManager.getInstance().getClusterServerless();
        final Map<String, XClusterNodeHealth> healthMap = new HashMap<>(
            1 + clusterServerless.getFollowers().size() + clusterServerless.getLearners().size());
        if (clusterServerless.getLeader() != null) {
            healthMap.put(clusterServerless.getLeader().getTag(), clusterServerless.getLeader());
        }
        for (final XClusterNodeHealth follower : clusterServerless.getFollowers()) {
            healthMap.put(follower.getTag(), follower);
        }
        for (final XClusterNodeHealth learner : clusterServerless.getLearners()) {
            healthMap.put(learner.getTag(), learner);
        }
        final Map<String, Integer> weights = HaManager.getInstance().getReadWriteSplittingPool().getNowRoWeights();

        final long nowNanos = System.nanoTime();
        for (final Map.Entry<String, Integer> ro : weights.entrySet()) {
            // build row
            final byte[][] row = new byte[ShowRwHandler.fields.length][];
            row[0] = ro.getKey().getBytes(StandardCharsets.UTF_8);
            row[1] = String.valueOf(ro.getValue()).getBytes(StandardCharsets.UTF_8);
            final BackendPool pool =
                HaManager.getInstance().getReadWriteSplittingPool().getRoPoolMap().get(ro.getKey());
            if (null == pool) {
                row[2] = row[3] = row[4] = null;
            } else {
                row[2] = String.valueOf(pool.getNowRunningConnectionCount()).getBytes(StandardCharsets.UTF_8);
                row[3] = String.valueOf(pool.getNowIdleConnectionCount()).getBytes(StandardCharsets.UTF_8);
                row[4] = String.valueOf(pool.getMaxPooled()).getBytes(StandardCharsets.UTF_8);
            }
            final XClusterNodeHealth health = healthMap.get(ro.getKey());
            if (null == health) {
                row[5] = row[6] = row[7] = row[9] = null;
            } else {
                row[5] = health.getRole().getBytes(StandardCharsets.UTF_8);
                row[6] = null == health.getProxyToken() ? null :
                    (health.getProxyToken().substring(0, 4) + "***").getBytes(StandardCharsets.UTF_8);
                row[7] = String.valueOf(health.getRttNanos() / 1000000.f).getBytes(StandardCharsets.UTF_8);
                row[9] = ((nowNanos - health.getUpdateNanos()) / 1000000 + " ms ago").getBytes(StandardCharsets.UTF_8);
            }
            final Long latencyNanos = HaManager.getInstance().getLatencyChecker().getLatencyNanos(ro.getKey());
            if (latencyNanos != null) {
                row[8] = String.valueOf(latencyNanos / 1000000.f).getBytes(StandardCharsets.UTF_8);
            } else {
                row[8] = health != null && health.getRole().equalsIgnoreCase("Leader") ?
                    "0".getBytes(StandardCharsets.UTF_8) : null;
            }

            consumer.accept(row);
        }
    }
}
