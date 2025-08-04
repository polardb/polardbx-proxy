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

import com.alibaba.polardbx.proxy.common.XClusterNodeBasic;
import com.alibaba.polardbx.proxy.common.XClusterNodeHealth;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowClusterHandler extends SystemTableRequestHandler {
    public ShowClusterHandler(FrontendContext context) {
        super(context);
        setTag("ShowClusterHandler");
    }

    private static final ColumnDefinition41[] fields = new ColumnDefinition41[] {
        new ColumnDefinition41().fieldVarchar("address".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 32).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("host".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 256).setNotNull(true),
        new ColumnDefinition41().fieldLong("port".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("xport".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("paxos port".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldVarchar("role".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 32).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("token".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 32).setNotNull(true),
        new ColumnDefinition41().fieldLong("commit index".getBytes(StandardCharsets.UTF_8)).setBinary(true),
        new ColumnDefinition41().fieldLong("apply index".getBytes(StandardCharsets.UTF_8)).setBinary(true),
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
        final List<XClusterNodeBasic> clusterPeers = HaManager.getInstance().getClusterPeers();
        final HaManager.XClusterServerless clusterServerless = HaManager.getInstance().getClusterServerless();
        final Map<String, XClusterNodeHealth> healthMap = new HashMap<>(
            1 + clusterServerless.getFollowers().size() + clusterServerless.getLearners().size());
        final XClusterNodeHealth leaderHealth = clusterServerless.getLeader();
        if (leaderHealth != null) {
            healthMap.put(leaderHealth.getTag(), leaderHealth);
        }
        for (final XClusterNodeHealth follower : clusterServerless.getFollowers()) {
            healthMap.put(follower.getTag(), follower);
        }
        for (final XClusterNodeHealth learner : clusterServerless.getLearners()) {
            healthMap.put(learner.getTag(), learner);
        }
        final long nowNanos = System.nanoTime();
        for (final XClusterNodeBasic peer : clusterPeers) {
            final XClusterNodeHealth health = healthMap.get(peer.getTag());

            // build row
            final byte[][] row = new byte[fields.length][];
            row[0] = peer.getTag().getBytes(StandardCharsets.UTF_8);
            row[1] = peer.getHost().getBytes(StandardCharsets.UTF_8);
            row[2] = String.valueOf(peer.getPort()).getBytes(StandardCharsets.UTF_8);
            row[3] = String.valueOf(peer.getXport()).getBytes(StandardCharsets.UTF_8);
            row[4] = String.valueOf(peer.getPaxosPort()).getBytes(StandardCharsets.UTF_8);
            row[5] = peer.getRole().getBytes(StandardCharsets.UTF_8);
            if (null == health) {
                row[6] = row[7] = row[8] = row[9] = row[11] = null;
            } else {
                row[6] = null == health.getProxyToken() ? null :
                    (health.getProxyToken().substring(0, 4) + "***").getBytes(StandardCharsets.UTF_8);
                row[7] = String.valueOf(health.getCommitIndex()).getBytes(StandardCharsets.UTF_8);
                row[8] = String.valueOf(health.getApplyIndex()).getBytes(StandardCharsets.UTF_8);
                row[9] = String.valueOf(health.getRttNanos() / 1000000.f).getBytes(StandardCharsets.UTF_8);
                row[11] = ((nowNanos - health.getUpdateNanos()) / 1000000 + " ms ago").getBytes(StandardCharsets.UTF_8);
            }
            final Long latencyNanos = HaManager.getInstance().getLatencyChecker().getLatencyNanos(peer.getTag());
            if (latencyNanos != null) {
                row[10] = String.valueOf(latencyNanos / 1000000.f).getBytes(StandardCharsets.UTF_8);
            } else {
                if (peer.getRole().equalsIgnoreCase("Leader")) {
                    row[10] = "0".getBytes(StandardCharsets.UTF_8);
                } else {
                    row[10] = null;
                }
            }

            consumer.accept(row);
        }
    }
}
