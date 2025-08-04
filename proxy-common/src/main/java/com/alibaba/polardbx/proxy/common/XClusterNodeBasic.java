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

import com.google.gson.annotations.SerializedName;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class XClusterNodeBasic {
    @SerializedName("tag")
    private final String tag;
    @SerializedName("host")
    private final String host;
    @SerializedName("port")
    private final int port;
    @SerializedName("xport")
    private final int xport;
    @SerializedName("paxos_port")
    private final int paxosPort;
    @SerializedName("role")
    private final String role;
    @SerializedName("peers")
    private final List<XClusterNodeBasic> peers;
    @SerializedName("version")
    private final String version;
    @SerializedName("cluster_id")
    private final long clusterId;
    @SerializedName("update_time")
    private final String updateTime;

    public XClusterNodeBasic(String tag, String host, int port, int xport, int paxosPort, String role,
                             List<XClusterNodeBasic> peers, String version, long clusterId, String updateTime) {
        this.tag = tag;
        this.host = host;
        this.port = port;
        this.xport = xport;
        this.paxosPort = paxosPort;
        this.role = role;
        this.peers = peers;
        this.version = version;
        this.clusterId = clusterId;
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof XClusterNodeBasic) {
            XClusterNodeBasic node = (XClusterNodeBasic) obj;
            return Objects.equals(tag, node.tag) && Objects.equals(host, node.host) && node.port == port
                && node.xport == xport && node.paxosPort == paxosPort && Objects.equals(role, node.role)
                && Objects.equals(peers, node.peers) && Objects.equals(version, node.version)
                && clusterId == node.clusterId; // ignore updateTime
        }
        return false;
    }

    @Override
    public String toString() {
        return "XClusterNode{" +
            "tag='" + tag + '\'' +
            ", host='" + host + '\'' +
            ", port=" + port +
            ", xport=" + xport +
            ", paxosPort=" + paxosPort +
            ", role='" + role + '\'' +
            ", peers=" + peers +
            ", version='" + version + '\'' +
            ", clusterId=" + clusterId +
            ", updateTime='" + updateTime + '\'' +
            '}';
    }
}
