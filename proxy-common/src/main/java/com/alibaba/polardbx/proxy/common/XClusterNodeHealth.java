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

import lombok.Getter;

@Getter
public class XClusterNodeHealth {
    private final String tag;
    private final String role;
    private final String proxyToken;
    private final long commitIndex;
    private final long applyIndex;
    private final long rttNanos;
    private final long updateNanos;

    public XClusterNodeHealth(String tag, String role, String proxyToken, long commitIndex, long applyIndex,
                              long rttNanos, long updateNanos) {
        this.tag = tag;
        this.role = role;
        this.proxyToken = proxyToken;
        this.commitIndex = commitIndex;
        this.applyIndex = applyIndex;
        this.rttNanos = rttNanos;
        this.updateNanos = updateNanos;
    }

    @Override
    public String toString() {
        return "XClusterNodeHealth{" +
            "tag='" + tag + '\'' +
            ", role='" + role + '\'' +
            ", proxyToken='" + proxyToken + '\'' +
            ", commitIndex=" + commitIndex +
            ", applyIndex=" + applyIndex +
            ", rtt=" + rttNanos / 1000 + " us" +
            ", updated=" + (System.nanoTime() - updateNanos) / 1000 + " us ago" +
            '}';
    }
}
