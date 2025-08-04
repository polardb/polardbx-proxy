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

package com.alibaba.polardbx.proxy.privilege;

import lombok.Getter;

@Getter
public class PrivilegeInfo {
    private final String host;
    private final QuarantineConfig.Netmask netmask;
    private final String user;
    private final byte[] authentication;
    private final boolean expired;
    private final boolean locked;

    public PrivilegeInfo(String host, String user, byte[] authentication, boolean expired, boolean locked) {
        this.host = host;
        QuarantineConfig.Netmask netmask = null;
        try {
            netmask = new QuarantineConfig.Netmask(host);
        } catch (Throwable ignore) {
        }
        this.netmask = netmask;
        this.user = user;
        this.authentication = authentication;
        this.expired = expired;
        this.locked = locked;
    }
}
