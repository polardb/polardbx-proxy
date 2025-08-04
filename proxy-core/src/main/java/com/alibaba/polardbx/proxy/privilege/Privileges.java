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

public interface Privileges {
    /**
     * 检查schema是否存在
     */
    default boolean schemaExists(String schema) {
        throw new UnsupportedOperationException();
    }

    /**
     * 返回用户的服务器端权限凭据, 如果有多个匹配, 则返回第一个匹配用户的.密码为(sha1(sha1(psw)))
     */
    default PrivilegeInfo getPrivilegeInfo(String user, String host) {
        throw new UnsupportedOperationException();
    }

    /**
     * 判断是否是信任的白名单IP，可以没有配置信任白名单
     */
    default boolean isTrustedIp(String host, String user) {
        throw new UnsupportedOperationException();
    }
}
