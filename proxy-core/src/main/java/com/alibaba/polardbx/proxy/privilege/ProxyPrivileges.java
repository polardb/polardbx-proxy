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

import java.security.SecureRandom;
import java.util.List;
import java.util.Set;

public class ProxyPrivileges implements Privileges {
    public static final byte[] BAD_PASSWORD = SecurityUtil.calcMysqlUserPassword(generateRandomBytes(20));

    private static byte[] generateRandomBytes(int length) {
        final SecureRandom random = new SecureRandom();
        final byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    @Override
    public boolean schemaExists(String schema) {
        final Set<String> schemaUpperSet = PrivilegeRefresher.getInstance().getSchemaUpperSet();
        return null != schemaUpperSet && schemaUpperSet.contains(schema.toUpperCase());
    }

    @Override
    public boolean isTrustedIp(String host, String user) {
        return host.equals("127.0.0.1") && user.equals("polardbx_root");
    }

    @Override
    public PrivilegeInfo getPrivilegeInfo(String user, String host) {
        final List<PrivilegeInfo> privilegeInfos =
            PrivilegeRefresher.getInstance().getPrivilegeInfoMap().get(user);
        if (null == privilegeInfos) {
            return null;
        }
        for (final PrivilegeInfo privilegeInfo : privilegeInfos) {
            if (null == privilegeInfo.getNetmask() || !privilegeInfo.getNetmask()
                .isInScope(QuarantineConfig.ip2long(host))) {
                continue;
            }
            // in scope
            if (privilegeInfo.isExpired() || privilegeInfo.isLocked()) {
                return null;
            }
            return privilegeInfo;
        }
        return null;
    }

    private static final class InstanceHolder {
        private static final ProxyPrivileges INSTANCE = new ProxyPrivileges();
    }

    public static ProxyPrivileges getInstance() {
        return InstanceHolder.INSTANCE;
    }
}
