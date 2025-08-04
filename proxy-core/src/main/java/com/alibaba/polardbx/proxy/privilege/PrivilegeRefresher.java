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

import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PrivilegeRefresher extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrivilegeRefresher.class);

    private static final String LOAD_PRIVILEGE_SQL =
        "/* PolarDB-X-Proxy PrivilegeRefresher */"
            + " select `Host`,`User`,`authentication_string`,(`password_expired`!='N') as `is_password_expired`,(`account_locked`='Y') as is_account_locked"
            + " from `mysql`.`user` where `plugin`='mysql_native_password'";
    private static final String LOAD_SCHEMA_SQL =
        "/* PolarDB-X-Proxy PrivilegeRefresher */ select `SCHEMA_NAME` from `information_schema`.`SCHEMATA`";

    private final AtomicReference<Map<String, List<PrivilegeInfo>>> privilegeInfoMapRef = new AtomicReference<>(null);
    private final AtomicReference<Set<String>> schemaSetRef = new AtomicReference<>(null);
    private final AtomicReference<Set<String>> schemaUpperSetRef = new AtomicReference<>(null);

    public PrivilegeRefresher() {
        super(ThreadNames.PRIVILEGE_REFRESHER);

        // set thread and start
        setDaemon(true);
        start();
    }

    public void refresh() {
        synchronized (privilegeInfoMapRef) {
            LOGGER.info("Notify refreshing privilege info...");
            privilegeInfoMapRef.notifyAll();
        }
    }

    public Map<String, List<PrivilegeInfo>> getPrivilegeInfoMap() {
        return privilegeInfoMapRef.getAcquire();
    }

    public Set<String> getSchemaSet() {
        return schemaSetRef.getAcquire();
    }

    public Set<String> getSchemaUpperSet() {
        return schemaUpperSetRef.getAcquire();
    }

    private static byte[] hex2bytes(String s, int offset, int length) {
        final byte[] data = new byte[length / 2];
        for (int i = 0; i < length; i += 2) {
            final char c0 = s.charAt(offset + i), c1 = s.charAt(offset + i + 1);
            int b;
            if (c0 >= '0' && c0 <= '9') {
                b = (byte) (c0 - '0');
            } else if (c0 >= 'a' && c0 <= 'f') {
                b = (byte) (c0 - 'a' + 10);
            } else if (c0 >= 'A' && c0 <= 'F') {
                b = (byte) (c0 - 'A' + 10);
            } else {
                throw new IllegalArgumentException("Invalid hex digit '" + c0 + "'");
            }
            b <<= 4;
            if (c1 >= '0' && c1 <= '9') {
                b |= (byte) (c1 - '0');
            } else if (c1 >= 'a' && c1 <= 'f') {
                b |= (byte) (c1 - 'a' + 10);
            } else if (c1 >= 'A' && c1 <= 'F') {
                b |= (byte) (c1 - 'A' + 10);
            } else {
                throw new IllegalArgumentException("Invalid hex digit '" + c1 + "'");
            }
            data[i / 2] = (byte) b;
        }
        return data;
    }

    @Override
    public void run() {
        while (true) {
            try {
                final int timeout =
                    Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.PRIVILEGE_REFRESH_TIMEOUT));
                final long limitTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
                try (final BackendConnectionWrapper connection = HaManager.getInstance().getAdminConnection()) {
                    final QueryResultHandler privilegeResult = connection.sendQuery(LOAD_PRIVILEGE_SQL);
                    final Map<String, List<PrivilegeInfo>> privilegeInfoList = new HashMap<>();
                    privilegeResult.consume(row -> {
                        final String host = new String(row[0]);
                        final String user = new String(row[1]);
                        final String authentication = null == row[2] ? null : new String(row[2]);
                        final boolean expired = new String(row[3]).equals("1");
                        final boolean locked = new String(row[4]).equals("1");
                        final byte[] authenticationBytes;
                        if (null == authentication || authentication.isEmpty()) {
                            authenticationBytes = null;
                        } else if (authentication.startsWith("*") && 0 == (authentication.length() - 1) % 2
                            && authentication.length() > 1) {
                            try {
                                authenticationBytes = hex2bytes(authentication, 1, authentication.length() - 1);
                            } catch (Throwable t) {
                                LOGGER.warn("Invalid authentication string: {}", authentication, t);
                                return; // ignore unknown authentication string
                            }
                        } else {
                            // unknown password, just set a denied password
                            authenticationBytes = ProxyPrivileges.BAD_PASSWORD;
                        }
                        final PrivilegeInfo info = new PrivilegeInfo(host, user, authenticationBytes, expired, locked);
                        privilegeInfoList.compute(user, (k, v) -> {
                            final List<PrivilegeInfo> list = null == v ? new ArrayList<>() : v;
                            list.add(info);
                            return list;
                        });
                    }, limitTimeNs);
                    if (!privilegeInfoList.isEmpty()) {
                        for (final Map.Entry<String, List<PrivilegeInfo>> entry : privilegeInfoList.entrySet()) {
                            entry.setValue(Collections.unmodifiableList(entry.getValue()));
                        }
                        synchronized (privilegeInfoMapRef) {
                            privilegeInfoMapRef.setRelease(Collections.unmodifiableMap(privilegeInfoList));
                            privilegeInfoMapRef.notifyAll();
                        }
                    }

                    final QueryResultHandler schemaResult = connection.sendQuery(LOAD_SCHEMA_SQL);
                    final Set<String> schemaSet = new HashSet<>(), schemaUpperSet = new HashSet<>();
                    schemaResult.consume(row -> {
                        final BackendContext context = connection.getContextReference().getAcquire();
                        final String schema = context.decodeStringResults(row[0]);
                        schemaSet.add(schema);
                        schemaUpperSet.add(schema.toUpperCase());
                    }, limitTimeNs);
                    if (!schemaSet.isEmpty()) {
                        schemaSetRef.setRelease(Collections.unmodifiableSet(schemaSet));
                        schemaUpperSetRef.setRelease(Collections.unmodifiableSet(schemaUpperSet));
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("Privilege refresher error", t);
            }

            try {
                final Map<String, List<PrivilegeInfo>> map = privilegeInfoMapRef.getAcquire();
                if (null == map || map.isEmpty()) {
                    Thread.sleep(1000);
                } else {
                    final int interval =
                        Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.PRIVILEGE_REFRESH_INTERVAL));
                    synchronized (privilegeInfoMapRef) {
                        privilegeInfoMapRef.wait(interval);
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("Privilege refresher sleep error", t);
            }
        }
    }

    private static PrivilegeRefresher INSTANCE;

    public static void init() throws InterruptedException {
        if (null == INSTANCE) {
            final boolean first;
            synchronized (HaManager.class) {
                if (null == INSTANCE) {
                    INSTANCE = new PrivilegeRefresher();
                    first = true;
                } else {
                    first = false;
                }
            }

            if (first) {
                // wait privileges ready
                LOGGER.info("Backend privilege initializing...");
                synchronized (INSTANCE.privilegeInfoMapRef) {
                    while (null == INSTANCE.privilegeInfoMapRef.getAcquire()) {
                        INSTANCE.privilegeInfoMapRef.wait();
                    }
                }
                LOGGER.info("Backend privilege initialized.");
            }
        }
    }

    public static PrivilegeRefresher getInstance() {
        return INSTANCE;
    }
}
