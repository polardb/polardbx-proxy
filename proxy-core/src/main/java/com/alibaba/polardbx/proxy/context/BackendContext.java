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

package com.alibaba.polardbx.proxy.context;

import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.help.LruCache;
import com.alibaba.polardbx.proxy.context.help.ServerPreparedStatementKey;
import com.alibaba.polardbx.proxy.protocol.common.MysqlClientState;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;

public class BackendContext extends MysqlContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackendContext.class);

    @Getter
    @Setter
    private MysqlClientState state = MysqlClientState.Init;

    @Getter
    @Setter
    private byte[] version;
    @Getter
    @Setter
    private boolean upToDate = true;

    public BackendContext(InetSocketAddress remoteAddress, int connectionId, int capabilities) {
        super(remoteAddress, connectionId, capabilities);
    }

    // cache for prepared statement
    private volatile LruCache<ServerPreparedStatementKey, Integer> preparedStatementCache;

    private LruCache<ServerPreparedStatementKey, Integer> initPreparedStatementCache(
        @NotNull BackendConnectionWrapper backend) {
        LruCache<ServerPreparedStatementKey, Integer> cache = preparedStatementCache;
        if (null == cache) {
            synchronized (this) {
                if (null == (cache = preparedStatementCache)) {
                    final int cache_size = Integer.parseInt(
                        ConfigLoader.PROPERTIES.getProperty(ConfigProps.PREPARED_STATEMENT_CACHE_SIZE));
                    cache = preparedStatementCache = new LruCache<>(cache_size,
                        (k, v) -> {
                            try {
                                backend.closePreparedStatement(v);
                            } catch (Throwable e) {
                                LOGGER.error("Failed to close prepared statement: {} id: {} on connection: {}", k, v,
                                    backend, e);
                            }
                        });
                }
            }
        }
        return cache;
    }

    public int countPreparedStatement() {
        final LruCache<ServerPreparedStatementKey, Integer> cache = preparedStatementCache;
        if (null == cache) {
            return 0;
        }
        synchronized (cache) {
            return cache.size();
        }
    }

    public String showPreparedStatement() {
        final LruCache<ServerPreparedStatementKey, Integer> cache = preparedStatementCache;
        if (null == cache) {
            return null;
        }
        final StringBuilder builder = new StringBuilder();
        boolean first = true;
        synchronized (cache) {
            for (final Map.Entry<ServerPreparedStatementKey, Integer> entry : cache.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    builder.append('\n');
                }
                builder.append(entry.getValue()).append(':').append(entry.getKey().getSchema()).append(':')
                    .append(entry.getKey().getPrepareSql());
            }
        }
        return 0 == builder.length() ? null : builder.toString();
    }

    public void recordPreparedStatement(@NotNull BackendConnectionWrapper backend,
                                        @NotNull ServerPreparedStatementKey key, int statementId, String tag) {
        final LruCache<ServerPreparedStatementKey, Integer> cache = initPreparedStatementCache(backend);
        synchronized (cache) {
            cache.compute(key, (k, v) -> {
                if (v != null) {
                    // replace whit latest
                    try {
                        backend.closePreparedStatement(v);
                    } catch (Throwable e) {
                        LOGGER.error("Failed to close prepared statement: {} id: {} on connection: {}", k, v, backend,
                            e);
                    }
                }
                LOGGER.debug("Cache backend prepared statement: {} id: {} on connection: {} tag: {}",
                    k, statementId, backend, tag);
                return statementId;
            });
        }
    }

    public Integer takePreparedStatement(@NotNull BackendConnectionWrapper backend,
                                         @NotNull ServerPreparedStatementKey key) {
        final LruCache<ServerPreparedStatementKey, Integer> cache = initPreparedStatementCache(backend);
        synchronized (cache) {
            return cache.remove(key);
        }
    }

    @Override
    public void close() {
        // set state to closed
        state = MysqlClientState.Closed;

        // finalize the leak check
        super.close();
    }
}
