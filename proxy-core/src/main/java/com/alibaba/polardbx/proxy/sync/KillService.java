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

package com.alibaba.polardbx.proxy.sync;

import com.alibaba.polardbx.proxy.ServiceHandler;
import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.connection.pool.BackendPool;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KillService implements ServiceHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KillService.class);

    private static void cancel(final BackendConnectionWrapper conn)
        throws IOException, SQLException, InterruptedException, TimeoutException {
        final int backendId = conn.probeConnectionId(); // may NPE and throw
        final int timeout = Integer.parseInt(ConfigLoader.PROPERTIES.getProperty(ConfigProps.GENERAL_SERVICE_TIMEOUT));
        final long timeoutNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);

        final BackendPool pool = conn.getBackendPool();
        try (final BackendConnectionWrapper another = pool.getConnection()) {
            final QueryResultHandler killQueryHandler = another.sendQuery("KILL QUERY " + (backendId & 0xFFFF_FFFFL));
            killQueryHandler.update(timeoutNanos); // ignore result
        }
    }

    @Override
    public String handle(String requestJson) {
        try {
            final KillMessage killMessage = SyncService.GSON.fromJson(requestJson, KillMessage.class);
            LOGGER.debug("Received synced kill message: {}", killMessage);

            // find frontend connection
            int found = 0;
            for (final FrontendConnection frontend : FrontendConnection.CONNECTIONS) {
                if (frontend.getContext().getConnectionId() != killMessage.getProcessId()) {
                    continue;
                }

                if (killMessage.isConnection()) {
                    // kill connection
                    frontend.close();
                } else {
                    // kill query
                    final FrontendTransactionContext transactionContext = frontend.getContext().getTransactionContext();
                    if (transactionContext != null) {
                        // send kill to backend via new connection from pool
                        BackendConnectionWrapper conn = transactionContext.getExistingRwConnection();
                        if (conn != null) {
                            try {
                                cancel(conn);
                            } catch (Throwable t) {
                                LOGGER.error("Error in kill RW query", t);
                            }
                        }
                        conn = transactionContext.getExistingRoConnection();
                        if (conn != null) {
                            try {
                                cancel(conn);
                            } catch (Throwable t) {
                                LOGGER.error("Error in kill RO query", t);
                            }
                        }
                    }
                }
                ++found;
            }

            // done
            return SyncService.GSON.toJson(
                new GeneralServiceResponse(GeneralServiceErrorCode.SUCCESS, found + " killed."));
        } catch (Throwable t) {
            LOGGER.error("Error in kill service", t);
            return SyncService.GSON.toJson(new GeneralServiceResponse(GeneralServiceErrorCode.ERROR, t.getMessage()));
        }
    }
}
