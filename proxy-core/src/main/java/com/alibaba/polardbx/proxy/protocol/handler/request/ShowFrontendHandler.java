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

import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.help.PreparedStatementContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.utils.CaseInsensitiveString;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ShowFrontendHandler extends SystemTableRequestHandler {
    private final boolean full;

    public ShowFrontendHandler(FrontendContext context, boolean full) {
        super(context);
        this.full = full;
        setTag("ShowFrontendHandler");
    }

    private static final ColumnDefinition41[] fields = new ColumnDefinition41[] {
        new ColumnDefinition41().fieldLong("id".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldVarchar("user".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("capabilities".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("host".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("privilege_host".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("db".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("state".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("client_charset".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("connection_charset".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("results_charset".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("prepared_statement".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 65535).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("transaction_status".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 65535).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("changed_variables".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 65535).setNotNull(true)
    };

    @Override
    protected ColumnDefinition41[] getFields() {
        return fields;
    }

    @Override
    protected void emitRows(RowConsumer consumer) throws IOException {
        for (final FrontendConnection connection : FrontendConnection.CONNECTIONS) {
            final FrontendContext context = connection.getContext();
            // build row
            final byte[][] row = new byte[fields.length][];
            row[0] = String.valueOf(context.getConnectionId()).getBytes(StandardCharsets.UTF_8);
            row[1] = null == context.getUsername() ? null : context.getUsername().getBytes(StandardCharsets.UTF_8);
            row[2] = ("0x" + Integer.toHexString(context.getCapabilities())).getBytes(StandardCharsets.UTF_8);
            row[3] = (context.getRemoteAddress().getHostString() + ':' + context.getRemoteAddress().getPort()).getBytes(
                StandardCharsets.UTF_8);
            row[4] = null == context.getPrivilegeHost() ?
                null : context.getPrivilegeHost().getBytes(StandardCharsets.UTF_8);
            row[5] = null == context.getDatabase() ? null : context.getDatabase().getBytes(StandardCharsets.UTF_8);
            row[6] = context.getState().name().getBytes(StandardCharsets.UTF_8);
            row[7] = CharsetMapping.getStaticCollationNameForCollationIndex(context.getClientCharsetIndex())
                .getBytes(StandardCharsets.UTF_8);
            row[8] = CharsetMapping.getStaticCollationNameForCollationIndex(context.getConnectionCharsetIndex())
                .getBytes(StandardCharsets.UTF_8);
            row[9] = CharsetMapping.getStaticCollationNameForCollationIndex(context.getResultsCharsetIndex())
                .getBytes(StandardCharsets.UTF_8);
            if (full) {
                final StringBuilder builder = new StringBuilder();
                boolean first = true;
                for (final PreparedStatementContext prepared : context.getPreparedStatementContexts().values()) {
                    if (!first) {
                        builder.append('\n');
                    } else {
                        first = false;
                    }
                    builder.append(prepared.getStatementId()).append(':').append(prepared.getSchema()).append(':')
                        .append(prepared.getPrepareSql());
                }
                row[10] = 0 == builder.length() ? null : builder.toString().getBytes(StandardCharsets.UTF_8);
                final FrontendTransactionContext transaction = context.getTransactionContext();
                builder.setLength(0);
                if (null != transaction) {
                    builder.append(transaction.getTrxId());
                    if (transaction.canTrxFreeIfNoReference()) {
                        builder.append(" (auto-free)");
                    } else {
                        builder.append(" (multi-stmt)");
                    }
                    BackendConnectionWrapper conn = transaction.getExistingRwConnection();
                    if (conn != null) {
                        final String tag = conn.probeBackendTag();
                        if (tag != null) {
                            builder.append(" rw: ").append(tag);
                        } else {
                            builder.append(" rw: <unknown>");
                        }
                    }
                    conn = transaction.getExistingRoConnection();
                    if (conn != null) {
                        final String tag = conn.probeBackendTag();
                        if (tag != null) {
                            builder.append(" ro: ").append(tag);
                        } else {
                            builder.append(" ro: <unknown>");
                        }
                    }
                    for (final Map.Entry<Integer, FrontendTransactionContext.ActiveBackendPreparedStatement> entry : transaction.getActiveBackendPreparedStatementMap()
                        .entrySet()) {
                        builder.append('\n').append(entry.getKey()).append(':')
                            .append(entry.getValue().getStatementId())
                            .append(':').append(entry.getValue().getSchema()).append(':')
                            .append(entry.getValue().getPrepareSql()).append('@')
                            .append(entry.getValue().getConnection().toString());
                    }
                }
                row[11] = 0 == builder.length() ? null : builder.toString().getBytes(StandardCharsets.UTF_8);
            } else {
                row[10] = ("count: " + context.getPreparedStatementContexts().size()).getBytes(StandardCharsets.UTF_8);
                final FrontendTransactionContext transaction = context.getTransactionContext();
                final StringBuilder builder = new StringBuilder();
                if (null != transaction) {
                    builder.append(transaction.getTrxId());
                    if (transaction.canTrxFreeIfNoReference()) {
                        builder.append(" (auto-free)");
                    } else {
                        builder.append(" (multi-stmt)");
                    }
                    BackendConnectionWrapper conn = transaction.getExistingRwConnection();
                    if (conn != null) {
                        final String tag = conn.probeBackendTag();
                        if (tag != null) {
                            builder.append(" rw: ").append(tag);
                        } else {
                            builder.append(" rw: <unknown>");
                        }
                    }
                    conn = transaction.getExistingRoConnection();
                    if (conn != null) {
                        final String tag = conn.probeBackendTag();
                        if (tag != null) {
                            builder.append(" ro: ").append(tag);
                        } else {
                            builder.append(" ro: <unknown>");
                        }
                    }
                    builder.append(", active ps count: ")
                        .append(transaction.getActiveBackendPreparedStatementMap().size());
                }
                row[11] = 0 == builder.length() ? null : builder.toString().getBytes(StandardCharsets.UTF_8);
            }
            final StringBuilder builder = new StringBuilder();
            builder.append("usr[");
            boolean first = true;
            for (final Map.Entry<CaseInsensitiveString, String> entry : context.getUserVariables().entrySet()) {
                if (!first) {
                    builder.append(',');
                } else {
                    first = false;
                }
                builder.append(entry.getKey()).append('=');
                final String val = entry.getValue();
                if (val.length() > 64) {
                    builder.append(val, 0, 64).append("...+").append(val.length() - 64);
                } else {
                    builder.append(val);
                }
            }
            builder.append("] sys[");
            first = true;
            for (final Map.Entry<CaseInsensitiveString, String> entry : context.getSystemVariables().entrySet()) {
                if (!first) {
                    builder.append(',');
                } else {
                    first = false;
                }
                builder.append(entry.getKey()).append('=');
                final String val = entry.getValue();
                if (val.length() > 64) {
                    builder.append(val, 0, 64).append("...+").append(val.length() - 64);
                } else {
                    builder.append(val);
                }
            }
            builder.append(']');
            row[12] = builder.toString().getBytes(StandardCharsets.UTF_8);

            consumer.accept(row);
        }
    }
}
