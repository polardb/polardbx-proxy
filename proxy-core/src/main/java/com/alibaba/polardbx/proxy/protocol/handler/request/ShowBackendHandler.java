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

import com.alibaba.polardbx.proxy.connection.BackendConnection;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.utils.CaseInsensitiveString;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ShowBackendHandler extends SystemTableRequestHandler {
    private final boolean full;

    public ShowBackendHandler(FrontendContext context, boolean full) {
        super(context);
        this.full = full;
        setTag("ShowBackendHandler");
    }

    public static final ColumnDefinition41[] fields = new ColumnDefinition41[] {
        new ColumnDefinition41().fieldLong("id".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldVarchar("user".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("capabilities".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("privilege_host".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("connection".getBytes(StandardCharsets.UTF_8),
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
        new ColumnDefinition41().fieldVarchar("idle_prepared_statement".getBytes(StandardCharsets.UTF_8),
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
        for (final BackendConnection connection : BackendConnection.CONNECTIONS) {
            final BackendContext context = connection.getContextReference().getAcquire();
            if (null == context) {
                continue;
            }

            // build row
            final byte[][] row = new byte[fields.length][];
            row[0] = (context.getRemoteAddress().getHostString() + ':' + context.getRemoteAddress().getPort() + '-'
                + context.getConnectionId()).getBytes(StandardCharsets.UTF_8);
            row[1] = null == context.getUsername() ? null : context.getUsername().getBytes(StandardCharsets.UTF_8);
            row[2] = ("0x" + Integer.toHexString(context.getCapabilities())).getBytes(StandardCharsets.UTF_8);
            row[3] = null == context.getPrivilegeHost() ?
                null : context.getPrivilegeHost().getBytes(StandardCharsets.UTF_8);
            row[4] = connection.connectionString().getBytes(StandardCharsets.UTF_8);
            row[5] = null == context.getDatabase() ? null : context.getDatabase().getBytes(StandardCharsets.UTF_8);
            row[6] = context.getState().name().getBytes(StandardCharsets.UTF_8);
            row[7] = CharsetMapping.getStaticCollationNameForCollationIndex(context.getClientCharsetIndex())
                .getBytes(StandardCharsets.UTF_8);
            row[8] = CharsetMapping.getStaticCollationNameForCollationIndex(context.getConnectionCharsetIndex())
                .getBytes(StandardCharsets.UTF_8);
            row[9] = CharsetMapping.getStaticCollationNameForCollationIndex(context.getResultsCharsetIndex())
                .getBytes(StandardCharsets.UTF_8);
            if (full) {
                final String ps = context.showPreparedStatement();
                row[10] = null == ps ? null : ps.getBytes(StandardCharsets.UTF_8);
            } else {
                row[10] = ("count: " + context.countPreparedStatement()).getBytes(StandardCharsets.UTF_8);
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
            row[11] = builder.toString().getBytes(StandardCharsets.UTF_8);

            consumer.accept(row);
        }
    }
}
