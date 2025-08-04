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

package com.alibaba.polardbx.proxy.context.help;

import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.protocol.handler.result.StmtPrepareResultHandler;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtPrepareOk;
import com.alibaba.polardbx.proxy.protocol.prepare.ParameterRebind;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Getter
public class PreparedStatementContext {
    private final int statementId;
    private final String schema;
    private final String prepareSql;

    // infos for this prepared statement
    private final ComStmtPrepareOk ok;
    private final List<ColumnDefinition41> parameters, fields;

    // rebind info
    private final AtomicReference<ParameterRebind[]> rebindParameters = new AtomicReference<>();

    // long data param flags
    private final byte[][] longDataParams;

    public PreparedStatementContext(int statementId, String schema, String prepareSql,
                                    StmtPrepareResultHandler handler) {
        this.statementId = statementId;
        this.schema = schema;
        this.prepareSql = prepareSql;
        this.ok = handler.getOk();
        this.parameters = handler.getParameters();
        this.fields = handler.getFields();
        this.longDataParams = new byte[null == parameters ? 0 : parameters.size()][];
    }

    public ServerPreparedStatementKey getKey() {
        return new ServerPreparedStatementKey(schema, prepareSql);
    }

    public void clearLongDataParams() {
        Arrays.fill(longDataParams, null);
    }

    @Override
    public String toString() {
        return "PreparedStatementContext{" +
            "statementId=" + statementId +
            ", schema='" + schema + '\'' +
            ", prepareSql='" + prepareSql + '\'' +
            ", ok=" + ok +
            ", parameters=" + parameters +
            ", fields=" + fields +
            '}';
    }
}
