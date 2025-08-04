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

import lombok.Getter;

import java.util.Objects;

@Getter
public class ServerPreparedStatementKey {
    private final String schema;
    private final String prepareSql;

    public ServerPreparedStatementKey(String schema, String prepareSql) {
        this.schema = schema;
        this.prepareSql = prepareSql;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ServerPreparedStatementKey that = (ServerPreparedStatementKey) o;
        return Objects.equals(prepareSql, that.prepareSql) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, prepareSql);
    }

    @Override
    public String toString() {
        return "ServerPreparedStatementKey{" +
            "schema='" + schema + '\'' +
            ", prepareSql='" + prepareSql + '\'' +
            '}';
    }
}
