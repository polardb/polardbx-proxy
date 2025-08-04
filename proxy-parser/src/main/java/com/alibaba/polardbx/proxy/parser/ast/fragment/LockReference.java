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
package com.alibaba.polardbx.proxy.parser.ast.fragment;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.LockTablesStatement.LockType;

public class LockReference {

    private Identifier table;
    private Identifier alias;
    private LockType lockType;

    public LockReference(Identifier table, Identifier alias, LockType lockType) {
        super();
        this.table = table;
        this.alias = alias;
        this.lockType = lockType;
    }

    public Identifier getTable() {
        return table;
    }

    public Identifier getAlias() {
        return alias;
    }

    public LockType getLockType() {
        return lockType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table:").append(String.valueOf(this.table)).append(",");
        sb.append("alias:").append(String.valueOf(this.alias)).append(",");
        sb.append("lock_type:").append(String.valueOf(this.lockType));

        return sb.toString();
    }

}
