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
package com.alibaba.polardbx.proxy.parser.ast.stmt.tms;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DDLStatement;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.Collections;
import java.util.List;

/**
 * @author 梦实 2017年8月24日 下午7:31:32
 * @since 5.0.0
 */
public class TMSOptimizeTableStatement implements DDLStatement {

    public static enum Mode {
        NO_WRITE_TO_BINLOG, LOCAL
    }

    private final List<Identifier> tableNames;
    private final Mode mode;

    public TMSOptimizeTableStatement(List<Identifier> tableNames) {
        this(tableNames, null);
    }

    public TMSOptimizeTableStatement(List<Identifier> tableNames, Mode mode) {
        if (tableNames == null || tableNames.isEmpty()) {
            this.tableNames = Collections.emptyList();
        } else {
            this.tableNames = tableNames;
        }

        this.mode = mode;
    }

    public List<Identifier> getTableNames() {
        return tableNames;
    }

    public Mode getMode() {
        return mode;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

}
