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
/**
 * (created at 2011-6-8)
 */
package com.alibaba.polardbx.proxy.parser.ast.stmt.mts;

import com.alibaba.polardbx.proxy.parser.ast.fragment.VariableScope;
import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * @author QIU Shuo
 */
public class MTSSetTransactionStatement implements SQLStatement {

    public static enum IsolationLevel {
        READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
    }

    public static enum AccessMode {
        READ_ONLY, READ_WRITE
    }

    private final VariableScope scope;
    private final IsolationLevel level;
    private final AccessMode accessMode;

    public MTSSetTransactionStatement(VariableScope scope, IsolationLevel level) {
        super();
        if (level == null) {
            throw new IllegalArgumentException("isolation level is null");
        }
        this.level = level;
        this.scope = scope;
        this.accessMode = null;
    }

    public MTSSetTransactionStatement(VariableScope scope, AccessMode accessMode) {
        super();
        if (accessMode == null) {
            throw new IllegalArgumentException("access mode is null");
        }
        this.accessMode = accessMode;
        this.level = null;
        this.scope = scope;
    }

    public VariableScope getScope() {
        return scope;
    }

    public IsolationLevel getLevel() {
        return level;
    }

    public AccessMode getAccessMode() {
        return accessMode;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
