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
 * (created at 2011-5-21)
 */
package com.alibaba.polardbx.proxy.parser.ast.stmt.dal;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.fragment.VariableScope;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * @author QIU Shuo
 */
public class ShowVariables extends DALShowStatement {

    private final VariableScope scope;
    private final String pattern;
    private final Expression where;

    public ShowVariables(VariableScope scope, String pattern) {
        this.scope = scope;
        this.pattern = pattern;
        this.where = null;
    }

    public ShowVariables(VariableScope scope, Expression where) {
        this.scope = scope;
        this.pattern = null;
        this.where = where;
    }

    public ShowVariables(VariableScope scope) {
        this.scope = scope;
        this.pattern = null;
        this.where = null;
    }

    public VariableScope getScope() {
        return scope;
    }

    public String getPattern() {
        return pattern;
    }

    public Expression getWhere() {
        return where;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
