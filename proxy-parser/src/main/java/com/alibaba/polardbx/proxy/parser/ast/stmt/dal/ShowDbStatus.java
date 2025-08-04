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
package com.alibaba.polardbx.proxy.parser.ast.stmt.dal;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.fragment.Limit;
import com.alibaba.polardbx.proxy.parser.ast.fragment.OrderBy;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * @author chenmo.cm
 * @since 5.1.0
 */
public class ShowDbStatus extends DALShowStatement {

    private String pattern;
    private final Expression where;
    private final OrderBy orderBy;
    private final Limit limit;
    private final boolean full;

    public ShowDbStatus(String pattern, boolean full) {
        this.pattern = pattern;
        this.where = null;
        this.orderBy = null;
        this.limit = null;
        this.full = full;
    }

    public ShowDbStatus(Expression where, OrderBy orderBy, Limit limit, boolean full) {
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.full = full;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public Expression getWhere() {
        return where;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public Limit getLimit() {
        return limit;
    }

    public boolean isFull() {
        return full;
    }
}
