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
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.fragment.Limit;
import com.alibaba.polardbx.proxy.parser.ast.fragment.OrderBy;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * @author mengshi.sunmengshi 2014年5月9日 下午3:55:03
 * @author arnkore 2016-07-03
 * @since 5.1.0
 */
public class ShowTopology extends TddlShow {

    private Identifier name;

    private final Expression where;

    private final OrderBy orderBy;

    private final Limit limit;

    public ShowTopology(Identifier name) {
        this(name, null, null, null);
    }

    public ShowTopology(Identifier name, Expression where, OrderBy orderBy, Limit limit) {
        this.name = name;
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public Identifier getName() {
        return name;
    }

    public void setName(Identifier name) {
        this.name = name;
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
}
