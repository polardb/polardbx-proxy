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
 * (created at 2011-1-29)
 */
package com.alibaba.polardbx.proxy.parser.ast.stmt.dml;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.fragment.Limit;
import com.alibaba.polardbx.proxy.parser.ast.fragment.OrderBy;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author QIU Shuo
 */
public class DMLSelectUnionStatement extends DMLQueryStatement {

    /**
     * might be {@link LinkedList}
     */
    private final List<DMLSelectStatement> selectStmtList;
    /**
     * <code>Mixed UNION types are treated such that a DISTINCT union overrides any ALL union to its left</code>
     * <br/>
     * 0 means all relations of selects are union all<br/>
     * last index of {@link #selectStmtList} means all relations of selects are
     * union distinct<br/>
     */
    private int firstDistinctIndex = 0;
    private List<UnionOption> options;
    private OrderBy orderBy;
    private Limit limit;

    public static enum UnionOption {
        DEFAULT, ALL, DISTINCT;
    }

    public DMLSelectUnionStatement(DMLSelectStatement select) {
        super();
        this.selectStmtList = new ArrayList<DMLSelectStatement>();
        this.selectStmtList.add(select);
        this.options = new ArrayList<UnionOption>();
    }

    public DMLSelectUnionStatement addSelect(DMLSelectStatement select, UnionOption option) {
        selectStmtList.add(select);
        if (option != UnionOption.ALL) {
            firstDistinctIndex = selectStmtList.size() - 1;
        }
        this.options.add(option);
        return this;
    }

    public DMLSelectUnionStatement setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    public DMLSelectUnionStatement setLimit(Limit limit) {
        this.limit = limit;
        return this;
    }

    public List<DMLSelectStatement> getSelectStmtList() {
        return selectStmtList;
    }

    public int getFirstDistinctIndex() {
        return firstDistinctIndex;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public Limit getLimit() {
        return limit;
    }

    public List<UnionOption> getOptions() {
        return options;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    private String originStr;

    @Override
    public String getOriginStr() {
        return originStr;
    }

    @Override
    public Expression setOriginStr(String str) {
        this.originStr = str;
        return this;
    }
}
