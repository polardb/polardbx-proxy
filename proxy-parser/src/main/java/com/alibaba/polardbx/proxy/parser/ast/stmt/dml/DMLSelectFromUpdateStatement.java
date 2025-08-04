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
 * (created at 2011-1-28)
 */
package com.alibaba.polardbx.proxy.parser.ast.stmt.dml;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ParamMarker;
import com.alibaba.polardbx.proxy.parser.ast.fragment.Limit;
import com.alibaba.polardbx.proxy.parser.ast.fragment.OrderBy;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.polardbx.proxy.parser.util.Pair;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author mengshi.sunmengshi 2015年1月20日 下午7:17:18
 * @since 5.1.0
 */
public class DMLSelectFromUpdateStatement extends DMLSelectStatement {

    public static enum SelectDuplicationStrategy {
        /**
         * default
         */
        ALL, DISTINCT, DISTINCTROW
    }

    public static enum QueryCacheStrategy {
        UNDEF, SQL_CACHE, SQL_NO_CACHE
    }

    public static enum SmallOrBigResult {
        UNDEF, SQL_SMALL_RESULT, SQL_BIG_RESULT
    }

    public static enum LockMode {
        UNDEF, FOR_UPDATE, LOCK_IN_SHARE_MODE
    }

    private final SelectFromUpdateOption option;
    /**
     * string: id | `id` | 'id'
     */
    private final List<Pair<Expression, String>> selectExprList;
    private final TableReferences tables;
    private final Expression where;
    private final OrderBy order;
    private final Limit limit;
    private List<Pair<Identifier, Expression>> values;
    private Identifier table;

    public static final class SelectFromUpdateOption {

        public boolean lowPriority = false;
        public boolean ignore = false;
        public boolean commitOnSuccess = false;
        public boolean rollbackOnFail = false;
        public boolean queueOnPk = false;
        public boolean targetAffectRow = false;
        public Number num = null;
        public ParamMarker numP = null;
        public Number queueOnPkNum = null;
        public ParamMarker queueOnPkNumP = null;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(getClass().getSimpleName()).append('{');
            sb.append(", ").append("lowPriority").append('=').append(lowPriority);
            sb.append(", ").append("ignore").append('=').append(ignore);
            sb.append(", ").append("commitOnSuccess").append('=').append(commitOnSuccess);
            sb.append(", ").append("rollbackOnFail").append('=').append(rollbackOnFail);
            sb.append(", ").append("queueOnPk").append('=').append(queueOnPk);
            sb.append(", ").append("targetAffectRow").append('=').append(targetAffectRow);
            sb.append(", ").append("queueOnPkNum").append('=').append(queueOnPkNum);
            sb.append(", ").append("queueOnPkNumP").append('=').append(queueOnPkNumP);
            sb.append(", ").append("num").append('=').append(num);
            sb.append(", ").append("numP").append('=').append(numP);
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public DMLSelectFromUpdateStatement(SelectFromUpdateOption option, List<Pair<Expression, String>> selectExprList,
                                        Identifier table, List<Pair<Identifier, Expression>> values, Expression where,
                                        OrderBy order, Limit limit) {

        super(new SelectOption(), selectExprList, null, null, null, null, order, limit);
        if (option == null) {
            throw new IllegalArgumentException("argument 'option' is null");
        }
        this.option = option;
        if (selectExprList == null || selectExprList.isEmpty()) {
            this.selectExprList = Collections.emptyList();
        } else {
            this.selectExprList = ensureListType(selectExprList);
        }
        this.tables = null;
        this.table = table;
        this.where = where;
        this.order = order;
        this.limit = limit;
        this.values = values;
    }

    public SelectFromUpdateOption getSelectFromUpdateOption() {
        return option;
    }

    /**
     * @return never null
     */
    @Override
    public List<Pair<Expression, String>> getSelectExprList() {
        return selectExprList;
    }

    /**
     *
     */
    @Override
    public List<Expression> getSelectExprListWithoutAlias() {
        if (selectExprList == null || selectExprList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Expression> list = new ArrayList<Expression>(selectExprList.size());
        for (Pair<Expression, String> p : selectExprList) {
            if (p != null && p.getKey() != null) {
                list.add(p.getKey());
            }
        }
        return list;
    }

    @Override
    public TableReferences getTables() {
        return tables;
    }

    @Override
    public Expression getWhere() {
        return where;
    }

    @Override
    public OrderBy getOrder() {
        return order;
    }

    @Override
    public Limit getLimit() {
        return limit;
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

    public List<Pair<Identifier, Expression>> getValues() {
        return values;
    }

    public Identifier getTable() {
        return table;
    }

}
