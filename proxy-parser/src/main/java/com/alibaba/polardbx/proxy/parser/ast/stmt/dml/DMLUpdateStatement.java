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
 * (created at 2011-5-19)
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
 * @author QIU Shuo
 */
public class DMLUpdateStatement extends DMLStatement {

    private final boolean lowPriority;
    private final boolean ignore;
    private final TableReferences tableRefs;
    private final List<Pair<Identifier, Expression>> values;
    private final Expression where;
    private final OrderBy orderBy;
    private final Limit limit;
    private final boolean commitOnSuccess;
    private final boolean rollbackOnFail;
    private final boolean queueOnPk;
    private final boolean targetAffectRow;
    private final Number num;
    private final ParamMarker numP;
    private final ParamMarker queueOnPkNumP;
    private final Number queueOnPkNum;

    public DMLUpdateStatement(boolean lowPriority, boolean ignore, TableReferences tableRefs,
                              List<Pair<Identifier, Expression>> values, Expression where, OrderBy orderBy,
                              Limit limit, boolean commitOnSuccess, boolean rollbackOnFail, boolean queueOnPk,
                              boolean targetAffectRow, Number queueOnPkNum, ParamMarker queueOnPkNumP, Number num,
                              ParamMarker numP) {
        this.lowPriority = lowPriority;
        this.ignore = ignore;
        if (tableRefs == null) {
            throw new IllegalArgumentException("argument tableRefs is null for update stmt");
        }
        this.tableRefs = tableRefs;
        if (values == null || values.size() <= 0) {
            this.values = Collections.emptyList();
        } else if (!(values instanceof ArrayList)) {
            this.values = new ArrayList<Pair<Identifier, Expression>>(values);
        } else {
            this.values = values;
        }
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.commitOnSuccess = commitOnSuccess;
        this.rollbackOnFail = rollbackOnFail;
        this.queueOnPk = queueOnPk;
        this.targetAffectRow = targetAffectRow;
        this.num = num;
        this.numP = numP;
        this.queueOnPkNum = queueOnPkNum;
        this.queueOnPkNumP = queueOnPkNumP;
    }

    public boolean isLowPriority() {
        return lowPriority;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public TableReferences getTableRefs() {
        return tableRefs;
    }

    public List<Pair<Identifier, Expression>> getValues() {
        return values;
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

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public boolean isCommitOnSuccess() {
        return commitOnSuccess;
    }

    public boolean isRollbackOnFail() {
        return rollbackOnFail;
    }

    public boolean isQueueOnPk() {
        return queueOnPk;
    }

    public boolean isTargetAffectRow() {
        return targetAffectRow;
    }

    public Number getNum() {
        return num;
    }

    public ParamMarker getNumP() {
        return numP;
    }

    public ParamMarker getQueueOnPkNumP() {
        return queueOnPkNumP;
    }

    public Number getQueueOnPkNum() {
        return queueOnPkNum;
    }

}
