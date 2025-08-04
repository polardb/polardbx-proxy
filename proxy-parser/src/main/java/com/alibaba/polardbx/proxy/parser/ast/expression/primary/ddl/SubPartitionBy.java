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
package com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl;

import java.util.List;

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao on 15-5-12.
 */
public class SubPartitionBy implements ASTNode {

    public SubPartitionByType getSubPartitionByType() {
        return subPartitionByType;
    }

    public void setSubPartitionByType(SubPartitionByType subPartitionByType) {
        this.subPartitionByType = subPartitionByType;
    }

    public boolean isLiner() {
        return liner;
    }

    public void setLiner(boolean liner) {
        this.liner = liner;
    }

    public Expression getHashExpr() {
        return hashExpr;
    }

    public void setHashExpr(Expression hashExpr) {
        this.hashExpr = hashExpr;
    }

    public LiteralNumber getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(LiteralNumber algorithm) {
        this.algorithm = algorithm;
    }

    public List<Identifier> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Identifier> columnList) {
        this.columnList = columnList;
    }

    public LiteralNumber getNum() {
        return num;
    }

    public void setNum(LiteralNumber num) {
        this.num = num;
    }

    public static enum SubPartitionByType {
        HASH, KEY
    }

    private SubPartitionByType subPartitionByType;

    // HASH, KEY
    private boolean            liner;

    // HASH
    private Expression         hashExpr;

    // KEY
    private LiteralNumber      algorithm;         // 1|2
    private List<Identifier>   columnList;        // like (c1,c2)

    // ------SUBPARTITIONS-----optional-----------
    private LiteralNumber      num;

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
