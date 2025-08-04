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
 * (created at 2011-1-20)
 */
package com.alibaba.polardbx.proxy.parser.ast.expression.type;

import com.alibaba.polardbx.proxy.parser.ast.expression.AbstractExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.Map;

/**
 * <code>higherExpr 'COLLATE' collateName</code>
 *
 * @author QIU Shuo
 */
public class CollateExpression extends AbstractExpression {

    private final String collateName;
    private final Expression string;

    public CollateExpression(Expression string, String collateName) {
        if (collateName == null) {
            throw new IllegalArgumentException("collateName is null");
        }
        this.string = string;
        this.collateName = collateName;
    }

    public String getCollateName() {
        return collateName;
    }

    public Expression getString() {
        return string;
    }

    @Override
    public int getPrecedence() {
        return PRECEDENCE_COLLATE;
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return string.evaluation(parameters);
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
