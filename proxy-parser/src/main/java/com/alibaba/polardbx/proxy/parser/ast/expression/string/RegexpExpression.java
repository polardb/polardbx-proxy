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
 * (created at 2011-1-19)
 */
package com.alibaba.polardbx.proxy.parser.ast.expression.string;

import com.alibaba.polardbx.proxy.parser.ast.expression.BinaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * <code>higherPreExpr 'NOT'? ('REGEXP'|'RLIKE') higherPreExp</code>
 *
 * @author QIU Shuo
 */
public class RegexpExpression extends BinaryOperatorExpression {

    private final boolean not;

    public RegexpExpression(boolean not, Expression comparee, Expression pattern) {
        super(comparee, pattern, PRECEDENCE_COMPARISION);
        this.not = not;
    }

    public boolean isNot() {
        return not;
    }

    @Override
    public String getOperator() {
        return not ? "NOT REGEXP" : "REGEXP";
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
