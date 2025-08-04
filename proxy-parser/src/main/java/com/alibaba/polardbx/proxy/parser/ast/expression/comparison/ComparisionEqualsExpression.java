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
package com.alibaba.polardbx.proxy.parser.ast.expression.comparison;

import com.alibaba.polardbx.proxy.parser.ast.expression.BinaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.ReplacableExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralBoolean;
import com.alibaba.polardbx.proxy.parser.util.ExprEvalUtils;
import com.alibaba.polardbx.proxy.parser.util.Pair;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.Map;

/**
 * <code>higherPreExpr '=' higherPreExpr</code>
 *
 * @author QIU Shuo
 */
public class ComparisionEqualsExpression extends BinaryOperatorExpression implements ReplacableExpression {

    public ComparisionEqualsExpression(Expression leftOprand, Expression rightOprand) {
        super(leftOprand, rightOprand, PRECEDENCE_COMPARISION);
    }

    @Override
    public String getOperator() {
        return "=";
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        Object left = leftOprand.evaluation(parameters);
        Object right = rightOprand.evaluation(parameters);
        if (left == null || right == null) {
            return null;
        }
        if (left == UNEVALUATABLE || right == UNEVALUATABLE) {
            return UNEVALUATABLE;
        }
        if (left instanceof Number || right instanceof Number) {
            Pair<Number, Number> pair = ExprEvalUtils.convertNum2SameLevel(left, right);
            left = pair.getKey();
            right = pair.getValue();
        }
        return left.equals(right) ? LiteralBoolean.TRUE : LiteralBoolean.FALSE;
    }

    private Expression replaceExpr;

    @Override
    public void setReplaceExpr(Expression replaceExpr) {
        this.replaceExpr = replaceExpr;
    }

    @Override
    public void clearReplaceExpr() {
        this.replaceExpr = null;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        if (replaceExpr == null) {
            visitor.visit(this);
        } else {
            replaceExpr.accept(visitor);
        }
    }
}
