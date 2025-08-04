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
 * (created at 2011-7-20)
 */
package com.alibaba.polardbx.proxy.parser.ast.expression.arithmeic;

import com.alibaba.polardbx.proxy.parser.ast.expression.BinaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.util.BinaryOperandCalculator;
import com.alibaba.polardbx.proxy.parser.util.ExprEvalUtils;
import com.alibaba.polardbx.proxy.parser.util.Pair;

import java.util.Map;

/**
 * @author QIU Shuo
 */
public abstract class ArithmeticBinaryOperatorExpression extends BinaryOperatorExpression
    implements BinaryOperandCalculator {

    protected ArithmeticBinaryOperatorExpression(Expression leftOprand, Expression rightOprand, int precedence) {
        super(leftOprand, rightOprand, precedence, true);
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
        Pair<Number, Number> pair = ExprEvalUtils.convertNum2SameLevel(left, right);
        return ExprEvalUtils.calculate(this, pair.getKey(), pair.getValue());
    }

}
