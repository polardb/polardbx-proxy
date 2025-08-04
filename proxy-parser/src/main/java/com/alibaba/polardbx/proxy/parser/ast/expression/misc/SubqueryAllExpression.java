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
package com.alibaba.polardbx.proxy.parser.ast.expression.misc;

import com.alibaba.polardbx.proxy.parser.ast.expression.BinaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * <code>'ALL' '(' subquery  ')'</code>
 *
 * @author QIU Shuo
 */
public class SubqueryAllExpression extends BinaryOperatorExpression {

    private MySQLToken op;

    public SubqueryAllExpression(Expression fst, QueryExpression subquery, MySQLToken op) {
        super(fst, subquery, PRECEDENCE_COMPARISION);
        this.op = op;
    }

    @Override
    public String getOperator() {
        return getNameFromOperator(op) + " ALL";
    }

    public static String getNameFromOperator(MySQLToken op) {
        switch (op) {
        case OP_GREATER_THAN:
            return ">";
        case OP_GREATER_OR_EQUALS:
            return ">=";
        case OP_EQUALS:
            return "=";
        case OP_LESS_THAN:
            return "<";
        case OP_LESS_OR_EQUALS:
            return "<=";
        case OP_LESS_OR_GREATER:
            return "<>";
        default:
            throw new RuntimeException("not support this operator:" + op);
        }
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
