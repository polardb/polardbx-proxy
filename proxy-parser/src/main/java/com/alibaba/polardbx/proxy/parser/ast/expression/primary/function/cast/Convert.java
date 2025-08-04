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
 * (created at 2011-1-23)
 */
package com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.cast;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.FunctionExpression;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.List;

/**
 * @author QIU Shuo
 */
public class Convert extends FunctionExpression {

    /**
     * Either {@link transcodeName} or {@link typeName} is null
     */
    private final String transcodeName;
    private final String typeName;
    private final Expression typeInfo1;
    private final Expression typeInfo2;

    public Convert(Expression arg, String transcodeName, String typeName, Expression typeInfo1, Expression typeInfo2) {
        super("CONVERT", wrapList(arg));
        if (null == typeName && null == transcodeName) {
            throw new IllegalArgumentException("typeName and transcodeName is null");
        }
        this.typeName = typeName;
        this.typeInfo1 = typeInfo1;
        this.typeInfo2 = typeInfo2;
        this.transcodeName = transcodeName;
    }

    /**
     * @return never null
     */
    public Expression getExpr() {
        return getArguments().get(0);
    }

    public String getTranscodeName() {
        return transcodeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public Expression getTypeInfo1() {
        return typeInfo1;
    }

    public Expression getTypeInfo2() {
        return typeInfo2;
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        throw new UnsupportedOperationException("function of char has special arguments");
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
