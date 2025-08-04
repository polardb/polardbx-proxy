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
package com.alibaba.polardbx.proxy.parser.ast.expression.primary;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * @author wuheng.zxy 2016-5-22 上午12:23:48
 */
public class JsonExtractExpression extends PrimaryExpression {

    /**
     * @return the leftOprand
     */
    public Expression getLeftOprand() {
        return leftOprand;
    }

    /**
     * @return the rightOprand
     */
    public Expression getRightOprand() {
        return rightOprand;
    }

    private boolean unquote;
    protected final Expression leftOprand;
    protected final Expression rightOprand;

    public JsonExtractExpression(Expression leftOprand, Expression rightOprand, boolean unquote) {
        super();
        this.setUnquote(unquote);
        this.leftOprand = leftOprand;
        this.rightOprand = rightOprand;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public String getOperator() {
        if (unquote) {
            return "->>";
        }
        return "->";
    }

    /**
     * @return the unquote
     */
    public boolean isUnquote() {
        return unquote;
    }

    /**
     * @param unquote the unquote to set
     */
    public void setUnquote(boolean unquote) {
        this.unquote = unquote;
    }

}
