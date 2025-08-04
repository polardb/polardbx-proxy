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
 * (created at 2011-1-21)
 */
package com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal;

import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.Map;

/**
 * @author QIU Shuo
 */
public class LiteralBoolean extends Literal {

    public static final Integer TRUE = new Integer(1);
    public static final Integer FALSE = new Integer(0);
    private final boolean value;

    public LiteralBoolean(boolean value) {
        super();
        this.value = value;
    }

    public boolean isTrue() {
        return value;
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return value ? TRUE : FALSE;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
