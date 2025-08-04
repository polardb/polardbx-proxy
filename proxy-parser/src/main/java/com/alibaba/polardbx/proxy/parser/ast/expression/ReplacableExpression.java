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
 * (created at 2011-8-1)
 */
package com.alibaba.polardbx.proxy.parser.ast.expression;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralBoolean;

/**
 * @author QIU Shuo
 */
public interface ReplacableExpression extends Expression {

    LiteralBoolean BOOL_FALSE = new LiteralBoolean(false);

    void setReplaceExpr(Expression replaceExpr);

    void clearReplaceExpr();
}
