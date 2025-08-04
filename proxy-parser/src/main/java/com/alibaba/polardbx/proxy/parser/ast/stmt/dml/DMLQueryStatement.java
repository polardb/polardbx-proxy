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
 * (created at 2011-6-17)
 */
package com.alibaba.polardbx.proxy.parser.ast.stmt.dml;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.misc.QueryExpression;

import java.util.Map;

/**
 * @author QIU Shuo
 */
public abstract class DMLQueryStatement extends DMLStatement implements QueryExpression {

    private boolean explain = false;

    @Override
    public int getPrecedence() {
        return PRECEDENCE_QUERY;
    }

    @Override
    public Expression setCacheEvalRst(boolean cacheEvalRst) {
        return this;
    }

    @Override
    public Object evaluation(Map<? extends Object, ? extends Object> parameters) {
        return UNEVALUATABLE;
    }

    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    public boolean isExplain() {
        return this.explain;
    }
}
