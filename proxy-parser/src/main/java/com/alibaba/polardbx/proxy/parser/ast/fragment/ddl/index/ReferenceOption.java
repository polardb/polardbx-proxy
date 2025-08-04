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
package com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index;

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao on 15-3-18.
 */
public class ReferenceOption implements ASTNode {

    public static enum OnType {
        ON_DELETE, ON_UPDATE
    }

    public static enum ReferenceOptionType {
        RESTRICT, CASCADE, SET_NULL, NO_ACTION
    }

    private final OnType onType;
    private final ReferenceOptionType referenceOptionType;

    public OnType getOnType() {
        return onType;
    }

    public ReferenceOptionType getReferenceOptionType() {
        return referenceOptionType;
    }

    public ReferenceOption(OnType onType, ReferenceOptionType referenceOptionType) {
        this.onType = onType;
        this.referenceOptionType = referenceOptionType;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
