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
package com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.IndexDefinition;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao on 14-12-10.
 */
// | ADD [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...)
// [index_option] ...
public class AddPrimaryKey implements AlterSpecification {

    private final Identifier      constraint;
    private final IndexDefinition indexDef;

    public AddPrimaryKey(IndexDefinition indexDef, Identifier constraint){
        this.indexDef = indexDef;
        this.constraint = constraint;
    }

    public IndexDefinition getIndexDef() {
        return indexDef;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public Identifier getConstraint() {
        return constraint;
    }
}
