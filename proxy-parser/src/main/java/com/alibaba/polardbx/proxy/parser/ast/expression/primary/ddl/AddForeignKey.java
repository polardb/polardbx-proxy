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
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.ReferenceDefinition;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * ADD [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name,...)
 * reference_definition Created by simiao on 15-2-12.
 */
public class AddForeignKey implements AlterSpecification {

    private final Identifier          constraint;
    private final IndexDefinition     indexDef;
    private final ReferenceDefinition referenceDefinition;
    private final Identifier          indexName;

    public AddForeignKey(IndexDefinition indexDef, ReferenceDefinition referenceDefinition, Identifier constraint,
                         Identifier indexName){
        this.indexDef = indexDef;
        this.referenceDefinition = referenceDefinition;
        this.constraint = constraint;
        this.indexName = indexName;
    }

    public IndexDefinition getIndexDef() {
        return indexDef;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public ReferenceDefinition getReferenceDefinition() {
        return referenceDefinition;
    }

    public AddForeignKey copyself() {
        return new AddForeignKey(indexDef, referenceDefinition.copyself(), constraint, indexName);
    }

    public Identifier getConstraint() {
        return constraint;
    }

    public Identifier getIndexName() {
        return indexName;
    }
}
