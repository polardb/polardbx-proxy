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
 * (created at 2012-8-13)
 */
package com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index;

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.Collections;
import java.util.List;

/**
 * @author QIU Shuo
 */
public class IndexDefinition implements ASTNode {

    public Identifier getUniqueConstraint() {
        return uniqueConstraint;
    }

    public void setUniqueConstraint(Identifier uniqueConstraint) {
        this.uniqueConstraint = uniqueConstraint;
    }

    public Identifier getForeignKeyConstraint() {
        return foreignKeyConstraint;
    }

    public void setForeignKeyConstraint(Identifier foreignKeyConstraint) {
        this.foreignKeyConstraint = foreignKeyConstraint;
    }

    public IndexDefinition getForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(IndexDefinition foreignKey) {
        this.foreignKey = foreignKey;
    }

    public ReferenceDefinition getForeignKeyReferenceDefinition() {
        return foreignKeyReferenceDefinition;
    }

    public void setForeignKeyReferenceDefinition(ReferenceDefinition foreignKeyReferenceDefinition) {
        this.foreignKeyReferenceDefinition = foreignKeyReferenceDefinition;
    }

    public boolean isHasConstraint() {
        return hasConstraint;
    }

    public void setHasConstraint(boolean hasConstraint) {
        this.hasConstraint = hasConstraint;
    }

    public static enum IndexType {
        BTREE, HASH
    }

    private final IndexType indexType;
    private final List<IndexColumnName> columns;
    private final List<IndexOption> options;

    private Identifier uniqueConstraint = null;
    private Identifier foreignKeyConstraint = null;
    private IndexDefinition foreignKey;
    private ReferenceDefinition foreignKeyReferenceDefinition;
    /* CONSTRAINT可以没有symbol */
    private boolean hasConstraint;

    @SuppressWarnings("unchecked")
    public IndexDefinition(IndexType indexType, List<IndexColumnName> columns, List<IndexOption> options) {
        this.indexType = indexType;
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns is null or empty");
        }
        this.columns = columns;
        this.options = (List<IndexOption>) (options == null || options.isEmpty() ? Collections.emptyList() : options);
    }

    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * @return never null
     */
    public List<IndexColumnName> getColumns() {
        return columns;
    }

    /**
     * @return never null
     */
    public List<IndexOption> getOptions() {
        return options;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

}
