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
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * 用在FOREIGN KEY中 reference_definition: REFERENCES tbl_name (index_col_name,...)
 * [MATCH FULL | MATCH PARTIAL | MATCH SIMPLE] [ON DELETE reference_option] [ON
 * UPDATE reference_option] Created by simiao on 15-2-12.
 */
public class ReferenceDefinition implements ASTNode {

    public Identifier getTblName() {
        return tblName;
    }

    public void setTblName(Identifier identifier) {
        this.tblName = identifier;
    }

    public List<IndexColumnName> getColumns() {
        return columns;
    }

    public MatchType getMatchType() {
        return matchType;
    }

    public List<ReferenceOption> getReferenceOptions() {
        return referenceOptions;
    }

    public static enum MatchType {
        MATCH_FULL, MATCH_PARTIAL, MATCH_SIMPLE
    }

    /* 只有名字是可变的，为了后面替换成物理名字用 */
    private Identifier tblName;
    private final List<IndexColumnName> columns;

    private final MatchType matchType;
    private final List<ReferenceOption> referenceOptions;

    public ReferenceDefinition(Identifier tblName, List<IndexColumnName> columns, MatchType matchType,
                               List<ReferenceOption> referenceOptions) {
        if (tblName == null) {
            throw new IllegalArgumentException("tblname is null or empty");
        }
        this.tblName = tblName;
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns is null or empty");
        }
        this.columns = columns;
        this.matchType = matchType;
        this.referenceOptions = referenceOptions;
    }

    public ReferenceDefinition copyself() {
        return new ReferenceDefinition(tblName, new ArrayList<IndexColumnName>(columns), matchType, referenceOptions);
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
