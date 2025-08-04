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
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.ColumnDefinition;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao on 14-12-10.
 */
// | CHANGE [COLUMN] old_col_name new_col_name column_definition
// [FIRST|AFTER col_name]
public class ChangeColumn implements AlterSpecification {

    private final Identifier       oldName;
    private final Identifier       newName;
    private final ColumnDefinition colDef;
    private final boolean          first;
    private final Identifier       afterColumn;

    public ChangeColumn(Identifier oldName, Identifier newName, ColumnDefinition colDef, Identifier afterColumn){
        this.oldName = oldName;
        this.newName = newName;
        this.colDef = colDef;
        this.first = afterColumn == null;
        this.afterColumn = afterColumn;
    }

    /**
     * without column position specification
     */
    public ChangeColumn(Identifier oldName, Identifier newName, ColumnDefinition colDef){
        this.oldName = oldName;
        this.newName = newName;
        this.colDef = colDef;
        this.first = false;
        this.afterColumn = null;
    }

    public Identifier getOldName() {
        return oldName;
    }

    public Identifier getNewName() {
        return newName;
    }

    public ColumnDefinition getColDef() {
        return colDef;
    }

    public boolean isFirst() {
        return first;
    }

    public Identifier getAfterColumn() {
        return afterColumn;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
