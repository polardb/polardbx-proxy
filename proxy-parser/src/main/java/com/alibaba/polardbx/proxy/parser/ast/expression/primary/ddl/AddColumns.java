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

import java.util.ArrayList;
import java.util.List;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.ColumnDefinition;
import com.alibaba.polardbx.proxy.parser.util.Pair;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao on 14-12-10.
 */
// | ADD [COLUMN] (col_name column_definition,...)
public class AddColumns implements AlterSpecification {

    private final List<Pair<Identifier, ColumnDefinition>> columns;

    public AddColumns(){
        this.columns = new ArrayList<Pair<Identifier, ColumnDefinition>>(2);
    }

    public AddColumns addColumn(Identifier name, ColumnDefinition colDef) {
        this.columns.add(new Pair<Identifier, ColumnDefinition>(name, colDef));
        return this;
    }

    public List<Pair<Identifier, ColumnDefinition>> getColumns() {
        return columns;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
