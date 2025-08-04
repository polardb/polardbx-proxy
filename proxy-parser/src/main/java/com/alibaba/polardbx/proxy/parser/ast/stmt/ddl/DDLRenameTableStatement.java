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
 * (created at 2011-7-5)
 */
package com.alibaba.polardbx.proxy.parser.ast.stmt.ddl;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.util.Pair;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author QIU Shuo
 */
public class DDLRenameTableStatement implements DDLStatement {

    private final List<Pair<Identifier, Identifier>> list;

    public DDLRenameTableStatement() {
        this.list = new ArrayList<Pair<Identifier, Identifier>>();
    }

    public DDLRenameTableStatement(List<Pair<Identifier, Identifier>> list) {
        if (list == null) {
            this.list = Collections.emptyList();
        } else {
            this.list = list;
        }
    }

    public DDLRenameTableStatement addRenamePair(Identifier from, Identifier to) {
        list.add(new Pair<Identifier, Identifier>(from, to));
        return this;
    }

    public List<Pair<Identifier, Identifier>> getList() {
        return list;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

}
