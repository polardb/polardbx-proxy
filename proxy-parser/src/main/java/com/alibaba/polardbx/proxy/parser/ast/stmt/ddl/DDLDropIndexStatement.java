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
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.Algorithm;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.Algorithm.AlgorithmType;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.LockOperation;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.LockOperation.LockType;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * @author QIU Shuo
 */
public class DDLDropIndexStatement implements DDLStatement {

    private final Identifier indexName;
    private final Identifier table;
    private final Algorithm algorithm;
    private final LockOperation lock;

    public DDLDropIndexStatement(Identifier indexName, Identifier table, AlgorithmType algorithmType,
                                 LockType lockType) {
        this.indexName = indexName;
        this.table = table;
        this.algorithm = algorithmType != null ? new Algorithm(algorithmType) : null;
        this.lock = lockType != null ? new LockOperation(lockType) : null;
    }

    public Identifier getIndexName() {
        return indexName;
    }

    public Identifier getTable() {
        return table;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public LockOperation getLock() {
        return lock;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

}
