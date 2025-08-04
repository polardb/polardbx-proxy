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
 * (created at 2011-7-4)
 */
package com.alibaba.polardbx.proxy.parser.ast.stmt.ddl;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AlterSpecification;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DBPartitionOptions;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.TableOptions;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * NOT FULL AST: partition options, foreign key, ORDER BY not supported
 *
 * @author QIU Shuo
 */
public class DDLAlterTableStatement implements DDLStatement {

    // | DISABLE KEYS
    // | ENABLE KEYS
    // | RENAME [TO] new_tbl_name
    // | ORDER BY col_name [, col_name] ...
    // | CONVERT TO CHARACTER SET charset_name [COLLATE collation_name]
    // | DISCARD TABLESPACE
    // | IMPORT TABLESPACE
    // /// | ADD [CONSTRAINT [symbol]] FOREIGN KEY [index_name]
    // (index_col_name,...) reference_definition
    // /// | DROP FOREIGN KEY fk_symbol
    // /// | ADD DBPARTITION (partition_definition)
    // /// | DROP DBPARTITION partition_names
    // /// | TRUNCATE DBPARTITION {partition_names | ALL }
    // /// | COALESCE DBPARTITION number
    // /// | REORGANIZE DBPARTITION partition_names INTO (partition_definitions)
    // /// | ANALYZE DBPARTITION {partition_names | ALL }
    // /// | CHECK DBPARTITION {partition_names | ALL }
    // /// | OPTIMIZE DBPARTITION {partition_names | ALL }
    // /// | REBUILD DBPARTITION {partition_names | ALL }
    // /// | REPAIR DBPARTITION {partition_names | ALL }
    // /// | REMOVE PARTITIONING

    // ADD, ALTER, DROP, and CHANGE can be multiple

    private final boolean ignore;
    private final Identifier table;
    private TableOptions tableOptions;
    private final List<AlterSpecification> alters;

    private DBPartitionOptions DBPartitionOptions;

    public DDLAlterTableStatement(boolean ignore, Identifier table) {
        this.ignore = ignore;
        this.table = table;
        this.alters = new ArrayList<AlterSpecification>(1);
    }

    public DDLAlterTableStatement addAlterSpecification(AlterSpecification alter) {
        alters.add(alter);
        return this;
    }

    public List<AlterSpecification> getAlters() {
        return alters;
    }

    public void setTableOptions(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
    }

    public TableOptions getTableOptions() {
        return tableOptions;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public Identifier getTable() {
        return table;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public DBPartitionOptions getDBPartitionOptions() {
        return DBPartitionOptions;
    }

    public void setDBPartitionOptions(DBPartitionOptions DBPartitionOptions) {
        this.DBPartitionOptions = DBPartitionOptions;
    }

}
