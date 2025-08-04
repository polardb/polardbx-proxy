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

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DBPartitionOptions;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.PartitionOptions;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.ColumnDefinition;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.TableOptions;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.IndexDefinition;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.polardbx.proxy.parser.util.Pair;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * [STORAGE {DISK|MEMORY|DEFAULT}] NOT SUPPORT
 *
 * @author QIU Shuo
 */
public class DDLCreateTableStatement implements DDLStatement {

    public List<Pair<Identifier, IndexDefinition>> getForeignKeys() {
        return foreignKeys;
    }

    public boolean isHasPrimaryKeyConstraint() {
        return hasPrimaryKeyConstraint;
    }

    public void setHasPrimaryKeyConstraint(boolean hasPrimaryKeyConstraint) {
        this.hasPrimaryKeyConstraint = hasPrimaryKeyConstraint;
    }

    public PartitionOptions getPartitionOptions() {
        return partitionOptions;
    }

    public void setPartitionOptions(PartitionOptions partitionOptions) {
        this.partitionOptions = partitionOptions;
    }

    public static enum SelectOption {
        IGNORED, REPLACE
    }

    // ------ create_definition
    private final boolean temporary;
    private final boolean ifNotExists;
    private final Identifier table;
    private final List<Pair<Identifier, ColumnDefinition>> colDefs;
    private IndexDefinition primaryKey;
    private final List<Pair<Identifier, IndexDefinition>> uniqueKeys;
    private final List<Pair<Identifier, IndexDefinition>> keys;
    private final List<Pair<Identifier, IndexDefinition>> fullTextKeys;
    private final List<Pair<Identifier, IndexDefinition>> spatialKeys;
    private final List<Pair<Identifier, IndexDefinition>> foreignKeys;
    private final List<Expression> checks;
    /* 应该只有一个CONSTRAINT for primary key */
    private Identifier paimaryKeyConstraint = null;
    private boolean hasPrimaryKeyConstraint = false;

    // ------ table_options
    private TableOptions tableOptions;

    // ------ partition_options
    private PartitionOptions partitionOptions;

    /**
     * Add auto DBpartition options
     * http://dev.mysql.com/doc/refman/5.1/en/create-table.html used this option
     * to do table shard 这里有可能不会拥有完整的partition信息，因此能获取多少就填多少，然后再visitor的时候由
     * visitor设置一些默认值，解析的工作由MySQLDDLParser
     */
    private DBPartitionOptions DBPartitionOptions;

    // ------ select_statement
    private Pair<SelectOption, DMLSelectStatement> select;

    /* LIKE old_tbl_name */
    private Identifier oldTblName;

    public DDLCreateTableStatement(boolean temporary, boolean ifNotExists, Identifier table) {
        this.table = table;
        this.temporary = temporary;
        this.ifNotExists = ifNotExists;
        this.colDefs = new ArrayList<Pair<Identifier, ColumnDefinition>>(4);
        this.uniqueKeys = new ArrayList<Pair<Identifier, IndexDefinition>>(1);
        this.keys = new ArrayList<Pair<Identifier, IndexDefinition>>(2);
        this.fullTextKeys = new ArrayList<Pair<Identifier, IndexDefinition>>(1);
        this.spatialKeys = new ArrayList<Pair<Identifier, IndexDefinition>>(1);
        this.checks = new ArrayList<Expression>(1);
        this.foreignKeys = new ArrayList<Pair<Identifier, IndexDefinition>>(1);
    }

    public DDLCreateTableStatement setTableOptions(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
        return this;
    }

    public DDLCreateTableStatement addColumnDefinition(Identifier colname, ColumnDefinition def) {
        colDefs.add(new Pair<Identifier, ColumnDefinition>(colname, def));
        return this;
    }

    public DDLCreateTableStatement setPrimaryKey(IndexDefinition def) {
        primaryKey = def;
        return this;
    }

    public DDLCreateTableStatement addUniqueIndex(Identifier colname, IndexDefinition def) {
        uniqueKeys.add(new Pair<Identifier, IndexDefinition>(colname, def));
        return this;
    }

    public DDLCreateTableStatement addForeignKey(Identifier colname, IndexDefinition def) {
        foreignKeys.add(new Pair<Identifier, IndexDefinition>(colname, def));
        return this;
    }

    public DDLCreateTableStatement addIndex(Identifier colname, IndexDefinition def) {
        keys.add(new Pair<Identifier, IndexDefinition>(colname, def));
        return this;
    }

    public DDLCreateTableStatement addFullTextIndex(Identifier colname, IndexDefinition def) {
        fullTextKeys.add(new Pair<Identifier, IndexDefinition>(colname, def));
        return this;
    }

    public DDLCreateTableStatement addSpatialIndex(Identifier colname, IndexDefinition def) {
        spatialKeys.add(new Pair<Identifier, IndexDefinition>(colname, def));
        return this;
    }

    public DDLCreateTableStatement addCheck(Expression check) {
        checks.add(check);
        return this;
    }

    public TableOptions getTableOptions() {
        return tableOptions;
    }

    public Pair<SelectOption, DMLSelectStatement> getSelect() {
        return select;
    }

    public void setSelect(SelectOption option, DMLSelectStatement select) {
        this.select = new Pair<SelectOption, DMLSelectStatement>(option, select);
    }

    public boolean isTemporary() {
        return temporary;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Identifier getTable() {
        return table;
    }

    /**
     * @return key := columnName
     */
    public List<Pair<Identifier, ColumnDefinition>> getColDefs() {
        return colDefs;
    }

    public IndexDefinition getPrimaryKey() {
        return primaryKey;
    }

    public List<Pair<Identifier, IndexDefinition>> getUniqueKeys() {
        return uniqueKeys;
    }

    public List<Pair<Identifier, IndexDefinition>> getKeys() {
        return keys;
    }

    public List<Pair<Identifier, IndexDefinition>> getFullTextKeys() {
        return fullTextKeys;
    }

    public List<Pair<Identifier, IndexDefinition>> getSpatialKeys() {
        return spatialKeys;
    }

    public List<Expression> getChecks() {
        return checks;
    }

    public DBPartitionOptions getDBPartitionOptions() {
        return DBPartitionOptions;
    }

    public DDLCreateTableStatement setDBPartitionOptions(DBPartitionOptions DBPartitionOptions) {
        this.DBPartitionOptions = DBPartitionOptions;
        return this;
    }

    public Identifier getPrimaryKeyConstraint() {
        return paimaryKeyConstraint;
    }

    public void setPaimaryKeyConstraint(Identifier paimaryKeyConstraint) {
        this.paimaryKeyConstraint = paimaryKeyConstraint;
    }

    public Identifier getOldTblName() {
        return oldTblName;
    }

    public void setOldTblName(Identifier oldTblName) {
        this.oldTblName = oldTblName;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
