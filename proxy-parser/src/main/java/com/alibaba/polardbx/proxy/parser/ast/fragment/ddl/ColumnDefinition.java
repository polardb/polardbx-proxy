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
package com.alibaba.polardbx.proxy.parser.ast.fragment.ddl;

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.datatype.DataType;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.ReferenceDefinition;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * NOT FULL AST
 *
 * @author QIU Shuo
 */
public class ColumnDefinition implements ASTNode {

    public ColumnNull getNotNull() {
        return notNull;
    }

    public boolean isOnUpdateCurrentTimestamp() {
        return onUpdateCurrentTimestamp;
    }

    public static enum SpecialIndex {
        PRIMARY, UNIQUE,
    }

    public static enum ColumnFormat {
        FIXED, DYNAMIC, DEFAULT,
    }

    public static enum Storage {
        DISK, MEMORY, DEFAULT,
    }

    /**
     * 对于没有指定NULL/NOT NULL时不输出
     */
    public static enum ColumnNull {
        NULL, NOTNULL,
    }

    private final DataType dataType;
    private final ColumnNull notNull;
    private final Expression defaultVal;
    private final boolean autoIncrement;
    private int unitCount;
    private int unitIndex;
    private int innerStep;
    private final SpecialIndex specialIndex;
    private final LiteralString comment;
    private final ColumnFormat columnFormat;
    private final Storage storage;
    private final ReferenceDefinition referenceDefinition;
    private final boolean onUpdateCurrentTimestamp;

    /**
     * @param defaultVal might be null
     * @param specialIndex might be null
     * @param comment might be null
     * @param columnFormat might be null
     */
    public ColumnDefinition(DataType dataType, ColumnNull notNull, Expression defaultVal, boolean autoIncrement,
                            int unitCount, int unitIndex, int innerStep, SpecialIndex specialIndex,
                            LiteralString comment, ColumnFormat columnFormat, Storage storage,
                            boolean onUpdateCurrentTimestamp, ReferenceDefinition referenceDefinition) {
        if (dataType == null) {
            throw new IllegalArgumentException("data type is null");
        }
        this.dataType = dataType;
        this.notNull = notNull;
        this.defaultVal = defaultVal;
        this.autoIncrement = autoIncrement;
        this.unitCount = unitCount;
        this.unitIndex = unitIndex;
        this.innerStep = innerStep;
        this.specialIndex = specialIndex;
        this.comment = comment;
        this.columnFormat = columnFormat;
        this.storage = storage;
        this.referenceDefinition = referenceDefinition;
        this.onUpdateCurrentTimestamp = onUpdateCurrentTimestamp;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Expression getDefaultVal() {
        return defaultVal;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public int getUnitCount() {
        return unitCount;
    }

    public void setUnitCount(int unitCount) {
        this.unitCount = unitCount;
    }

    public int getUnitIndex() {
        return unitIndex;
    }

    public void setUnitIndex(int unitIndex) {
        this.unitIndex = unitIndex;
    }

    public int getInnerStep() {
        return innerStep;
    }

    public void setInnerStep(int innerStep) {
        this.innerStep = innerStep;
    }

    public SpecialIndex getSpecialIndex() {
        return specialIndex;
    }

    public LiteralString getComment() {
        return comment;
    }

    public ColumnFormat getColumnFormat() {
        return columnFormat;
    }

    public Storage getStorage() {
        return storage;
    }

    public ReferenceDefinition getReferenceDefinition() {
        return referenceDefinition;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

}
