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

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao on 15-5-12.
 */
public class SubpartitionDefinition implements ASTNode {

    private final Identifier logicalName;

    private boolean          isStorage;
    private Identifier       engineName;

    private LiteralString    commentText;
    private LiteralString    dataDir;
    private LiteralString    indexDir;
    private LiteralNumber    maxNumberOfRows;
    private LiteralNumber    minNumberOfRows;
    private Identifier       tablespaceName;
    private Identifier       nodeGroupId;

    public SubpartitionDefinition(Identifier logicalName){
        this.logicalName = logicalName;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public Identifier getLogicalName() {
        return logicalName;
    }

    public boolean isStorage() {
        return isStorage;
    }

    public void setIsStorage(boolean isStorage) {
        this.isStorage = isStorage;
    }

    public Identifier getEngineName() {
        return engineName;
    }

    public void setEngineName(Identifier engineName) {
        this.engineName = engineName;
    }

    public LiteralNumber getMaxNumberOfRows() {
        return maxNumberOfRows;
    }

    public void setMaxNumberOfRows(LiteralNumber maxNumberOfRows) {
        this.maxNumberOfRows = maxNumberOfRows;
    }

    public LiteralNumber getMinNumberOfRows() {
        return minNumberOfRows;
    }

    public void setMinNumberOfRows(LiteralNumber minNumberOfRows) {
        this.minNumberOfRows = minNumberOfRows;
    }

    public Identifier getTablespaceName() {
        return tablespaceName;
    }

    public void setTablespaceName(Identifier tablespaceName) {
        this.tablespaceName = tablespaceName;
    }

    public Identifier getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(Identifier nodeGroupId) {
        this.nodeGroupId = nodeGroupId;
    }

    public LiteralString getCommentText() {
        return commentText;
    }

    public void setCommentText(LiteralString commentText) {
        this.commentText = commentText;
    }

    public LiteralString getDataDir() {
        return dataDir;
    }

    public void setDataDir(LiteralString dataDir) {
        this.dataDir = dataDir;
    }

    public LiteralString getIndexDir() {
        return indexDir;
    }

    public void setIndexDir(LiteralString indexDir) {
        this.indexDir = indexDir;
    }
}
