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

import java.util.List;

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao on 15-5-12.
 */
public class PartitionOptions implements ASTNode {

    private PartitionBy               partitionBy;
    private LiteralNumber             num;
    private SubPartitionBy            subPartitionBy;
    private List<PartitionDefinition> partitionDefinitionList;

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public PartitionBy getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(PartitionBy partitionBy) {
        this.partitionBy = partitionBy;
    }

    public LiteralNumber getNum() {
        return num;
    }

    public void setNum(LiteralNumber num) {
        this.num = num;
    }

    public SubPartitionBy getSubPartitionBy() {
        return subPartitionBy;
    }

    public void setSubPartitionBy(SubPartitionBy subPartitionBy) {
        this.subPartitionBy = subPartitionBy;
    }

    public List<PartitionDefinition> getPartitionDefinitionList() {
        return partitionDefinitionList;
    }

    public void setPartitionDefinitionList(List<PartitionDefinition> partitionDefinitionList) {
        this.partitionDefinitionList = partitionDefinitionList;
    }
}
