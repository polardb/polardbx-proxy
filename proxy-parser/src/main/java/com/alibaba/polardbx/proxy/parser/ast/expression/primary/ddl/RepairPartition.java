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

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao on 15-5-14.
 */
public class RepairPartition implements AlterSpecification {

    public RepairPartitionType getRepairPartitionType() {
        return repairPartitionType;
    }

    public void setRepairPartitionType(RepairPartitionType repairPartitionType) {
        this.repairPartitionType = repairPartitionType;
    }

    public List<Identifier> getPartition_names() {
        return partition_names;
    }

    public void setPartition_names(List<Identifier> partition_names) {
        this.partition_names = partition_names;
    }

    public static enum RepairPartitionType {
        PARTITION_NAMES, ALL
    }

    private RepairPartitionType repairPartitionType;
    private List<Identifier>    partition_names;

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }
}
