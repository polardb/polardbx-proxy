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

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;

/**
 * Created by simiao on 14-12-10.
 */
public class DBPartitionDefinition {

    private Expression                  partition_name;
    private DBPartitionDefinitionType   type;
    private Expression                  lessThan;                 // used by
    private Integer                     startWith;
    private Integer                     endWith;
    // RANGE

    private List<TBPartitionDefinition> tbpartitionDefinitionList;

    public List<TBPartitionDefinition> getTbpartitionDefinitionList() {
        return tbpartitionDefinitionList;
    }

    public void setTbpartitionDefinitionList(List<TBPartitionDefinition> tbpartitionDefinitionList) {
        this.tbpartitionDefinitionList = tbpartitionDefinitionList;
    }

    public Expression getPartition_name() {
        return partition_name;
    }

    public void setPartition_name(Expression partition_name) {
        this.partition_name = partition_name;
    }

    public DBPartitionDefinitionType getType() {
        return type;
    }

    public void setType(DBPartitionDefinitionType type) {
        this.type = type;
    }

    public Expression getLessThan() {
        return lessThan;
    }

    public void setLessThan(Expression lessThan) {
        this.lessThan = lessThan;
    }

    public Integer getStartWith() {
        return startWith;
    }

    public void setStartWith(Integer startWith) {
        this.startWith = startWith;
    }

    public Integer getEndWith() {
        return endWith;
    }

    public void setEndWith(Integer endWith) {
        this.endWith = endWith;
    }
}
