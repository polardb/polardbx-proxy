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

/**
 * Created by simiao on 14-12-10.
 */
public enum PartitionByType {
                             RIGHT_SHIFT, RANGE_HASH, RANGE_HASH1, UNI_HASH, HASH, KEY, RANGE, LIST, FREE, MM, DD, WEEK, MMDD,
                             YYYYMM, YYYYMM_OPT, YYYYDD, YYYYDD_OPT, YYYYWEEK, YYYYWEEK_OPT, YYYYMM_NOLOOP,
                             YYYYDD_NOLOOP, YYYYWEEK_NOLOOP, STR_HASH;

    public boolean canCoverRule() {
        return this.ordinal() > YYYYWEEK_OPT.ordinal();
    }
}
