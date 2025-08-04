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

package com.alibaba.polardbx.proxy.protocol.common;

public class MysqlError {
    public static final String GENERAL_STATE = "HY000";

    public static final int ER_ACCESS_DENIED_ERROR = 1045;
    public static final int ER_DUP_ENTRY = 1062;
    public static final int ER_NO_SUCH_TABLE = 1146;
    public static final int ER_NOT_SUPPORTED_YET = 1235;
    public static final int ER_UNKNOWN_STMT_HANDLER = 1243;
    public static final int ER_QUERY_INTERRUPTED = 1317;
    public static final int ER_INTERNAL_ERROR = 1815;
    public static final int ER_SERVER_ISNT_AVAILABLE = 3168;
}
