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

package com.alibaba.polardbx.proxy.protocol.command;

public class StatusFlags {
    public static final int SERVER_STATUS_IN_TRANS = 1;
    public static final int SERVER_STATUS_AUTOCOMMIT = 2;
    public static final int SERVER_MORE_RESULTS_EXISTS = 8;
    public static final int SERVER_QUERY_NO_GOOD_INDEX_USED = 16;
    public static final int SERVER_QUERY_NO_INDEX_USED = 32;
    public static final int SERVER_STATUS_CURSOR_EXISTS = 64;
    public static final int SERVER_STATUS_LAST_ROW_SENT = 128;
    public static final int SERVER_STATUS_DB_DROPPED = 256;
    public static final int SERVER_STATUS_NO_BACKSLASH_ESCAPES = 512;
    public static final int SERVER_STATUS_METADATA_CHANGED = 1024;
    public static final int SERVER_QUERY_WAS_SLOW = 2048;
    public static final int SERVER_PS_OUT_PARAMS = 4096;
    public static final int SERVER_STATUS_IN_TRANS_READONLY = 8192;
    public static final int SERVER_SESSION_STATE_CHANGED = (1 << 14);
}
