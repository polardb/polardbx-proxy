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

public class FieldFlags {
    public static final int NOT_NULL_FLAG = 1;
    public static final int PRI_KEY_FLAG = 2;
    public static final int UNIQUE_KEY_FLAG = 4;
    public static final int MULTIPLE_KEY_FLAG = 16;
    public static final int BLOB_FLAG = 16;
    public static final int UNSIGNED_FLAG = 32;
    public static final int ZEROFILL_FLAG = 64;
    public static final int BINARY_FLAG = 128;
    public static final int ENUM_FLAG = 256;
    public static final int AUTO_INCREMENT_FLAG = 512;
    public static final int TIMESTAMP_FLAG = 1024;
    public static final int SET_FLAG = 2048;
    public static final int NO_DEFAULT_VALUE_FLAG = 4096;
    public static final int ON_UPDATE_NOW_FLAG = 8192;
    public static final int NUM_FLAG = 32768;
    public static final int PART_KEY_FLAG = 16384;
    public static final int GROUP_FLAG = 32768;
    public static final int UNIQUE_FLAG = 65536;
    public static final int BINCMP_FLAG = 131072;
}
