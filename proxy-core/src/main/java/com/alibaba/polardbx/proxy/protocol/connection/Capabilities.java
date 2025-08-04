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

package com.alibaba.polardbx.proxy.protocol.connection;

public class Capabilities {
    public static final int CLIENT_LONG_PASSWORD = 1;
    public static final int CLIENT_FOUND_ROWS = 2;
    public static final int CLIENT_LONG_FLAG = 4;
    public static final int CLIENT_CONNECT_WITH_DB = 8;
    public static final int CLIENT_NO_SCHEMA = 16;
    public static final int CLIENT_COMPRESS = 32;
    public static final int CLIENT_ODBC = 64;
    public static final int CLIENT_LOCAL_FILES = 128;
    public static final int CLIENT_IGNORE_SPACE = 256;
    public static final int CLIENT_PROTOCOL_41 = 512;
    public static final int CLIENT_INTERACTIVE = 1024;
    public static final int CLIENT_SSL = 2048;
    public static final int CLIENT_IGNORE_SIGPIPE = 4096;
    public static final int CLIENT_TRANSACTIONS = 8192;
    public static final int CLIENT_RESERVED = 16384;
    public static final int CLIENT_RESERVED2 = 32768;
    public static final int CLIENT_MULTI_STATEMENTS = (1 << 16);
    public static final int CLIENT_MULTI_RESULTS = (1 << 17);
    public static final int CLIENT_PS_MULTI_RESULTS = (1 << 18);
    public static final int CLIENT_PLUGIN_AUTH = (1 << 19);
    public static final int CLIENT_CONNECT_ATTRS = (1 << 20);
    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = (1 << 21);
    public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = (1 << 22);
    public static final int CLIENT_SESSION_TRACK = (1 << 23);
    public static final int CLIENT_DEPRECATE_EOF = (1 << 24);
    public static final int CLIENT_OPTIONAL_RESULTSET_METADATA = (1 << 25);
    public static final int CLIENT_ZSTD_COMPRESSION_ALGORITHM = (1 << 26);
    public static final int CLIENT_QUERY_ATTRIBUTES = (1 << 27);
    public static final int MULTI_FACTOR_AUTHENTICATION = (1 << 28);
    public static final int CLIENT_CAPABILITY_EXTENSION = (1 << 29);
    public static final int CLIENT_SSL_VERIFY_SERVER_CERT = (1 << 30);
    public static final int CLIENT_REMEMBER_OPTIONS = (1 << 31);

    public static int getBaseCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        // flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        // DEPRECATED: Old flag for 4.1 authentication \ CLIENT_SECURE_CONNECTION.
        flag |= Capabilities.CLIENT_RESERVED2;
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        // flag |= Capabilities.CLIENT_PS_MULTI_RESULTS;
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        flag |= Capabilities.CLIENT_DEPRECATE_EOF;
        return flag;
    }
}
