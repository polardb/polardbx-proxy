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

package com.alibaba.polardbx.proxy.cluster;

import com.alibaba.polardbx.proxy.context.MysqlContext;

import java.util.concurrent.ThreadLocalRandom;

public class GlobalMock {
    public static volatile String MOCK;

    public static String getMock(MysqlContext context) {
        final String mock = context.getMock();
        return null == mock ? MOCK : mock;
    }

    public static boolean mockSetReadLsnTimeout(MysqlContext context) {
        final String mock = getMock(context);
        return mock != null && mock.contains("mock_set_read_lsn_timeout");
    }

    public static boolean mockFailedToFetchLsn(MysqlContext context) {
        final String mock = getMock(context);
        return mock != null && mock.contains("mock_failed_to_fetch_lsn");
    }

    public static void randomThrowWhenForwardPacket(MysqlContext context) {
        final String mock = getMock(context);
        if (mock != null && mock.contains("random_throw_when_forward_packet") && ThreadLocalRandom.current()
            .nextBoolean()) {
            throw new RuntimeException("Mock random throw when forward packet.");
        }
    }

    public static boolean forceFrontendNoDeprecateEof() {
        return MOCK != null && MOCK.contains("force_frontend_no_deprecate_eof");
    }
}
