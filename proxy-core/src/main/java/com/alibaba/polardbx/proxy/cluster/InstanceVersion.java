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

import com.alibaba.polardbx.proxy.serverless.HaManager;

import java.nio.charset.StandardCharsets;

public class InstanceVersion {
    private static final String VERSION_TAIL = "-X-Proxy-1.0.0";
    private static final byte[] VERSION_TAIL_BYTES = VERSION_TAIL.getBytes(StandardCharsets.UTF_8);

    public static byte[] buildVersion() {
        final int version = HaManager.getInstance().getVersion();
        final int major = version / 10000;
        final int minor = version % 10000 / 100;
        final int patch = version % 100;
        final int len =
            (major >= 10 ? 3 : 2) + (minor >= 10 ? 3 : 2) + (patch >= 10 ? 2 : 1) + VERSION_TAIL_BYTES.length;
        final byte[] bytes = new byte[len];
        int pos = 0;
        if (major >= 10) {
            bytes[pos++] = (byte) (major / 10 + '0');
            bytes[pos++] = (byte) (major % 10 + '0');
            bytes[pos++] = '.';
        } else {
            bytes[pos++] = (byte) (major + '0');
            bytes[pos++] = '.';
        }
        if (minor >= 10) {
            bytes[pos++] = (byte) (minor / 10 + '0');
            bytes[pos++] = (byte) (minor % 10 + '0');
            bytes[pos++] = '.';
        } else {
            bytes[pos++] = (byte) (minor + '0');
            bytes[pos++] = '.';
        }
        if (patch >= 10) {
            bytes[pos++] = (byte) (patch / 10 + '0');
            bytes[pos++] = (byte) (patch % 10 + '0');
        } else {
            bytes[pos++] = (byte) (patch + '0');
        }
        System.arraycopy(VERSION_TAIL_BYTES, 0, bytes, pos, VERSION_TAIL_BYTES.length);
        return bytes;
    }
}
