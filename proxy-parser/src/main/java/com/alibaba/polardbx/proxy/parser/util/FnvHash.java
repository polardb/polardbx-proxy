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

package com.alibaba.polardbx.proxy.parser.util;

public final class FnvHash {
    public final static long BASIC = 0xcbf29ce484222325L;
    public final static long PRIME = 0x100000001b3L;

    public static long fnv1a_64_lower(final byte[] bytes, final int offset, final int len) {
        long hashCode = BASIC;
        final int limit = Math.min(bytes.length, offset + len);
        for (int i = offset; i < limit; ++i) {
            byte ch = bytes[i];
            if (ch >= 'A' && ch <= 'Z') {
                ch = (byte) (ch + 32);
            }
            hashCode ^= ch & 0xFF;
            hashCode *= PRIME;
        }
        return hashCode;
    }
}
