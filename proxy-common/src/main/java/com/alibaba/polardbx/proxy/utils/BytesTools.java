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

package com.alibaba.polardbx.proxy.utils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class BytesTools {
    public static long crc32(byte[] bytes, int offset, int length) {
        final CRC32 crc32 = new CRC32();
        crc32.update(ByteBuffer.wrap(bytes, offset, length));
        return crc32.getValue();
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytes2Hex(byte[] bytes) {
        return bytes2Hex(bytes, 0, bytes.length);
    }

    public static String bytes2Hex(byte[] bytes, int offset, int length) {
        char[] hexChars = new char[length * 2];
        for (int j = 0; j < length; j++) {
            int v = bytes[j + offset] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static String beautifulHex(byte[] bytes, int offset, int length) {
        final StringBuilder builder = new StringBuilder();
        final int bytesPerLine = 16;

        for (int i = offset; i < offset + length || i == offset; i += bytesPerLine) {
            builder.append(String.format("%08X  ", i - offset));

            int j = 0;
            for (; j < bytesPerLine && (i + j) < offset + length; j++) {
                builder.append(String.format("%02X ", bytes[i + j]));
            }

            for (; j < bytesPerLine; j++) {
                builder.append("   ");
            }

            builder.append(" |");

            for (j = 0; j < bytesPerLine && (i + j) < offset + length; j++) {
                final byte b = bytes[i + j];
                if (b >= 0x20 && b <= 0x7E) {
                    builder.append((char) b);
                } else {
                    builder.append('.');
                }
            }

            for (; j < bytesPerLine; j++) {
                builder.append(' ');
            }

            builder.append('|');
            if (i + bytesPerLine < offset + length) {
                builder.append('\n');
            }
        }

        return builder.toString();
    }
}
