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

public class FastCharTypes {
    private final static byte DIGIT_FLAGS = 0x01;
    private final static byte HEX_FLAGS = 0x02;
    private final static byte IDENTIFIER_FLAGS = 0x04;
    private final static byte SPACE_FLAGS = 0x08;

    private final static byte[] charFlags = new byte[128];

    static {
        for (char c = 0; c < charFlags.length; ++c) {
            if (c >= 'A' && c <= 'Z') {
                if (c <= 'F') {
                    charFlags[c] |= HEX_FLAGS | IDENTIFIER_FLAGS;
                } else {
                    charFlags[c] |= IDENTIFIER_FLAGS;
                }
            } else if (c >= 'a' && c <= 'z') {
                if (c <= 'f') {
                    charFlags[c] |= HEX_FLAGS | IDENTIFIER_FLAGS;
                } else {
                    charFlags[c] |= IDENTIFIER_FLAGS;
                }
            } else if (c >= '0' && c <= '9') {
                charFlags[c] |= DIGIT_FLAGS | HEX_FLAGS | IDENTIFIER_FLAGS;
            }
        }

        charFlags['_'] |= IDENTIFIER_FLAGS;
        charFlags['$'] |= IDENTIFIER_FLAGS;

        charFlags['\t'] |= SPACE_FLAGS;
        charFlags['\n'] |= SPACE_FLAGS;
        charFlags['\b'] |= SPACE_FLAGS;
        charFlags['\f'] |= SPACE_FLAGS;
        charFlags['\r'] |= SPACE_FLAGS;
        charFlags[0x1C] |= SPACE_FLAGS;
        charFlags[0x1D] |= SPACE_FLAGS;
        charFlags[0x1E] |= SPACE_FLAGS;
        charFlags[0x1F] |= SPACE_FLAGS;
        charFlags[' '] |= SPACE_FLAGS;
    }

    public static boolean isDigit(char c) {
        assert c > 0;
        return c < charFlags.length && (charFlags[c] & DIGIT_FLAGS) != 0;
    }

    public static boolean isHex(char c) {
        assert c > 0;
        return c < charFlags.length && (charFlags[c] & HEX_FLAGS) != 0;
    }

    public static boolean isIdentifier(char c) {
        assert c > 0;
        return c >= charFlags.length || (charFlags[c] & IDENTIFIER_FLAGS) != 0;
    }

    public static boolean isSpace(char c) {
        assert c > 0;
        return c < charFlags.length && (charFlags[c] & SPACE_FLAGS) != 0;
    }

    public static boolean isDigit(byte c) {
        return c >= 0 && (charFlags[c] & DIGIT_FLAGS) != 0;
    }

    public static boolean isHex(byte c) {
        return c >= 0 && (charFlags[c] & HEX_FLAGS) != 0;
    }

    public static boolean isIdentifier(byte c) {
        return c < 0 || (charFlags[c] & IDENTIFIER_FLAGS) != 0;
    }

    public static boolean isSpace(byte c) {
        return c > 0 && (charFlags[c] & SPACE_FLAGS) != 0;
    }
}
