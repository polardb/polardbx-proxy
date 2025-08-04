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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

public class UnsafeBytes {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnsafeBytes.class);

    public static final Unsafe UNSAFE;
    public static final int BYTE_ARRAY_BASE_OFFSET;
    public static final Field BUFFER_ADDRESS_FIELD;

    static {
        Unsafe unsafe = null;
        try {
            // Try another way to get unsafe instance
            final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
        } catch (Exception e) {
            LOGGER.warn("Error when load unsafe simple way.", e);
        }
        if (null == unsafe) {
            try {
                unsafe = AccessController.doPrivileged(
                    (PrivilegedExceptionAction<Unsafe>) () -> {
                        Class<Unsafe> k = Unsafe.class;
                        for (Field f : k.getDeclaredFields()) {
                            f.setAccessible(true);
                            Object x = f.get(null);
                            if (k.isInstance(x)) {
                                return k.cast(x);
                            }
                        }
                        // The sun.misc.Unsafe field does not exist.
                        return null;
                    });
            } catch (Throwable e) {
                LOGGER.warn("Error when load unsafe complex way.", e);
            }
        }

        // done unsafe
        UNSAFE = unsafe;
        if (null == UNSAFE) {
            LOGGER.warn("Unsafe is not available.");
            BYTE_ARRAY_BASE_OFFSET = 0;
        } else {
            BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

            // check and confirm LITTLE ENDIAN
            if (UNSAFE.getInt(new byte[] {(byte) 0xFF, 0x00, 0x00, 0x00}, BYTE_ARRAY_BASE_OFFSET) != 0xFF) {
                Kill.fatalError("Not LITTLE ENDIAN !!!");
                throw new RuntimeException("Not LITTLE ENDIAN !!!");
            }
        }

        // load address field of Buffer
        try {
            BUFFER_ADDRESS_FIELD = Buffer.class.getDeclaredField("address");
            BUFFER_ADDRESS_FIELD.setAccessible(true);
        } catch (Exception e) {
            Kill.fatalError("Failed to get address of Buffer!!! %s", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
