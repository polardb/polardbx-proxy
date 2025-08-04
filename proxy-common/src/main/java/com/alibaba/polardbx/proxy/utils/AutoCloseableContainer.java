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

import java.util.ArrayList;

/**
 * Hold the object and close them when close
 */
public final class AutoCloseableContainer<T extends AutoCloseable> extends ArrayList<T> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoCloseableContainer.class);

    public AutoCloseableContainer() {
    }

    public AutoCloseableContainer(int initialCapacity) {
        super(initialCapacity);
    }

    @Override
    public synchronized void close() {
        if (!super.isEmpty()) {
            for (final AutoCloseable obj : this) {
                try {
                    obj.close();
                } catch (Throwable t) {
                    LOGGER.error("Error while closing object: {}", obj, t);
                }
            }
            super.clear();
        }
    }
}
