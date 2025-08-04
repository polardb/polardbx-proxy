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

package com.alibaba.polardbx.proxy.context.help;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class LruCache<K, V> extends LinkedHashMap<K, V> {
    private final int capacity;
    private final BiConsumer<K, V> evictionCallback;

    public LruCache(int capacity, BiConsumer<K, V> callback) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
        this.evictionCallback = callback;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        if (size() > capacity) {
            if (evictionCallback != null) {
                evictionCallback.accept(eldest.getKey(), eldest.getValue());
            }
            return true;
        }
        return false;
    }
}
