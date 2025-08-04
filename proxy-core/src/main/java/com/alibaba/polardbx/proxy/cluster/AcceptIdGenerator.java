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

import java.util.concurrent.atomic.AtomicInteger;

public class AcceptIdGenerator {
    private static final long WORKER_ID_BITS = 4L; // at most 16 proxy
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    private final long workerId;
    private final AtomicInteger acceptId = new AtomicInteger(0);

    public AcceptIdGenerator(long workerId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(
                String.format("worker Id can't be greater than %d or less than 0", MAX_WORKER_ID));
        }
        this.workerId = workerId;
    }

    public int nextAcceptId() {
        final int id;
        while (true) {
            final int val = acceptId.incrementAndGet();
            if (val <= 0) {
                if (acceptId.compareAndSet(val, 1)) {
                    id = 1;
                    break;
                }
            } else {
                id = val;
                break;
            }
        }
        return (int) (((id << WORKER_ID_BITS) + workerId) & 0x7FFF_FFFF);
    }
}
