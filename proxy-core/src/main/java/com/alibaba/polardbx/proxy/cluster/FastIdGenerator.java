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

import java.util.concurrent.atomic.AtomicLong;

public class FastIdGenerator {
    // 42 bits for timestamp
    // 5 bits for worker id
    // 1 bit for overflow
    // 16 bits for sequence
    private static final long TWEPOCH = 1303895660503L;
    private static final long WORKER_ID_BITS = 5L;
    private static final long SEQUENCE_BITS = 16L;
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS + 1;
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + 1;
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    private final long workerId;
    private final AtomicLong lastId = new AtomicLong(0);

    private long genFirst(final long time) {
        return time << TIMESTAMP_LEFT_SHIFT | workerId << WORKER_ID_SHIFT;
    }

    public FastIdGenerator(long workerId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(
                String.format("worker Id can't be greater than %d or less than 0", MAX_WORKER_ID));
        }

        this.workerId = workerId;
        lastId.setRelease(genFirst(System.currentTimeMillis() - TWEPOCH));
    }

    public long nextId() {
        long myTime = System.currentTimeMillis() - TWEPOCH;
        _retry_get:
        while (true) {
            long id = lastId.getAndIncrement();
            long time = id >>> TIMESTAMP_LEFT_SHIFT;

            if (time >= myTime && 0 == ((id >>> SEQUENCE_BITS) & 1)) {
                // correct time(or future) and not overflow
                return id;
            }
            ++id; // set to current id
            assert time < myTime || ((id >>> SEQUENCE_BITS) & 1) != 0;

            while (time >= myTime) {
                assert ((id >>> SEQUENCE_BITS) & 1) != 0;
                // overflow, wait for next time
                Thread.yield();
                myTime = System.currentTimeMillis() - TWEPOCH;
                id = lastId.getAcquire();
                time = id >>> TIMESTAMP_LEFT_SHIFT;
                if (0 == ((id >>> SEQUENCE_BITS) & 1)) {
                    continue _retry_get;
                }
            }

            while (true) {
                // need update physical time
                final long genId = genFirst(myTime);
                final long prevId = lastId.compareAndExchange(id, genId + 1);
                if (prevId == id) {
                    // good and return
                    return genId;
                }
                id = prevId;
                time = prevId >>> TIMESTAMP_LEFT_SHIFT;
                if (time >= myTime) {
                    // updated by other thread, just retry
                    myTime = time;
                    continue _retry_get;
                }
                // or need update to new physical time, retry CAS physical time
            }
        }
    }
}
