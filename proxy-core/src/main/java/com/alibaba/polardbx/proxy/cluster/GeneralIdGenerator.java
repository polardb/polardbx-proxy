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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralIdGenerator {
    protected final static Logger LOGGER = LoggerFactory.getLogger(GeneralIdGenerator.class);

    /**
     * The maximum number of IDs per range (i.e. one millisecond): 2^12 = 4096.
     */
    public static final int MAX_NUM_OF_IDS_PER_RANGE = 4096;

    private static final long TWEPOCH = 1303895660503L;

    private static final long WORKER_ID_BITS = 10L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    private static final long TIMESTAMP_MASK = -1L << TIMESTAMP_LEFT_SHIFT;
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);
    private static final long WORKER_ID_MASK = ~TIMESTAMP_MASK ^ SEQUENCE_MASK;

    private volatile long workerId;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public GeneralIdGenerator(long workerId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(
                String.format("worker Id can't be greater than %d or less than 0", MAX_WORKER_ID));
        }
        this.workerId = workerId;
    }

    public void rebind(long workerId) {
        if (this.workerId != workerId) {
            this.workerId = workerId;
        }
    }

    /**
     * Get single ID.
     *
     * @return a ID
     */
    public synchronized long nextId() {
        long timestamp;
        int tryTimes = 0;
        do {
            tryTimes++;
            timestamp = this.timeGen();

            if (this.lastTimestamp == timestamp) {
                this.sequence = this.sequence + 1 & SEQUENCE_MASK;
                if (this.sequence == 0) {
                    timestamp = this.tilNextMillis(this.lastTimestamp);
                }
            } else {
                this.sequence = 0;
            }
        } while (timestamp < this.lastTimestamp);

        if (tryTimes > 10) {
            LOGGER.debug("Clock moved backwards {} times! It may be ugly if this warning appear too much times.",
                tryTimes);
        }

        this.lastTimestamp = timestamp;

        return assembleId(timestamp, this.workerId, this.sequence);
    }

    /**
     * Get a batch of IDs with input size and return the max one in them.
     *
     * @return maximum ID in the batch
     */
    public synchronized long nextId(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("Size must be greater than 0.");
        }
        if (size == 1) {
            return nextId();
        }

        boolean needMoreRanges = true;
        long timestamp = timeGen();

        if (this.lastTimestamp == timestamp) {
            int numOfAssignableIdsInFirstRange = MAX_NUM_OF_IDS_PER_RANGE - ((int) this.sequence) - 1;
            if (numOfAssignableIdsInFirstRange < 0) {
                // This should never happen, but still reset it in case of accident.
                numOfAssignableIdsInFirstRange = 0;
            }
            if (numOfAssignableIdsInFirstRange >= size) {
                // There are enough ids to assign for the batch in the first range.
                this.sequence += size;
                needMoreRanges = false;
            } else {
                size -= numOfAssignableIdsInFirstRange;
                this.sequence = 0;
                // The sequences in current range has been exhausted, so let's move to the next millisecond.
                timestamp = tilNextMillis(this.lastTimestamp);
            }
        } else {
            this.sequence = 0;
            while (timestamp < this.lastTimestamp) {
                timestamp = timeGen();
            }
        }

        if (needMoreRanges) {
            // The number of consecutively full ranges (one range per ms) needed for the rest of ids.
            long numOfFullRanges = size / MAX_NUM_OF_IDS_PER_RANGE;

            // The number of ids in the last range must be less than the maximum number 4096.
            long numOfIdsInLastRange = size % MAX_NUM_OF_IDS_PER_RANGE;

            if (numOfIdsInLastRange == 0) {
                numOfFullRanges--;
                numOfIdsInLastRange = MAX_NUM_OF_IDS_PER_RANGE;
            }

            if (numOfFullRanges > 0) {
                // Move to the last range directly.
                timestamp += numOfFullRanges;
                // Catch up with the last timestamp.
                while (this.lastTimestamp <= timestamp) {
                    this.lastTimestamp = this.timeGen();
                }
            }

            // Increase the sequence for the rest of ids
            this.sequence += numOfIdsInLastRange - 1;
        }

        this.lastTimestamp = timestamp;

        // Return the maximum ID in the whole batch.
        return assembleId(timestamp, this.workerId, this.sequence);
    }

    public static long assembleId(long timestamp, long workerId, long sequence) {
        return ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT) | (workerId << WORKER_ID_SHIFT) | (sequence);
    }

    public static String extractId(long id) {
        return extractTimestamp(id) + "," + extractWorkerId(id) + "," + extractSequence(id);
    }

    public static long extractTimestamp(long id) {
        return ((id & TIMESTAMP_MASK) >>> TIMESTAMP_LEFT_SHIFT) + TWEPOCH;
    }

    public static long extractWorkerId(long id) {
        return (id & WORKER_ID_MASK) >>> WORKER_ID_SHIFT;
    }

    public static long extractSequence(long id) {
        return id & SEQUENCE_MASK;
    }

    private long tilNextMillis(long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }
}
