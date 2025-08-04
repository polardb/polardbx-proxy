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

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

public final class FastBufferPool {
    @Getter
    private final int blockSize;
    private final ByteBuffer pool;
    private final long address;
    private final AtomicLong stack = new AtomicLong(0);
    private final AtomicIntegerArray next;
    private final AtomicIntegerArray refCnt;

    private void recycle(int id) {
        // pre check
        int ref;
        if ((ref = refCnt.getPlain(id)) != 0) {
            Kill.fatalError("FastBufferPool block recycle with ref not zero(%d).", ref);
        }
        long expected, val = stack.getPlain();
        long seq;
        do {
            expected = val;
            next.setPlain(id, (int) expected); // low 32 bits
            seq = (expected & 0xFFFF_FFFF_0000_0000L) + 0x0000_0001_0000_0000L;
        } while ((val = stack.compareAndExchange(expected, seq | (id + 1))) != expected);
    }

    public FastBufferPool(int blockSize, int blockNumber) {
        this.blockSize = blockSize;
        this.pool = ByteBuffer.allocateDirect(blockSize * blockNumber);
        try {
            this.address = UnsafeBytes.BUFFER_ADDRESS_FIELD.getLong(this.pool);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (0 == this.address) {
            throw new RuntimeException("Failed to get internal address of direct buffer.");
        }
        this.next = new AtomicIntegerArray(blockNumber);
        this.refCnt = new AtomicIntegerArray(blockNumber);

        // put all blocks into the stack with reverse order
        for (int i = blockNumber - 1; i >= 0; --i) {
            recycle(i);
        }
    }

    @Getter
    public final class BufferHolder {
        private final int id;

        // only allow to construct internal
        private BufferHolder(int id) {
            this.id = id;
        }

        public ByteBuffer duplicateBuffer() {
            return pool.duplicate().position(id * blockSize).limit((id + 1) * blockSize);
        }

        public long address() {
            return address + (long) id * blockSize;
        }

        public int size() {
            return blockSize;
        }

        public void addReference() {
            final int before = refCnt.getAndIncrement(id);
            if (before <= 0) {
                Kill.fatalError("Bad BufferHolder add ref cnt before not greater zero(%d). %s",
                    before, this.toString());
            }
        }

        // only allow to invoke internal
        private void addReferenceConstruct() {
            final int before = refCnt.getAndIncrement(id);
            if (before != 0) {
                Kill.fatalError("Bad BufferHolder construct with ref cnt not zero(%d). %s",
                    before, this.toString());
            }
        }

        public void subReference() {
            final int before = refCnt.getAndDecrement(id);
            if (1 == before) {
                // free the buffer
                recycle(id);
            } else if (before <= 0) {
                Kill.fatalError("Bad BufferHolder sub ref cnt before not greater zero(%s). %s",
                    before, this.toString());
            }
        }

        @Override
        public String toString() {
            return "BufferHolder{" +
                "pool=" + FastBufferPool.this +
                ", id=" + id +
                ", ref=" + refCnt.getPlain(id) +
                '}';
        }
    }

    public BufferHolder allocateAndAddReference() {
        long expected, val = stack.getPlain();
        long seq;
        int id, next_id;
        do {
            expected = val;
            id = (int) expected;
            if (0 == id) {
                // no free block
                return null;
            }
            --id;
            next_id = next.getPlain(id);
            seq = (expected & 0xFFFF_FFFF_0000_0000L) + 0x0000_0001_0000_0000L;
        } while ((val = stack.compareAndExchange(expected, seq | next_id)) != expected);
        final BufferHolder holder = new BufferHolder(id);
        holder.addReferenceConstruct();
        return holder;
    }

    public int estimatedFreeBlocks() {
        long expected, top = stack.getAcquire();
        loop:
        while (true) {
            expected = top;
            long probe = expected;
            // visit stack optimistic
            int cnt = 0;
            while ((int) probe != 0) {
                ++cnt;
                probe = next.getPlain((int) probe - 1);
                if (((top = stack.get()) != expected)) {
                    continue loop;
                }
            }
            return cnt;
        }
    }

    public int capacity() {
        return pool.capacity();
    }

    @Override
    public String toString() {
        final int blockNumber = pool.capacity() / blockSize;
        final int frees = estimatedFreeBlocks();
        return "FastBufferPool{" +
            "blockSize=" + blockSize +
            ", blockNumber=" + blockNumber +
            ", address=" + address +
            ", estimatedFreeBlocks=" + frees +
            ", estimatedUsedBlocks=" + (blockNumber - frees) +
            '}';
    }
}
