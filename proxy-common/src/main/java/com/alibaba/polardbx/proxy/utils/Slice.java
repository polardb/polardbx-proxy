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
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Represent a slice from fast buffer pool or heap buffer.
 * Add the reference of buffer holder when slice created, and sub when slice closed.
 * When slice is from buffer holder, offset is the relevant offset in buffer holder.
 * When slice is from heap buffer, offset is the absolute offset of heap byte array.
 * Consumed and valid are then internal variables of slice, which are used to mark the slice's status.
 */
public class Slice implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Slice.class);

    // basic info
    private FastBufferPool.BufferHolder holder;
    private final ByteBuffer heap;
    @Getter
    private final int offset;
    @Getter
    private int length;

    // for extra dealing
    @Getter
    @Setter
    private int consumed;
    @Getter
    @Setter
    private int valid;

    // todo support large packet (one heap slice and multi slice part)

    private boolean validation() {
        // check val
        if (offset < 0 || length < 0) {
            return false;
        }

        // check whether inside the buffer
        if (holder != null) {
            if (offset + length > holder.size()) {
                return false;
            }
        } else if (null == heap) {
            return false; // closed?
        } else {
            if (offset + length > heap.capacity()) {
                return false;
            }
        }

        // check status
        return consumed >= 0 && valid >= 0 && consumed + valid <= length;
    }

    public Slice(@NotNull FastBufferPool.BufferHolder holder, int offset, int length) {
        // add reference
        holder.addReference();
        this.holder = holder;
        this.heap = null;
        this.offset = offset;
        this.length = length;
        this.consumed = 0;
        this.valid = length;
        assert validation();
    }

    public Slice(@NotNull ByteBuffer heap, int offset, int length) {
        this.holder = null;
        this.heap = heap;
        this.offset = offset;
        this.length = length;
        this.consumed = 0;
        this.valid = length;
        assert validation();
    }

    public void consume(int len) {
        assert len <= valid;
        consumed += len;
        valid -= len;
        assert validation();
    }

    public boolean merge(Slice next) {
        assert validation() && next.validation();
        // check same buffer holder
        if (holder != next.holder || heap != next.heap) {
            return false;
        }
        // tail is next's head
        if (offset + consumed + valid == next.offset + next.consumed) {
            final int limit = next.offset + next.consumed + next.valid;
            if (limit > offset + length) {
                length = limit - offset; // extend length if needed
            }
            valid = limit - offset - consumed;
            assert validation() && next.validation();
            return true;
        }
        return false;
    }

    /**
     * Get ByteBuffer represent the slice(without consumed and valid).
     *
     * @return buffer of the slice
     */
    public ByteBuffer duplicateBuffer() {
        assert validation();
        if (holder != null) {
            final ByteBuffer buffer = holder.duplicateBuffer();
            buffer.position(buffer.position() + offset);
            buffer.limit(buffer.position() + length);
            return buffer;
        }
        return heap.duplicate().position(offset).limit(offset + length);
    }

    /**
     * Get physical address of the slice(without consumed and valid).
     *
     * @return physical address of the slice or 0 if slice is from Java heap
     */
    public long getAddress() {
        assert validation();
        if (holder != null) {
            return holder.address() + offset;
        } else if (heap.isDirect()) {
            try {
                return UnsafeBytes.BUFFER_ADDRESS_FIELD.getLong(heap) + offset;
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                return 0;
            }
        } else {
            return 0;
        }
    }

    /**
     * Get base heap byte buffer(without offset, length, consumed and valid).
     *
     * @return base heap byte buffer or null if not Java heap
     */
    public byte[] getHeapBuffer() {
        assert validation();
        return heap != null && heap.hasArray() ? heap.array() : null;
    }

    public byte[] dump() {
        assert validation();
        final byte[] bytes = new byte[valid];
        if (null == heap) {
            if (holder != null) {
                final ByteBuffer buf = holder.duplicateBuffer();
                buf.position(buf.position() + offset + consumed).get(bytes);
            }
        } else {
            heap.duplicate().position(offset + consumed).get(bytes);
        }
        return bytes;
    }

    @Override
    public synchronized void close() {
        if (holder != null) {
            assert validation();
            holder.subReference();
            holder = null;
        } // or closed, no assert
    }

    @Override
    public String toString() {
        final byte[] bytes = new byte[length];
        if (null == heap) {
            if (holder != null) {
                final ByteBuffer buf = holder.duplicateBuffer();
                buf.position(buf.position() + offset).get(bytes);
            }
        } else {
            heap.duplicate().position(offset).get(bytes);
        }
        return "Slice{" +
            "holder=" + holder +
            ", offset=" + offset +
            ", length=" + length +
            ", crc=" + String.format("0x%08x", BytesTools.crc32(bytes, 0, bytes.length)) +
            "consumed=" + consumed +
            ", valid=" + valid +
            "}\n" + BytesTools.beautifulHex(bytes, 0, bytes.length);
    }
}
