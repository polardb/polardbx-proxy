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

package com.alibaba.polardbx.proxy.protocol.encoder;

import com.alibaba.polardbx.proxy.net.NIOProcessor;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.utils.AutoCloseableContainer;
import com.alibaba.polardbx.proxy.utils.FastBufferPool;
import com.alibaba.polardbx.proxy.utils.Slice;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class EncoderImpl extends Encoder {
    private static final int FLUSH_THRESH_0 = 8;
    private static final int FLUSH_THRESH_1 = 10;

    private final FastBufferPool pool;
    private final ExportConsumer consumer;
    private final AutoCloseableContainer<Slice> container = new AutoCloseableContainer<>();

    // last buffer writer, also indicate container empty or not
    private ByteBuffer writer = null;

    private boolean allocateSlice() {
        assert pool != null;
        final FastBufferPool.BufferHolder holder = pool.allocateAndAddReference();
        if (holder != null) {
            try {
                final Slice slice = new Slice(holder, 0, holder.size());
                slice.setValid(0);
                try {
                    container.add(slice);
                    writer = slice.duplicateBuffer().order(ByteOrder.LITTLE_ENDIAN);
                    return true;
                } catch (Throwable t) {
                    slice.close();
                    throw t;
                }
            } finally {
                holder.subReference();
            }
        }
        return false;
    }

    private void allocateSliceWithMinimumSize(int sz) {
        if (pool != null && sz * 2 < pool.getBlockSize() && allocateSlice()) {
            return; // new direct slice allocated
        }
        // failed or large space needed
        final int size = pool != null ?
            Math.max(sz * 2, pool.getBlockSize()) : Math.max(sz * 2, NIOProcessor.DEFAULT_BLOCK_SIZE);
        final Slice slice = new Slice(ByteBuffer.allocate(size), 0, size);
        slice.setValid(0);
        container.add(slice);
        writer = slice.duplicateBuffer().order(ByteOrder.LITTLE_ENDIAN);
    }

    public EncoderImpl(FastBufferPool pool, ExportConsumer consumer) {
        this.pool = pool;
        this.consumer = consumer;
    }

    private void finalizeLastWrite() {
        assert writer != null;
        assert !container.isEmpty();
        final Slice last = container.get(container.size() - 1);
        last.setValid(last.getLength() - writer.remaining());
    }

    private void ensureSpace(int sz) {
        if (null == writer) {
            assert container.isEmpty();
            allocateSliceWithMinimumSize(sz);
        } else if (writer.remaining() < sz) {
            finalizeLastWrite();
            allocateSliceWithMinimumSize(sz);
        }
    }

    private ByteBuffer headWriter = null;
    private int written = 0;

    @Override
    public void begin() {
        if (headWriter != null) {
            throw new RuntimeException("Duplicate begin when build packet.");
        }
        ensureSpace(MysqlPacket.NORMAL_HEADER_SIZE);
        headWriter = writer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        written = 0;

        // move write pos
        writer.position(writer.position() + 3);
        writer.put((byte) seq);
    }

    @Override
    public void end() throws IOException {
        if (null == headWriter) {
            throw new RuntimeException("Missing begin when build packet.");
        }
        headWriter.putShort((short) written);
        headWriter.put((byte) (written >>> 16));
        headWriter = null;

        /// try active flush
        final int sz = container.size();
        if (sz > FLUSH_THRESH_1) {
            flush();
        } else if (sz > FLUSH_THRESH_0 && writer.remaining() < MysqlPacket.DEFAULT_RESERVE_BUFFER_SIZE) {
            flush();
        }
    }

    private void postDealing() throws IOException {
        // only deal segmentation when in packet build and payload size exceed
        if (headWriter != null && written >= MysqlPacket.MAX_PAYLOAD_SIZE) {
            // copy data which exceed the size and put it to new packet
            final byte[] extra;
            if (written > MysqlPacket.MAX_PAYLOAD_SIZE) {
                extra = new byte[written - MysqlPacket.MAX_PAYLOAD_SIZE];
                writer.position(writer.position() - extra.length);
                writer.get(extra);
                written = MysqlPacket.MAX_PAYLOAD_SIZE;
            } else {
                extra = null;
            }
            // end packet
            end();
            addSequence();
            begin();
            // put extra data
            if (extra != null) {
                str(extra);
            }
        }
    }

    @Override
    public void zeros(int len) throws IOException {
        ensureSpace(len);
        int rest = len;
        while (true) {
            if (rest >= Long.BYTES) {
                writer.putLong(0);
                rest -= Long.BYTES;
            } else if (rest >= Integer.BYTES) {
                writer.putInt(0);
                rest -= Integer.BYTES;
            } else if (rest >= Short.BYTES) {
                writer.putShort((short) 0);
                rest -= Short.BYTES;
            } else if (rest > 0) {
                writer.put((byte) 0);
                --rest;
            } else {
                assert 0 == rest;
                break;
            }
        }
        written += len;
        postDealing();
    }

    @Override
    public void u8(int v) throws IOException {
        ensureSpace(1);
        writer.put((byte) v);
        ++written;
        postDealing();
    }

    @Override
    public void u16(int v) throws IOException {
        ensureSpace(2);
        writer.putShort((short) v);
        written += 2;
        postDealing();
    }

    @Override
    public void u24(int v) throws IOException {
        ensureSpace(3);
        headWriter.putShort((short) v);
        headWriter.put((byte) (v >>> 16));
        written += 3;
        postDealing();
    }

    @Override
    public void u32(long v) throws IOException {
        ensureSpace(4);
        writer.putInt((int) v);
        written += 4;
        postDealing();
    }

    @Override
    public void u48(long v) throws IOException {
        ensureSpace(6);
        headWriter.putInt((int) v);
        headWriter.putShort((short) (v >>> 32));
        written += 6;
        postDealing();
    }

    @Override
    public void u64(long v) throws IOException {
        ensureSpace(8);
        writer.putLong(v);
        written += 8;
        postDealing();
    }

    @Override
    public void f(float v) throws IOException {
        ensureSpace(4);
        writer.putFloat(v);
        written += 4;
        postDealing();
    }

    @Override
    public void d(double v) throws IOException {
        ensureSpace(8);
        writer.putDouble(v);
        written += 8;
        postDealing();
    }

    @Override
    public void lei(long v) throws IOException {
        ensureSpace(9);
        if (v < 0) {
            writer.put((byte) 0xFE);
            writer.putLong(v);
            written += 9;
        } else if (v < 251) {
            writer.put((byte) v);
            written += 1;
        } else if (v < 65536) {
            writer.put((byte) 0xFC);
            writer.putShort((short) v);
            written += 3;
        } else if (v < 16777216) {
            writer.put((byte) 0xFD);
            headWriter.putShort((short) v);
            headWriter.put((byte) (v >>> 16));
            written += 4;
        } else {
            writer.put((byte) 0xFE);
            writer.putLong(v);
            written += 9;
        }
        postDealing();
    }

    @Override
    public void str(byte[] buf, int off, int len) throws IOException {
        ensureSpace(len);
        writer.put(buf, off, len);
        written += len;
        postDealing();
    }

    @Override
    public void flush() throws IOException {
        if (writer != null) {
            finalizeLastWrite();
            try (container) {
                consumer.accept(container);
            } finally {
                writer = null;
            }
        }
    }

    @Override
    public void close() {
        container.close();
    }
}
