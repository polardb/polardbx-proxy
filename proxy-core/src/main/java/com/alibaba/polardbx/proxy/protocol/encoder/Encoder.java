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

import com.alibaba.polardbx.proxy.utils.AutoCloseableContainer;
import com.alibaba.polardbx.proxy.utils.FastBufferPool;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import lombok.Setter;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
public abstract class Encoder implements AutoCloseable {
    protected int seq = 0;

    public void resetSequence() {
        seq = 0;
    }

    public void addSequence() {
        ++seq;
    }

    public abstract void begin();

    public abstract void end() throws IOException;

    public abstract void zeros(int len) throws IOException;

    public abstract void u8(int v) throws IOException;

    public abstract void u16(int v) throws IOException;

    public abstract void u24(int v) throws IOException;

    public abstract void u32(long v) throws IOException;

    public abstract void u48(long v) throws IOException;

    public abstract void u64(long v) throws IOException;

    public abstract void f(float v) throws IOException;

    public abstract void d(double v) throws IOException;

    public static int lei_len(long v) {
        if (v < 0) {
            return 9;
        } else if (v < 251) {
            return 1;
        } else if (v < 65536) {
            return 3;
        } else if (v < 16777216) {
            return 4;
        }
        return 9;
    }

    public abstract void lei(long v) throws IOException;

    public abstract void str(byte[] buf, int off, int len) throws IOException;

    public void str(byte[] buf) throws IOException {
        str(buf, 0, buf.length);
    }

    public void str(String v) throws IOException {
        str(v.getBytes(StandardCharsets.UTF_8));
    }

    public void le_str(byte[] buf, int off, int len) throws IOException {
        lei(len);
        str(buf, off, len);
    }

    public void le_str(byte[] buf) throws IOException {
        le_str(buf, 0, buf.length);
    }

    public void le_str(String v) throws IOException {
        le_str(v.getBytes(StandardCharsets.UTF_8));
    }

    public void nt_str(byte[] buf, int off, int len) throws IOException {
        str(buf, off, len);
        u8(0);
    }

    public void nt_str(byte[] buf) throws IOException {
        nt_str(buf, 0, buf.length);
    }

    public void nt_str(String v) throws IOException {
        nt_str(v.getBytes(StandardCharsets.UTF_8));
    }

    public void pkt(byte[] buf) throws IOException {
        str(buf);
    }

    public abstract void flush() throws IOException;

    @Override
    public abstract void close();

    public interface ExportConsumer {
        void accept(AutoCloseableContainer<Slice> c) throws IOException;
    }

    public static class BytesOutput implements ExportConsumer, Closeable {
        private final ByteArrayOutputStream stream = new ByteArrayOutputStream();

        public byte[] getBytes() {
            return stream.toByteArray();
        }

        @Override
        public void accept(AutoCloseableContainer<Slice> c) throws IOException {
            for (final Slice s : c) {
                final byte[] heap = s.getHeapBuffer();
                if (heap != null) {
                    stream.write(heap, s.getOffset() + s.getConsumed(), s.getValid());
                } else {
                    stream.write(s.dump());
                }
            }
            // leave cleanup to caller
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    /**
     * Construct encoder with export consumer.
     *
     * @param pool fast direct buffer pool, use heap if null
     * @param consumer export consumer
     * @return encoder
     */
    public static Encoder create(FastBufferPool pool, ExportConsumer consumer) {
        return new EncoderImpl(pool, consumer);
    }
}
