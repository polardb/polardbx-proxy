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

package com.alibaba.polardbx.proxy.protocol.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SimpleDecoder extends Decoder {
    private final ByteBuffer buf;

    public SimpleDecoder(ByteBuffer duplicated, int base, int length) {
        super(base, length);
        this.buf = duplicated.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public byte peek() {
        assert pos < length;
        return buf.get(base + pos);
    }

    @Override
    public byte peek_s() {
        if (pos >= length) {
            throw new IllegalStateException("bad peek decoding, pos=" + pos + ", length=" + length);
        }
        return buf.get(base + pos);
    }

    @Override
    public int u8() {
        assert pos + 1 <= length;
        return buf.get(base + pos++) & 0xFF;
    }

    @Override
    public int u8_s() {
        if (pos + 1 > length) {
            throw new IllegalStateException("bad u8 decoding, pos=" + pos + ", length=" + length);
        }
        return buf.get(base + pos++) & 0xFF;
    }

    @Override
    public int u16() {
        assert pos + 2 <= length;
        final int v = buf.getShort(base + pos) & 0xFFFF;
        pos += 2;
        return v;
    }

    @Override
    public int u16_s() {
        if (pos + 2 > length) {
            throw new IllegalStateException("bad u16 decoding, pos=" + pos + ", length=" + length);
        }
        final int v = buf.getShort(base + pos) & 0xFFFF;
        pos += 2;
        return v;
    }

    @Override
    public int u24() {
        assert pos + 3 <= length;
        final int v = (buf.getShort(base + pos) & 0xFFFF) + ((buf.get(base + pos + 2) & 0xFF) << 16);
        pos += 3;
        return v;
    }

    @Override
    public int u24_s() {
        if (pos + 3 > length) {
            throw new IllegalStateException("bad u24 decoding, pos=" + pos + ", length=" + length);
        }
        final int v = (buf.getShort(base + pos) & 0xFFFF) + ((buf.get(base + pos + 2) & 0xFF) << 16);
        pos += 3;
        return v;
    }

    @Override
    public long u32() {
        assert pos + 4 <= length;
        final long v = buf.getInt(base + pos) & 0xFFFFFFFFL;
        pos += 4;
        return v;
    }

    @Override
    public long u32_s() {
        if (pos + 4 > length) {
            throw new IllegalStateException("bad u32 decoding, pos=" + pos + ", length=" + length);
        }
        final long v = buf.getInt(base + pos) & 0xFFFFFFFFL;
        pos += 4;
        return v;
    }

    @Override
    public long u48() {
        assert pos + 6 <= length;
        final long v = (buf.getInt(base + pos) & 0xFFFFFFFFL) + ((buf.getShort(base + pos + 4) & 0xFFFFL) << 32);
        pos += 6;
        return v;
    }

    @Override
    public long u48_s() {
        if (pos + 6 > length) {
            throw new IllegalStateException("bad u48 decoding, pos=" + pos + ", length=" + length);
        }
        final long v = (buf.getInt(base + pos) & 0xFFFFFFFFL) + ((buf.getShort(base + pos + 4) & 0xFFFFL) << 32);
        pos += 6;
        return v;
    }

    @Override
    public long i64() {
        assert pos + 8 <= length;
        final long v = buf.getLong(base + pos);
        pos += 8;
        return v;
    }

    @Override
    public long i64_s() {
        if (pos + 8 > length) {
            throw new IllegalStateException("bad i64 decoding, pos=" + pos + ", length=" + length);
        }
        final long v = buf.getLong(base + pos);
        pos += 8;
        return v;
    }

    @Override
    public float f() {
        assert pos + 4 <= length;
        final float v = buf.getFloat(base + pos);
        pos += 4;
        return v;
    }

    @Override
    public float f_s() {
        if (pos + 4 > length) {
            throw new IllegalStateException("bad f decoding, pos=" + pos + ", length=" + length);
        }
        final float v = buf.getFloat(base + pos);
        pos += 4;
        return v;
    }

    @Override
    public double d() {
        assert pos + 8 <= length;
        final double v = buf.getDouble(base + pos);
        pos += 8;
        return v;
    }

    @Override
    public double d_s() {
        if (pos + 8 > length) {
            throw new IllegalStateException("bad d decoding, pos=" + pos + ", length=" + length);
        }
        final double v = buf.getDouble(base + pos);
        pos += 8;
        return v;
    }

    @Override
    public byte[] str() {
        // probe until null or EOF
        int probe = pos;
        byte[] v = null;
        while (probe < length) {
            final byte b = buf.get(base + probe++);
            if (0 == b) {
                final int l = probe - 1 - pos;
                if (0 == l) {
                    return null;
                }
                v = new byte[l];
                break;
            }
        }
        if (null == v) {
            // end of buffer
            final int l = probe - pos;
            if (0 == l) {
                return null;
            }
            v = new byte[l];
        }
        buf.duplicate().position(base + pos).get(v);
        pos = probe;
        return v;
    }

    @Override
    public int str(byte[] out, int off) {
        // probe until null or EOF
        int probe = pos;
        while (probe < length) {
            final byte b = buf.get(base + probe++);
            if (b != 0) {
                assert off < out.length;
                out[off++] = b;
            } else {
                final int l = probe - 1 - pos;
                pos = probe;
                return l;
            }
        }
        final int l = probe - pos;
        pos = probe;
        return l;
    }

    @Override
    public byte[] str(int sz) {
        assert pos + sz <= length;
        final byte[] v = new byte[sz];
        buf.duplicate().position(base + pos).get(v);
        pos += sz;
        return v;
    }

    @Override
    public byte[] str_s(int sz) {
        if (pos + sz > length) {
            throw new IllegalStateException("bad str decoding, pos=" + pos + ", sz=" + sz + ", length=" + length);
        }
        final byte[] v = new byte[sz];
        buf.duplicate().position(base + pos).get(v);
        pos += sz;
        return v;
    }

    @Override
    public void str(int sz, byte[] out, int off) {
        assert pos + sz <= length;
        assert off + sz <= out.length;
        buf.duplicate().position(base + pos).get(out, off, sz);
        pos += sz;
    }

    @Override
    public void str_s(int sz, byte[] out, int off) {
        if (pos + sz > length) {
            throw new IllegalStateException("bad str decoding, pos=" + pos + ", sz=" + sz + ", length=" + length);
        }
        if (off + sz > out.length) {
            throw new IllegalStateException("bad str decoding, off=" + off + ", sz=" + sz + ", length=" + out.length);
        }
        buf.duplicate().position(base + pos).get(out, off, sz);
        pos += sz;
    }
}
