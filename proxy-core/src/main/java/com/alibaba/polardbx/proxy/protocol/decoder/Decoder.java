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

import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.utils.Slice;
import com.alibaba.polardbx.proxy.utils.UnsafeBytes;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

public abstract class Decoder {
    protected final int base;
    protected final int length;

    @Getter
    @Setter
    protected int pos = 0;

    @Getter
    protected int lastSeq = 0;

    public Decoder(int base, int length) {
        this.base = base;
        this.length = length;
    }

    public int remaining() {
        return length - pos;
    }

    public void skip() {
        skip(1);
    }

    public void skip_s() {
        skip_s(1);
    }

    public void skip(int sz) {
        pos += sz;
    }

    public void skip_s(int sz) {
        if (remaining() < sz) {
            throw new IllegalArgumentException("invalid length for skipping");
        }
        pos += sz;
    }

    public abstract byte peek();

    public abstract byte peek_s();

    public abstract int u8();

    public abstract int u8_s();

    public abstract int u16();

    public abstract int u16_s();

    public abstract int u24();

    public abstract int u24_s();

    public abstract long u32();

    public abstract long u32_s();

    public abstract long u48();

    public abstract long u48_s();

    public abstract long i64();

    public abstract long i64_s();

    public abstract float f();

    public abstract float f_s();

    public abstract double d();

    public abstract double d_s();

    public long lei() {
        final int b0 = u8();
        if (b0 < 0xFC) {
            return b0;
        } else if (0xFC == b0) {
            return u16();
        } else if (0xFD == b0) {
            return u24();
        } else if (0xFE == b0) {
            return i64();
        }
        throw new IllegalArgumentException("Invalid length indicator: " + b0);
    }

    public long lei_s() {
        if (remaining() < 1) {
            throw new IllegalArgumentException("invalid length for length encoding");
        }
        final int b0 = u8();
        if (b0 < 0xFC) {
            return b0;
        } else if (0xFC == b0) {
            if (remaining() < 2) {
                throw new IllegalArgumentException("invalid length for length encoding");
            }
            return u16();
        } else if (0xFD == b0) {
            if (remaining() < 3) {
                throw new IllegalArgumentException("invalid length for length encoding");
            }
            return u24();
        } else if (0xFE == b0) {
            if (remaining() < 8) {
                throw new IllegalArgumentException("invalid length for length encoding");
            }
            return i64();
        }
        throw new IllegalArgumentException("Invalid length indicator: " + b0);
    }

    public abstract byte[] str();

    public abstract int str(byte[] out, int off);

    public abstract byte[] str(int sz);

    public abstract byte[] str_s(int sz);

    public abstract void str(int sz, byte[] out, int off);

    public abstract void str_s(int sz, byte[] out, int off);

    public byte[] le_str() {
        final int len = (int) lei();
        return str(len);
    }

    public byte[] le_str_s() {
        final int len = (int) lei_s();
        return str_s(len);
    }

    public int le_str(byte[] out, int off) {
        final int len = (int) lei();
        str(len, out, off);
        return len;
    }

    public int le_str_s(byte[] out, int off) {
        final int len = (int) lei_s();
        str_s(len, out, off);
        return len;
    }

    private static Decoder decodeNormalPacketLarge(long address, int length) {
        int consumed = 0, total = 0;
        while (consumed < length) {
            if (length - consumed < MysqlPacket.NORMAL_HEADER_SIZE) {
                throw new IllegalArgumentException("invalid length " + length + " for decoding");
            }
            final int u24 = (UnsafeBytes.UNSAFE.getShort(address + consumed) & 0xFFFF) + (
                (UnsafeBytes.UNSAFE.getByte(address + consumed + 2) & 0xFF) << 16);
            if (MysqlPacket.MAX_PAYLOAD_SIZE == u24) {
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + MysqlPacket.MAX_PAYLOAD_SIZE;
                total += MysqlPacket.MAX_PAYLOAD_SIZE;
            } else {
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + u24;
                total += u24;
                if (consumed != length) {
                    throw new IllegalArgumentException("invalid length " + length + " for decoding");
                }
                break;
            }
        }

        // allocate for the whole payload
        final byte[] payload = new byte[total];
        consumed = total = 0;
        int seq = 0;
        while (consumed < length) {
            final int u24 = (UnsafeBytes.UNSAFE.getShort(address + consumed) & 0xFFFF) + (
                (UnsafeBytes.UNSAFE.getByte(address + consumed + 2) & 0xFF) << 16);
            seq = UnsafeBytes.UNSAFE.getByte(address + consumed + 3) & 0xFF;
            if (MysqlPacket.MAX_PAYLOAD_SIZE == u24) {
                UnsafeBytes.UNSAFE.copyMemory(null, address + consumed + MysqlPacket.NORMAL_HEADER_SIZE, payload,
                    UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + total, MysqlPacket.MAX_PAYLOAD_SIZE);
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + MysqlPacket.MAX_PAYLOAD_SIZE;
                total += MysqlPacket.MAX_PAYLOAD_SIZE;
            } else {
                UnsafeBytes.UNSAFE.copyMemory(null, address + consumed + MysqlPacket.NORMAL_HEADER_SIZE, payload,
                    UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + total, u24);
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + u24;
                total += u24;
                break;
            }
        }

        final Decoder decoder = new UnsafeDecoder(payload, 0, total);
        decoder.lastSeq = seq;
        return decoder;
    }

    private static Decoder decodeNormalPacketLarge(byte[] buf, int base, int length) {
        int consumed = 0, total = 0;
        while (consumed < length) {
            if (length - consumed < MysqlPacket.NORMAL_HEADER_SIZE) {
                throw new IllegalArgumentException("invalid length " + length + " for decoding");
            }
            final int u24 =
                (UnsafeBytes.UNSAFE.getShort(buf, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + consumed) & 0xFFFF) + (
                    (UnsafeBytes.UNSAFE.getByte(buf, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + consumed + 2) & 0xFF)
                        << 16);
            if (MysqlPacket.MAX_PAYLOAD_SIZE == u24) {
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + MysqlPacket.MAX_PAYLOAD_SIZE;
                total += MysqlPacket.MAX_PAYLOAD_SIZE;
            } else {
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + u24;
                total += u24;
                if (consumed != length) {
                    throw new IllegalArgumentException("invalid length " + length + " for decoding");
                }
                break;
            }
        }

        // allocate for the whole payload
        final byte[] payload = new byte[total];
        consumed = total = 0;
        int seq = 0;
        while (consumed < length) {
            final int u24 =
                (UnsafeBytes.UNSAFE.getShort(buf, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + consumed) & 0xFFFF) + (
                    (UnsafeBytes.UNSAFE.getByte(buf, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + consumed + 2) & 0xFF)
                        << 16);
            seq = UnsafeBytes.UNSAFE.getByte(buf, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + consumed + 3) & 0xFF;
            if (MysqlPacket.MAX_PAYLOAD_SIZE == u24) {
                UnsafeBytes.UNSAFE.copyMemory(buf,
                    UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + consumed + MysqlPacket.NORMAL_HEADER_SIZE, payload,
                    UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + total, MysqlPacket.MAX_PAYLOAD_SIZE);
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + MysqlPacket.MAX_PAYLOAD_SIZE;
                total += MysqlPacket.MAX_PAYLOAD_SIZE;
            } else {
                UnsafeBytes.UNSAFE.copyMemory(buf,
                    UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + consumed + MysqlPacket.NORMAL_HEADER_SIZE, payload,
                    UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + total, u24);
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + u24;
                total += u24;
                break;
            }
        }

        final Decoder decoder = new UnsafeDecoder(payload, 0, total);
        decoder.lastSeq = seq;
        return decoder;
    }

    private static Decoder decodeNormalPacketLarge(ByteBuffer duplicated, int base, int length) {
        int consumed = 0, total = 0;
        while (consumed < length) {
            if (length - consumed < MysqlPacket.NORMAL_HEADER_SIZE) {
                throw new IllegalArgumentException("invalid length " + length + " for decoding");
            }
            final int u24 =
                (duplicated.getShort(base + consumed) & 0xFFFF) + ((duplicated.get(base + consumed + 2) & 0xFF) << 16);
            if (MysqlPacket.MAX_PAYLOAD_SIZE == u24) {
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + MysqlPacket.MAX_PAYLOAD_SIZE;
                total += MysqlPacket.MAX_PAYLOAD_SIZE;
            } else {
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + u24;
                total += u24;
                if (consumed != length) {
                    throw new IllegalArgumentException("invalid length " + length + " for decoding");
                }
            }
        }

        // allocate for the whole payload
        final byte[] payload = new byte[total];
        consumed = total = 0;
        int seq = 0;
        while (consumed < length) {
            final int u24 =
                (duplicated.getShort(base + consumed) & 0xFFFF) + ((duplicated.get(base + consumed + 2) & 0xFF) << 16);
            seq = duplicated.get(base + consumed + 3) & 0xFF;
            if (MysqlPacket.MAX_PAYLOAD_SIZE == u24) {
                duplicated.position(base + consumed + MysqlPacket.NORMAL_HEADER_SIZE).get(payload, total,
                    MysqlPacket.MAX_PAYLOAD_SIZE);
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + MysqlPacket.MAX_PAYLOAD_SIZE;
                total += MysqlPacket.MAX_PAYLOAD_SIZE;
            } else {
                duplicated.position(base + consumed + MysqlPacket.NORMAL_HEADER_SIZE).get(payload, total, u24);
                consumed += MysqlPacket.NORMAL_HEADER_SIZE + u24;
                total += u24;
                break;
            }
        }

        final Decoder decoder = new SimpleDecoder(ByteBuffer.wrap(payload), 0, total);
        decoder.lastSeq = seq;
        return decoder;
    }

    public static Decoder decodeNormalPacket(Slice slice) {
        if (slice.getValid() < MysqlPacket.NORMAL_HEADER_SIZE) {
            throw new IllegalArgumentException("invalid length " + slice.getValid() + " for decoding");
        }

        if (UnsafeBytes.UNSAFE != null) {
            final long address = slice.getAddress();
            if (address != 0) {
                final long base = address + slice.getConsumed();
                if (-1 == UnsafeBytes.UNSAFE.getShort(base) && -1 == UnsafeBytes.UNSAFE.getByte(base + 2)) {
                    return decodeNormalPacketLarge(base, slice.getValid());
                }
                final Decoder decoder = new NativeDecoder(base + MysqlPacket.NORMAL_HEADER_SIZE,
                    slice.getValid() - MysqlPacket.NORMAL_HEADER_SIZE);
                decoder.lastSeq = UnsafeBytes.UNSAFE.getByte(base + 3) & 0xFF;
                return decoder;
            }
            final byte[] heapBuf = slice.getHeapBuffer();
            if (heapBuf != null) {
                final int base = slice.getOffset() + slice.getConsumed();
                if (-1 == UnsafeBytes.UNSAFE.getShort(heapBuf,
                    UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base) && -1 == UnsafeBytes.UNSAFE.getByte(heapBuf,
                    UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + 2)) {
                    return decodeNormalPacketLarge(heapBuf, base, slice.getValid());
                }
                final Decoder decoder = new UnsafeDecoder(heapBuf, base + MysqlPacket.NORMAL_HEADER_SIZE,
                    slice.getValid() - MysqlPacket.NORMAL_HEADER_SIZE);
                decoder.lastSeq =
                    UnsafeBytes.UNSAFE.getByte(heapBuf, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + base + 3) & 0xFF;
                return decoder;
            }
        }

        // or simple decoder
        final ByteBuffer buffer = slice.duplicateBuffer();
        final int base = buffer.position() + slice.getConsumed();
        if (-1 == buffer.getShort(base) && -1 == buffer.get(base + 2)) {
            return decodeNormalPacketLarge(buffer, base, slice.getValid());
        }
        final Decoder decoder = new SimpleDecoder(buffer, base, slice.getValid());
        decoder.lastSeq = buffer.get(base + 3) & 0xFF;
        return decoder;
    }
}
