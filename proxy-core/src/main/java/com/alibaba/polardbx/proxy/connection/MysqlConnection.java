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

package com.alibaba.polardbx.proxy.connection;

import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.net.NIOConnection;
import com.alibaba.polardbx.proxy.net.NIOProcessor;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.AutoCloseableContainer;
import com.alibaba.polardbx.proxy.utils.Slice;
import com.alibaba.polardbx.proxy.utils.UnsafeBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public abstract class MysqlConnection extends NIOConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlConnection.class);

    protected boolean compressedPacket = false;
    protected int packetHeaderSize = MysqlPacket.NORMAL_HEADER_SIZE;

    public MysqlConnection(SocketChannel channel, NIOProcessor processor, boolean connected) {
        super(channel, processor, connected, FastConfig.maxAllowedPacket, MysqlPacket.DEFAULT_RESERVE_BUFFER_SIZE);
    }

    public void setCompressedPacket(boolean compressedPacket) {
        if (compressedPacket) {
            // todo
            throw new UnsupportedOperationException();
        }
        this.compressedPacket = compressedPacket;
        this.packetHeaderSize = compressedPacket ? MysqlPacket.COMPRESSED_HEADER_SIZE : MysqlPacket.NORMAL_HEADER_SIZE;
    }

    private static int u24(ByteBuffer buf, int offset) {
        if (!buf.isDirect() && UnsafeBytes.UNSAFE != null) {
            final byte[] array = buf.array();
            return (UnsafeBytes.UNSAFE.getShort(array, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + offset) & 0xFFFF) +
                ((UnsafeBytes.UNSAFE.getByte(array, UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + offset + 2) & 0xFF) << 16);
        } else {
            assert ByteOrder.LITTLE_ENDIAN == buf.order();
            return (buf.getShort(offset) & 0xFFFF) + ((buf.get(offset + 2) & 0xFF) << 16);
        }
    }

    @Override
    protected int probeLength(ByteBuffer buf, int offset, int length) {
        if (length < packetHeaderSize) {
            return -1;
        }
        int totalSize = packetHeaderSize;
        int payloadSize = u24(buf, offset);
        totalSize += payloadSize;
        // dealing large packet
        while (MysqlPacket.MAX_PAYLOAD_SIZE == payloadSize) {
            // extra packet needed
            if (totalSize > Integer.MAX_VALUE - MysqlPacket.MAX_PAYLOAD_SIZE - packetHeaderSize) {
                throw new RuntimeException("Packet too long.");
            }
            totalSize += packetHeaderSize;
            if (length < totalSize) {
                return totalSize;
            }
            payloadSize = u24(buf, offset + totalSize - packetHeaderSize);
            totalSize += payloadSize;
        }
        return totalSize;
    }

    protected abstract boolean handleAndTakePacket(Slice packet, Decoder decoder, Encoder encoder) throws Exception;

    protected abstract void handleFinish() throws Exception;

    @Override
    protected void onPacket(AutoCloseableContainer<Slice> packets) {
        try (final Encoder encoder = Encoder.create(processor.getBufferPool(), this::write)) {
            // free or release ownership instead of remove from container(impl of container is ArrayList)
            Throwable err = null;
            for (Slice packet : packets) {
                try {
                    // handle and take ownership, or leave and free in finally
                    if (null == err) {
                        final Decoder decoder;
                        if (compressedPacket) {
                            // todo dealing compressed
                            throw new UnsupportedOperationException();
                        } else {
                            decoder = Decoder.decodeNormalPacket(packet);
                        }

                        encoder.setSeq(decoder.getLastSeq() + 1);
                        final boolean taken = handleAndTakePacket(packet, decoder, encoder);
                        if (taken) {
                            packet = null;
                        }
                    }
                } catch (Throwable t) {
                    err = t;
                } finally {
                    if (packet != null) {
                        packet.close();
                    }
                }
            }
            packets.clear();

            // throw error
            if (err != null) {
                throw err;
            }

            // flush all packets
            encoder.flush();
        } catch (Throwable t) {
            LOGGER.error("process packet failed", t);
            close();
        } finally {
            // finish callback
            try {
                handleFinish();
            } catch (Throwable t) {
                LOGGER.error("process finish failed", t);
                close();
            }
        }
    }

    @Override
    public String toString() {
        try {
            return "MySQL-Connection " + connectionString();
        } catch (Throwable ignore) {
            return "MySQL-Connection";
        }
    }
}
