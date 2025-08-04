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

package com.alibaba.polardbx.proxy.protocol.handler;

import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.common.MysqlProtocolHandler;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.AutoCloseableContainer;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MysqlForwarder extends MysqlProtocolHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlForwarder.class);

    @Getter
    private final FrontendConnection connection;
    @Getter
    private final FrontendContext context;
    private final AutoCloseableContainer<Slice> packets = new AutoCloseableContainer<>(16);

    public MysqlForwarder(FrontendConnection connection, FrontendContext context) {
        setTag("MysqlForwarder of " + connection);
        this.connection = connection;
        this.context = context;
    }

    // Note: synchronized needed because write may fast and another request is forwarding and add packet before close in handleFinish.

    // should invoke this in ResultHandler with the check of LSN
    public synchronized void push(Collection<byte[]> bytesPackets) {
        try (final Encoder encoder = Encoder.create(connection.getProcessor().getBufferPool(),
            c -> {
                packets.addAll(c);
                c.clear(); // leave ownership to forwarder
            })) {
            for (final byte[] bytes : bytesPackets) {
                encoder.pkt(bytes);
            }
            encoder.flush();
        } catch (Throwable t) {
            LOGGER.error("Failed to push pending packets", t);
            connection.close();
        }
    }

    @Override
    public synchronized boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        if (leakCheckClosed.getPlain()) {
            throw new IllegalStateException("MysqlForwarder is closed");
        }
        packets.add(packet);
        return true;
    }

    @Override
    public synchronized void handleFinish() {
        if (leakCheckClosed.getPlain()) {
            throw new IllegalStateException("MysqlForwarder is closed");
        }
        try (packets) {
            connection.write(packets);
        } catch (Throwable t) {
            LOGGER.error("Failed to write packets", t);
            connection.close();
        }
    }

    @Override
    public synchronized void close() {
        packets.close();
        // because we use leakCheckClosed to check context closed, so finalize the leak check in synchronize block
        super.close();
    }
}
