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

package com.alibaba.polardbx.proxy.echo;

import com.alibaba.polardbx.proxy.net.NIOConnection;
import com.alibaba.polardbx.proxy.net.NIOProcessor;
import com.alibaba.polardbx.proxy.utils.AutoCloseableContainer;
import com.alibaba.polardbx.proxy.utils.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class EchoConnection extends NIOConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(EchoConnection.class);

    public EchoConnection(SocketChannel channel, NIOProcessor processor) {
        super(channel, processor, true, 64 * 1024, 128);
    }

    @Override
    public int probeLength(ByteBuffer buf, int offset, int length) {
        return length;
    }

    @Override
    protected void onEstablished() {
    }

    @Override
    public void onPacket(AutoCloseableContainer<Slice> packets) {
        try {
            write(packets);
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
        }
    }

    @Override
    public void onFatalError(Throwable t) {
        LOGGER.error(t.getMessage(), t);
    }
}
