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

package com.alibaba.polardbx.proxy.net;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class NIOAcceptor extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOAcceptor.class);

    @Getter
    private final int port;
    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final NIOWorker worker;
    private final NIOConnectionFactory factory;

    public NIOAcceptor(String name, int port, NIOWorker worker, NIOConnectionFactory factory)
        throws IOException {
        this.port = port;
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.socket().bind(new InetSocketAddress(port), 65535);
        this.serverChannel.configureBlocking(false);
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.worker = worker;
        this.factory = factory;

        super.setName(name);
        // note acceptor thread is not daemon thread
    }

    private void accept() {
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.configureBlocking(false);
            final NIOProcessor processor = worker.getProcessor();
            NIOConnection c = factory.accept(channel, processor);
            try {
                processor.postRegister(c);
                c = null;
            } finally {
                if (c != null) {
                    c.close(); // prevent leak
                }
            }
        } catch (Throwable e) {
            closeChannel(channel);
            LOGGER.info(e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        for (; ; ) {
            try {
                selector.select(1000L);
                final Set<SelectionKey> keys = selector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (key.isValid() && key.isAcceptable()) {
                            accept();
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Throwable e) {
                LOGGER.warn(getName(), e);
            }
        }
    }

    private static void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        final Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignore) {
            }
        }
        try {
            channel.close();
        } catch (IOException ignore) {
        }
    }

    public synchronized void offline() {
        try {
            this.serverChannel.close();
            this.selector.close();
            LOGGER.info("{} offline success {}", super.getName(), port);
        } catch (IOException e) {
            LOGGER.error("offline error", e);
        }
    }

    @Override
    public String toString() {
        return "NIOAcceptor{" +
            "name=" + super.getName() +
            ", port=" + port +
            ", worker=" + worker +
            ", factory=" + factory +
            '}';
    }
}
