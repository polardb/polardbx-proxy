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

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.ProxyServer;
import com.alibaba.polardbx.proxy.cluster.GlobalMock;
import com.alibaba.polardbx.proxy.cluster.InstanceVersion;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.MysqlContext;
import com.alibaba.polardbx.proxy.net.NIOProcessor;
import com.alibaba.polardbx.proxy.protocol.command.StatusFlags;
import com.alibaba.polardbx.proxy.protocol.common.MysqlServerState;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.connection.HandshakeV10;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.protocol.handler.FrontendAuthenticator;
import com.alibaba.polardbx.proxy.protocol.handler.FrontendCommandHandler;
import com.alibaba.polardbx.proxy.utils.RandomUtil;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class FrontendConnection extends MysqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendConnection.class);
    public static final Set<FrontendConnection> CONNECTIONS = new ConcurrentSkipListSet<>();

    private final AtomicBoolean resourceClosed = new AtomicBoolean(false);

    // connection context
    @Getter
    private final FrontendContext context;

    // handler
    private volatile FrontendAuthenticator authenticator;
    private volatile FrontendCommandHandler commander;

    public FrontendConnection(SocketChannel channel, NIOProcessor processor) {
        super(channel, processor, true);
        this.context =
            new FrontendContext(remoteAddress(), ProxyServer.getInstance().getAcceptIdGenerator().nextAcceptId(),
                Capabilities.getBaseCapabilities());
        try {
            if (GlobalMock.forceFrontendNoDeprecateEof()) {
                this.context.removeCapability(Capabilities.CLIENT_DEPRECATE_EOF);
            }
            final boolean ignore = this.context.setCharset(MysqlContext.DEFAULT_CHARSET_INDEX);
            final byte[] seed = RandomUtil.randomBytes(21);
            seed[20] = 0; // with 0 terminator
            this.authenticator = new FrontendAuthenticator(context, seed);
        } catch (Throwable t) {
            // prevent leak
            this.context.close();
            if (this.authenticator != null) {
                this.authenticator.close();
                this.authenticator = null;
            }
            throw t;
        }

        // add to global set
        CONNECTIONS.add(this);
    }

    @Override
    protected void onEstablished() {
        // send handshake packet
        final HandshakeV10 handshake = new HandshakeV10();
        handshake.setVersion(InstanceVersion.buildVersion());
        handshake.setConnectionId(context.getConnectionId());
        handshake.setAuthPluginData(authenticator.getSeed());
        handshake.setCapabilityFlags(context.getCapabilities());
        // all charset are same and default(utf8mb4) when here, send any one
        handshake.setCharacterSet((byte) context.getClientCharsetIndex());
        handshake.setStatusFlags((short) StatusFlags.SERVER_STATUS_AUTOCOMMIT);
        handshake.setAuthPluginName(FrontendAuthenticator.AUTH_METHOD_BYTES);

        // prepare for send
        try (final Encoder encoder = Encoder.create(processor.getBufferPool(), this::write)) {
            handshake.encode(encoder, context.getCapabilities());
            encoder.flush();
            context.setState(MysqlServerState.Greeting);
        } catch (Throwable t) {
            // port probe on public cloud, just ignore peer reset before actual handshake established
            LOGGER.debug("send handshake packet failed", t);
            close();
        }
    }

    @Override
    protected boolean handleAndTakePacket(Slice packet, Decoder decoder, Encoder encoder) throws Exception {
        final boolean taken;
        // optimistic read
        final FrontendAuthenticator auth = authenticator;
        if (auth != null) {
            taken = auth.handleAndTakePacket(packet, decoder, encoder);
            if (MysqlServerState.Authenticated == context.getState()) {
                // auth success
                auth.handleFinish();
                auth.close();
                authenticator = null;
            }
        } else {
            FrontendCommandHandler handler = commander;
            if (null == handler) {
                // use resourceClosed as lock
                synchronized (resourceClosed) {
                    handler = commander;
                    if (null == handler) {
                        if (resourceClosed.getPlain()) {
                            throw new IllegalStateException("connection is closed");
                        }
                        commander = handler = new FrontendCommandHandler(this, context);
                    }
                }
            }
            taken = handler.handleAndTakePacket(packet, decoder, encoder);
        }
        return taken;
    }

    @Override
    protected void handleFinish() {
        // invoke handle finish with optimistic read
        final FrontendAuthenticator auth = authenticator;
        if (auth != null) {
            auth.handleFinish();
        }
        final FrontendCommandHandler handler = commander;
        if (handler != null) {
            handler.handleFinish();
        }
        // close connection if any error occurs
        if (MysqlServerState.Closed == context.getState()) {
            close();
        }
    }

    @Override
    protected void onFatalError(Throwable t) {
        LOGGER.error("fatal error on {}", this, t);
        close();
    }

    @Override
    public void close() {
        final boolean needClose;
        final FrontendAuthenticator auth;
        final FrontendCommandHandler handler;
        // use resourceClosed as lock
        synchronized (resourceClosed) {
            if (resourceClosed.compareAndSet(false, true)) {
                needClose = true;
                // get resources and free outside
                auth = authenticator;
                authenticator = null;
                handler = commander;
                commander = null;
            } else {
                assert null == authenticator;
                assert null == commander;
                needClose = false;
                auth = null;
                handler = null;
            }
        }

        if (needClose) {
            // close all handlers and context in async task to prevent any potential deadlock
            ProxyExecutor.getInstance().getExecutor().submit(() -> {
                try {
                    if (auth != null) {
                        auth.close();
                    }
                    if (handler != null) {
                        handler.close();
                    }
                    context.close();
                } catch (Throwable t) {
                    LOGGER.error("close connection {} free resources failed", this, t);
                }
            });
        }

        // remove from global set
        CONNECTIONS.remove(this);

        // finalize the TCP close
        super.close();
    }

    @Override
    public String toString() {
        try {
            return "Frontend-Connection " + connectionString();
        } catch (Throwable ignore) {
            return "Frontend-Connection";
        }
    }
}
