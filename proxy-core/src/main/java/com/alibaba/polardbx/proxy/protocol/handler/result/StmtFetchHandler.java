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

package com.alibaba.polardbx.proxy.protocol.handler.result;

import com.alibaba.polardbx.proxy.callback.ResultCallback;
import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.MysqlContext;
import com.alibaba.polardbx.proxy.protocol.command.EofPacket;
import com.alibaba.polardbx.proxy.protocol.command.ErrPacket;
import com.alibaba.polardbx.proxy.protocol.command.OkPacket;
import com.alibaba.polardbx.proxy.protocol.command.StatusFlags;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.protocol.handler.MysqlForwarder;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class StmtFetchHandler extends ResultHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StmtFetchHandler.class);

    @Getter
    private ErrPacket err; // valid when state is Error
    @Getter
    private EofPacket eof; // valid when state is EOF
    @Getter
    private OkPacket ok; // valid when state is Ok

    // flow control read resume callback(strong ref here to prevent GC)
    private final Runnable writeResumeCallback = () -> {
        final BackendConnectionWrapper backend = scheduler.getBackend();
        if (backend != null && forwarder != null && !forwarder.getConnection().isWriteBlocking()) {
            backend.enableRead();
        }
    };

    public StmtFetchHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                            MysqlForwarder forwarder, ResultCallback stateCallback) {
        super(contextReference, scheduler, forwarder, stateCallback);
        setTag("CursorFetchHandler");
    }

    @Override
    public synchronized boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        final BackendContext context = contextReference.getAcquire();
        assert context != null;

        // early abort on bad state
        if (ResultState.Abort == state) {
            throw new IllegalStateException("CursorFetchHandler is in Abort state");
        }

        // highest level error check
        final int peek = decoder.peek_s() & 0xFF;
        if (0xFF == peek) {
            // error
            final ErrPacket tmp = new ErrPacket();
            tmp.decode(decoder, context.getCapabilities());
            err = tmp;
            updateState(ResultState.Error);
            return forwardPacket(packet, decoder);
        }

        // or check with state
        if (ResultState.Init == state) {
            // deal rows
            updateState(ResultState.Rows);
        } else if (state != ResultState.Rows) {
            throw new RuntimeException("Bad state: " + state + " peek bytes: 0x" + Integer.toHexString(peek));
        }

        // error checked before, now just check eof
        if (0xFE == peek && decoder.remaining() < 9) {
            // eof(ok packet of select result should less than 9 bytes)
            final int warnings, statusFlags;
            if (context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                final OkPacket tmp = new OkPacket();
                tmp.decode(decoder, context.getCapabilities());
                warnings = tmp.getWarnings();
                statusFlags = tmp.getStatusFlags();
                ok = tmp;
                // auto update status in context
                context.updateStatus(tmp.getWarnings(), tmp.getStatusFlags());
                if ((tmp.getStatusFlags() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) != 0) {
                    more = new QueryResultHandler(contextReference, scheduler, forwarder, stateCallback,
                        packetForwarded, packetDroppedByLsn, true, this);
                }
                updateState(ResultState.OK);
            } else {
                final EofPacket tmp = new EofPacket();
                tmp.decode(decoder, context.getCapabilities());
                warnings = tmp.getWarnings();
                statusFlags = tmp.getStatusFlags();
                eof = tmp;
                // auto update status in context
                context.updateStatus(tmp.getWarnings(), tmp.getStatusFlags());
                if ((tmp.getStatusFlags() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) != 0) {
                    more = new QueryResultHandler(contextReference, scheduler, forwarder, stateCallback,
                        packetForwarded, packetDroppedByLsn, true, this);
                }
                updateState(ResultState.EOF);
            }

            // rebuild end packet if CLIENT_DEPRECATE_EOF mismatch
            if (forwarder != null) {
                final MysqlContext forwardContext = forwarder.getContext();
                if (context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF) != forwardContext.hasCapability(
                    Capabilities.CLIENT_DEPRECATE_EOF)) {
                    try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                        try (final Encoder encoder = Encoder.create(null, output)) {
                            final ByteBuffer buffer = packet.duplicateBuffer();
                            final int realSeqPosition = buffer.position() + 3;
                            encoder.setSeq(buffer.get(realSeqPosition) & 0xFF);
                            if (forwardContext.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                                // build ok packet
                                final OkPacket tmp = new OkPacket();
                                tmp.setEOF(true);
                                tmp.setWarnings(warnings);
                                tmp.setStatusFlags(statusFlags);
                                tmp.encode(encoder, forwardContext.getCapabilities());
                            } else {
                                // build eof packet
                                final EofPacket tmp = new EofPacket();
                                tmp.setWarnings(warnings);
                                tmp.setStatusFlags(statusFlags);
                                tmp.encode(encoder, forwardContext.getCapabilities());
                            }
                            encoder.flush();
                        }
                        // push this rewrite packet
                        pushPackets(Collections.singletonList(output.getBytes()));
                    } catch (IOException e) {
                        throw new RuntimeException("Error when construct fields EOF packet", e);
                    }
                    // and drop original packet
                    return false; // not taken
                }
            }
        }

        // just forward
        return forwardPacket(packet, decoder);
    }

    @Override
    public void handleFinish() {
        super.handleFinish();
        // done flush on forwarder

        // flow control
        if (forwarder != null) {
            final FrontendConnection frontend = forwarder.getConnection();
            if (frontend.isWriteBlocking()) {
                // back pressure on backend needed
                final BackendConnectionWrapper backend = scheduler.getBackend();
                if (backend != null) {
                    backend.disableRead();
                    frontend.registerWriteResumeListener(writeResumeCallback);
                    // recheck resumed
                    if (!frontend.isWriteBlocking()) {
                        backend.enableRead();
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup() {
        super.cleanup();

        // remove flow control listener when done
        if (forwarder != null && getState().isDone()) {
            forwarder.getConnection().removeWriteResumeListener(writeResumeCallback);
        }
    }
}
