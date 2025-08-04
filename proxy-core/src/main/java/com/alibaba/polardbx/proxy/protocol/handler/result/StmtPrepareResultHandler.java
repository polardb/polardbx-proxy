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
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.protocol.command.EofPacket;
import com.alibaba.polardbx.proxy.protocol.command.ErrPacket;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.protocol.handler.MysqlForwarder;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtPrepareOk;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class StmtPrepareResultHandler extends ResultHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StmtPrepareResultHandler.class);

    @Getter
    private ComStmtPrepareOk ok; // valid when state is OK
    @Getter
    private ErrPacket err; // valid when state is Error

    @Getter
    private List<ColumnDefinition41> parameters, fields;

    // blocking head & fields for forwarding(this enabled error packet when abort state)
    private final List<byte[]> pendingPackets;
    private int compatibleSeqPatch = 0;

    public StmtPrepareResultHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                                    MysqlForwarder forwarder, ResultCallback stateCallback) {
        super(contextReference, scheduler, forwarder, stateCallback);
        setTag("StmtPrepareResultHandler");
        pendingPackets = null == forwarder ? null : new ArrayList<>();
    }

    private boolean dealingPushEOF(Slice packet) {
        assert contextReference.getAcquire().hasCapability(Capabilities.CLIENT_DEPRECATE_EOF);
        if (forwarder != null) {
            assert pendingPackets != null;
            final FrontendContext forwardContext = forwarder.getContext();
            if (!forwardContext.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                // push this packet(last one of parameters or fields)
                pendingPackets.add(packet.dump());
                // need add parameters/fields EOF and add seq of each packet following
                final ByteBuffer buffer = packet.duplicateBuffer();
                final int realSeqPosition = buffer.position() + 3;
                final int eofSeq = (buffer.get(realSeqPosition) & 0xFF) + 1;
                try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                    try (final Encoder encoder = Encoder.create(null, output)) {
                        encoder.setSeq(eofSeq);
                        final EofPacket tmp = new EofPacket();
                        tmp.setStatusFlags(forwardContext.genStatusFlags(false));
                        tmp.encode(encoder, forwardContext.getCapabilities());
                        encoder.flush();
                    }
                    pendingPackets.add(output.getBytes());
                } catch (IOException e) {
                    throw new RuntimeException("Error when construct fields EOF packet", e);
                }
                compatibleSeqPatch += 1;
                return false;
            }
            // same with CLIENT_DEPRECATE_EOF, just ignore
            return true;
        }
        return false;
    }

    private void patchSequence(ByteBuffer duplicated, int nowSeq) {
        int pos = duplicated.position() + 3, limit = duplicated.limit();
        do {
            duplicated.put(pos, (byte) nowSeq++);
            pos += MysqlPacket.NORMAL_HEADER_SIZE + MysqlPacket.MAX_PAYLOAD_SIZE;
        } while (pos < limit);
    }

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        final BackendContext context = contextReference.getAcquire();
        assert context != null;

        // early abort on bad state
        if (ResultState.Abort == state) {
            throw new IllegalStateException("StmtPrepareResultHandler is in Abort state");
        }

        // patch seq for deprecate EOF compatible
        if (compatibleSeqPatch != 0) {
            final ByteBuffer buffer = packet.duplicateBuffer();
            final int realSeqPosition = buffer.position() + 3;
            assert forwarder != null;
            final int nowSeq = (buffer.get(realSeqPosition) & 0xFF) + compatibleSeqPatch;
            patchSequence(buffer, nowSeq);
        }

        // highest level error check
        final int peek = decoder.peek_s() & 0xFF;
        if (0xFF == peek) {
            // error
            final ErrPacket tmp = new ErrPacket();
            tmp.decode(decoder, context.getCapabilities());
            err = tmp;
            if (pendingPackets != null && !pendingPackets.isEmpty()) {
                pushPackets(pendingPackets);
                pendingPackets.clear();
            }
            updateState(ResultState.Error);
            return forwardPacket(packet, decoder);
        }

        // or check with state

        final boolean forwardPacket;
        switch (state) {
        case Init: {
            // error checked before
            final ComStmtPrepareOk tmp = new ComStmtPrepareOk();
            tmp.decode(decoder, context.getCapabilities());
            // auto update status in context
            context.setWarnings(tmp.getWarningCount());
            ok = tmp;
            if (tmp.getNumParams() > 0 && tmp.isMetadataFollows()) {
                parameters = new ArrayList<>(tmp.getNumParams());
            }
            if (tmp.getNumColumns() > 0 && tmp.isMetadataFollows()) {
                fields = new ArrayList<>(tmp.getNumColumns());
            }
            // Note: only eof when parameters > 0 or fields > 0
            if (parameters != null) {
                updateState(ResultState.Parameters);
            } else if (fields != null) {
                updateState(ResultState.Fields);
            } else {
                // no param and no fields
                updateState(ResultState.OK);
            }
            forwardPacket = forwarder != null;
        }
        break;

        case Parameters: {
            assert parameters != null && ok != null;
            final ColumnDefinition41 definition = new ColumnDefinition41();
            definition.decode(decoder, context.getCapabilities());
            parameters.add(definition);
            if (parameters.size() >= ok.getNumParams()) {
                if (!context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                    updateState(ResultState.ParametersEOF);
                    forwardPacket = forwarder != null;
                } else {
                    forwardPacket = dealingPushEOF(packet);
                    if (fields != null) {
                        updateState(ResultState.Fields);
                    } else {
                        updateState(ResultState.OK);
                    }
                }
            } else {
                forwardPacket = forwarder != null;
            }
        }
        break;

        case Fields: {
            assert fields != null && ok != null;
            final ColumnDefinition41 definition = new ColumnDefinition41();
            definition.decode(decoder, context.getCapabilities());
            fields.add(definition);
            if (fields.size() >= ok.getNumColumns()) {
                if (!context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                    updateState(ResultState.FieldsEOF);
                    forwardPacket = forwarder != null;
                } else {
                    forwardPacket = dealingPushEOF(packet);
                    // all done
                    updateState(ResultState.OK);
                }
            } else {
                forwardPacket = forwarder != null;
            }
        }
        break;

        case ParametersEOF:
        case FieldsEOF: {
            final EofPacket eof = new EofPacket();
            eof.decode(decoder, context.getCapabilities());
            // auto update status in context
            context.updateStatus(eof.getWarnings(), eof.getStatusFlags());
            // just ignore this packet
            if (ResultState.ParametersEOF == state && fields != null) {
                updateState(ResultState.Fields);
            } else {
                updateState(ResultState.OK);
            }
            if (forwarder != null) {
                if (forwarder.getContext().hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                    // drop it and each following packets' seq sub 1
                    forwardPacket = false;
                    compatibleSeqPatch -= 1;
                } else {
                    forwardPacket = true;
                }
            } else {
                forwardPacket = false;
            }
        }
        break;

        default:
            throw new RuntimeException(
                "Bad state: " + state + " peek bytes: 0x" + Integer.toHexString(peek));
        }

        // dealing pending and packet
        if (ResultState.OK == state) {
            // forward all pending packets
            if (pendingPackets != null && !pendingPackets.isEmpty()) {
                pushPackets(pendingPackets);
                pendingPackets.clear();
            }
            return forwardPacket && forwardPacket(packet, decoder);
        } else if (forwardPacket) {
            pendingPackets.add(packet.dump()); // make a heap copy
        }
        return false; // not taken
    }

    public boolean patchStatementId(int statementId) {
        if (ResultState.OK == state && !pendingPackets.isEmpty()) {
            final ByteBuffer buffer = ByteBuffer.wrap(pendingPackets.get(0)).order(ByteOrder.LITTLE_ENDIAN);
            final int pos = buffer.position() + 5;
            buffer.putInt(pos, statementId);
            return true;
        }
        return false;
    }
}
