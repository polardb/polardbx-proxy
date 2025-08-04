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
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.protocol.command.EofPacket;
import com.alibaba.polardbx.proxy.protocol.command.ErrPacket;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.handler.MysqlForwarder;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Getter
public class FieldListResultHandler extends ResultHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldListResultHandler.class);

    // result info
    private List<ColumnDefinition41> fields; // may null when no filed meta
    private ErrPacket err; // valid when state is Error
    private EofPacket eof; // valid when state is EOF

    private final List<byte[]> pendingPackets;

    public FieldListResultHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                                  MysqlForwarder forwarder, ResultCallback stateCallback) {
        super(contextReference, scheduler, forwarder, stateCallback);
        setTag("FieldListResultHandler");
        pendingPackets = null == forwarder ? null : new ArrayList<>();
    }

    @Override
    public synchronized boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        final BackendContext context = contextReference.getAcquire();
        assert context != null;

        // early abort on bad state
        if (ResultState.Abort == state) {
            throw new IllegalStateException("FieldListResultHandler is in Abort state");
        }

        // highest level error check
        final int peek = decoder.peek_s() & 0xFF;
        if (0xFF == peek) {
            // error
            final ErrPacket tmp = new ErrPacket();
            tmp.decode(decoder, context.getCapabilities());
            err = tmp;
            updateState(ResultState.Error);
            assert null == pendingPackets || pendingPackets.isEmpty(); // fields or just error
            return forwardPacket(packet, decoder);
        }

        // or check with state
        final boolean pending;
        switch (state) {
        case Init:
            updateState(ResultState.Fields);
            // fall through
        case Fields:
            // check EOF
            if (0xFE == peek && decoder.remaining() < 9) {
                final EofPacket tmp = new EofPacket();
                tmp.decode(decoder, context.getCapabilities());
                eof = tmp;
                // auto update status in context
                context.updateStatus(tmp.getWarnings(), tmp.getStatusFlags());
                updateState(ResultState.EOF);
                pending = false; // can push all and send
            } else {
                if (null == fields) {
                    fields = new ArrayList<>();
                }
                final ColumnDefinition41 definition = new ColumnDefinition41();
                definition.decode(decoder, context.getCapabilities());
                fields.add(definition);
                pending = true;
            }
            break;

        default:
            throw new RuntimeException("Bad state: " + state + " peek bytes: 0x" + Integer.toHexString(peek));
        }

        if (pending) {
            pendingPackets.add(packet.dump()); // make a heap copy
            return false; // not taken
        } else {
            // push all pending packets
            if (pendingPackets != null && !pendingPackets.isEmpty()) {
                pushPackets(pendingPackets);
                pendingPackets.clear();
            }
            return forwardPacket(packet, decoder);
        }
    }
}
