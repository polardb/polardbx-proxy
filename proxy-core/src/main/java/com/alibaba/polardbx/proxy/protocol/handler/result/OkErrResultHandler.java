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
import com.alibaba.polardbx.proxy.protocol.command.ErrPacket;
import com.alibaba.polardbx.proxy.protocol.command.OkPacket;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.handler.MysqlForwarder;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@Getter
public class OkErrResultHandler extends ResultHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(OkErrResultHandler.class);

    private OkPacket ok; // valid when state is Ok
    private ErrPacket err; // valid when state is Error

    public OkErrResultHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                              MysqlForwarder forwarder, ResultCallback stateCallback) {
        super(contextReference, scheduler, forwarder, stateCallback);
        setTag("OkErrResultHandler");
    }

    @Override
    public synchronized boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        final int peek = decoder.peek_s() & 0xFF;
        if (ResultState.Init == state) {
            final BackendContext context = contextReference.getAcquire();
            assert context != null;
            if (0x00 == peek) {
                // ok
                final OkPacket tmp = new OkPacket();
                tmp.decode(decoder, context.getCapabilities());
                ok = tmp;
                // auto update status in context
                context.updateStatus(tmp.getWarnings(), tmp.getStatusFlags());
                updateState(ResultState.OK);
                return forwardPacket(packet, decoder);
            } else if (0xFF == peek) {
                // error
                final ErrPacket tmp = new ErrPacket();
                tmp.decode(decoder, context.getCapabilities());
                err = tmp;
                updateState(ResultState.Error);
                return forwardPacket(packet, decoder);
            }
        }
        throw new RuntimeException("Bad state: " + state + " peek bytes: 0x" + Integer.toHexString(peek));
    }
}
