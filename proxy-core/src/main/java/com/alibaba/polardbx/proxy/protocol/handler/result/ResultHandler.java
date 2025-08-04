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
import com.alibaba.polardbx.proxy.protocol.common.MysqlProtocolHandler;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.handler.MysqlForwarder;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ResultHandler extends MysqlProtocolHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultHandler.class);

    @Getter
    protected final AtomicReference<BackendContext> contextReference;
    protected final Scheduler scheduler;
    protected final MysqlForwarder forwarder;
    protected final ResultCallback stateCallback;

    // state, protected by this object's synchronize
    protected ResultState state = ResultState.Init;

    // flag to check whether packet has been forwarded
    @Getter
    protected boolean packetForwarded = false;
    private boolean lsnChecked = false;
    // flag to check whether packet has been dropped by out-dated LSN
    @Getter
    protected boolean packetDroppedByLsn = false;

    // flag for system internal request
    @Getter
    @Setter
    protected boolean systemRequest = false;

    protected ResultHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                            MysqlForwarder forwarder, ResultCallback stateCallback) {
        setTag("ResultHandler");
        this.contextReference = contextReference;
        this.scheduler = scheduler;
        this.forwarder = forwarder;
        this.stateCallback = stateCallback;
    }

    // for multi-result query's sub results
    protected ResultHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                            MysqlForwarder forwarder, ResultCallback stateCallback, boolean packetForwarded,
                            boolean packetDroppedByLsn) {
        this(contextReference, scheduler, forwarder, stateCallback);
        this.packetForwarded = packetForwarded;
        this.lsnChecked = true;
        this.packetDroppedByLsn = packetDroppedByLsn;
    }

    // more result, protected by this object's synchronize
    protected ResultHandler more;

    protected synchronized void updateState(ResultState state) {
        final ResultState before = this.state;
        this.state = state;
        if (stateCallback != null) {
            stateCallback.onStateChangeWithinLock(this, before, state);
        }
    }

    public synchronized ResultState getState() {
        return state;
    }

    protected void pushPackets(Collection<byte[]> packets) {
        if (lsnChecked) {
            if (packetDroppedByLsn) {
                return;
            }
        } else {
            lsnChecked = true; // mark checked
            if (scheduler != null && scheduler.getSpecificLsn() != null) {
                final BackendContext backendContext = contextReference.getAcquire();
                assert backendContext != null;
                if (!backendContext.isUpToDate()) {
                    packetDroppedByLsn = true; // mark should drop
                    return;
                }
            }
        }
        if (forwarder != null) {
            if (!packetForwarded) {
                packetForwarded = true;
            }
            forwarder.push(packets);
        }
    }

    protected boolean forwardPacket(Slice packet, Decoder decoder) {
        if (lsnChecked) {
            if (packetDroppedByLsn) {
                return false; // not taken and not forwarded
            }
        } else {
            lsnChecked = true; // mark checked
            if (scheduler != null && scheduler.getSpecificLsn() != null) {
                final BackendContext backendContext = contextReference.getAcquire();
                assert backendContext != null;
                if (!backendContext.isUpToDate()) {
                    packetDroppedByLsn = true; // mark should drop
                    return false; // not taken and not forwarded
                }
            }
        }
        if (forwarder != null) {
            if (!packetForwarded) {
                packetForwarded = true;
            }
            return forwarder.handleAndTakePacket(packet, decoder);
        }
        return false; // not taken and not forwarded
    }

    @Override
    public void handleFinish() {
        if (forwarder != null) {
            forwarder.handleFinish();
        }
    }

    public synchronized boolean isDone() {
        return state.isDone();
    }

    public synchronized ResultHandler hasMore() {
        if (!isDone()) {
            throw new RuntimeException("Not finished yet.");
        }
        return more;
    }

    // for customized cleanup
    protected void cleanup() {
    }

    @Override
    public void close() {
        final ResultState lastValidState, finalState;
        synchronized (this) {
            // prevent any pending data in forwarder
            try {
                handleFinish();
            } catch (Throwable t) {
                LOGGER.error("Error while finishing result handler when close.", t);
            }
            lastValidState = state;
            try {
                if (!isDone()) {
                    updateState(ResultState.Abort);
                }
            } catch (Throwable t) {
                LOGGER.error("Error while closing result handler.", t);
            }
            finalState = state;
        }

        // use leakCheckClosed as run once
        if (leakCheckClosed.compareAndSet(false, true)) {
            // invoke onDone callback
            if (stateCallback != null) {
                try {
                    stateCallback.onDone(this, lastValidState, finalState);
                } catch (Throwable t) {
                    LOGGER.error("Error while invoking onDone callback.", t);
                }
            }
            // cleanup
            try {
                cleanup();
            } catch (Throwable t) {
                LOGGER.error("Error while cleaning up result handler.", t);
            }
            // finalize the leak check
            super.close();
        }
    }
}
