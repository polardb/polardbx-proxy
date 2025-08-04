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

package com.alibaba.polardbx.proxy.protocol.common;

import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.LeakChecker;
import com.alibaba.polardbx.proxy.utils.Slice;

import java.io.IOException;

/**
 * Thread-safe closable protocol handler.
 * This interface may invoke concurrently cause by concurrent close and packet handler.
 * - Should deal concurrency of close and (handleAndTakePacket or handleFinish).
 * - `handleAndTakePacket` and `handleFinish` will invoke sequentially.
 */
public abstract class MysqlProtocolHandler extends LeakChecker {
    public MysqlProtocolHandler() {
        setTag("MysqlProtocolHandler");
    }

    /**
     * Invoke per packet without encoder.
     *
     * @param packet packet to be handled
     * @param decoder decoder for packet
     * @return taken or not
     */
    public abstract boolean handleAndTakePacket(Slice packet, Decoder decoder);

    /**
     * Invoke per packet with encoder.
     *
     * @param packet packet to be handled
     * @param decoder decoder for packet
     * @param encoder encoder for output
     * @return taken or not
     * @throws IOException when io error in encoder
     */
    public boolean handleAndTakePacket(Slice packet, Decoder decoder, Encoder encoder) throws IOException {
        return handleAndTakePacket(packet, decoder);
    }

    /**
     * Invoke when all packets in batch are handled.
     */
    public void handleFinish() {
        // do nothing
    }
}
