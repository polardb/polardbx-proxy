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

import java.io.IOException;

public interface MysqlPacket {
    /**
     * Packet:
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html#sect_protocol_basic_packets_packet
     */
    int NORMAL_HEADER_SIZE = 4;
    int COMPRESSED_HEADER_SIZE = 7;
    int MAX_PAYLOAD_SIZE = 0xFFFFFF;
    int DEFAULT_RESERVE_BUFFER_SIZE = Math.max(128, Math.max(NORMAL_HEADER_SIZE, COMPRESSED_HEADER_SIZE));

    void decode(Decoder decoder, int capabilities);

    default void encode(Encoder encoder, int capabilities) throws IOException {
        throw new UnsupportedOperationException("not implemented enc: " + encoder + " caps: " + capabilities);
    }
}
