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

package com.alibaba.polardbx.proxy.protocol.connection;

import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SSLRequest implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html
     * <p>
     * Type	Name	Description
     * if capabilities & CLIENT_PROTOCOL_41 {
     * int<4>	client_flag	Capabilities Flags
     * int<4>	max_packet_size	maximum packet size
     * int<1>	character_set	client charset a_protocol_character_set, only the lower 8-bits
     * string[23]	filler	filler to the size of the handhshake response packet. All 0s.
     * } else {
     * int<2>	client_flag	Capabilities Flags, only the lower 16 bits
     * int<3>	max_packet_size	maximum packet size, 0xFFFFFF max
     * }
     */
    private int clientFlag;
    private int maxPacketSize;
    private byte characterSet;

    public SSLRequest() {
    }

    @Override
    public void decode(Decoder decoder, int ignored) {
        if (decoder.remaining() < 5) {
            throw new IllegalArgumentException("SSLRequest packet is too short");
        }
        final int capabilities = (int) decoder.u32();
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            if (decoder.remaining() < 28) {
                throw new IllegalArgumentException("SSLRequest packet is too short");
            }
            this.clientFlag = capabilities;
            this.maxPacketSize = (int) decoder.u32();
            this.characterSet = (byte) decoder.u8();
        } else {
            this.clientFlag = capabilities & 0xFFFF;
            this.maxPacketSize = ((capabilities >>> 16) & 0xFFFF) | (decoder.u8() << 16);
            this.characterSet = 0;
        }
    }

    @Override
    public String toString() {
        return "SSLRequest{" +
            "clientFlag=" + clientFlag +
            ", maxPacketSize=" + maxPacketSize +
            ", characterSet=" + characterSet +
            '}';
    }
}
