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

package com.alibaba.polardbx.proxy.protocol.command;

import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

@Getter
@Setter
public class EofPacket implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
     * <p>
     * Type	Name	Description
     * int<1>	header	0xFE EOF packet header
     * if capabilities & CLIENT_PROTOCOL_41 {
     * int<2>	warnings	number of warnings
     * int<2>	status_flags	SERVER_STATUS_flags_enum
     */
    private int warnings;
    private int statusFlags;

    public EofPacket() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.u8_s() != 0xFE) {
            throw new IllegalArgumentException("invalid eof packet");
        }
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            if (decoder.remaining() < 4) {
                throw new IllegalArgumentException("EOF packet too short");
            }
            this.warnings = decoder.u16();
            this.statusFlags = decoder.u16();
        } else {
            if (decoder.remaining() < 2) {
                throw new IllegalArgumentException("EOF packet too short");
            }
            this.warnings = 0;
            this.statusFlags = decoder.u16();
        }
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        encoder.begin();

        encoder.u8(0xFE);
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            encoder.u16(warnings);
            encoder.u16(statusFlags);
        }

        encoder.end();
    }

    @Override
    public String toString() {
        return "EofPacket{" +
            "warnings=" + warnings +
            ", statusFlags=0x" + Integer.toHexString(statusFlags) +
            '}';
    }
}
