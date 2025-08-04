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
public class ErrPacket implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
     * <p>
     * Type	Name	Description
     * int<1>	header	0xFF ERR packet header
     * int<2>	error_code	error-code
     * if capabilities & CLIENT_PROTOCOL_41 {
     * string[1]	sql_state_marker	# marker of the SQL state
     * string[5]	sql_state	SQL state
     * }
     * string<EOF>	error_message	human readable error message
     */
    private int errorCode;
    private byte sqlStateMarker;
    private byte[] sqlState;
    private byte[] errorMessage;

    public ErrPacket() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.remaining() < 3) {
            throw new IllegalArgumentException("ERR packet too short");
        }
        if (decoder.u8() != 0xFF) {
            throw new IllegalArgumentException("ERR packet header is not 0xFF");
        }
        this.errorCode = decoder.u16();
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            if (decoder.remaining() < 6) {
                throw new IllegalArgumentException("ERR packet too short");
            }
            this.sqlStateMarker = (byte) decoder.u8();
            this.sqlState = decoder.str(5);
        } else {
            this.sqlStateMarker = 0;
            this.sqlState = null;
        }
        this.errorMessage = decoder.str();
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        encoder.begin();

        encoder.u8(0xFF);
        encoder.u16(errorCode);
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            encoder.u8(sqlStateMarker);
            if (null == sqlState) {
                encoder.u32(0);
                encoder.u8(0);
            } else if (sqlState.length >= 5) {
                encoder.str(sqlState, 0, 5);
            } else {
                encoder.str(sqlState);
                encoder.zeros(5 - sqlState.length);
            }
        }
        if (null != errorMessage) {
            encoder.str(errorMessage);
        }

        encoder.end();
    }

    @Override
    public String toString() {
        return "ErrPacket{" +
            "errorCode=" + errorCode +
            ", sqlStateMarker='" + (char) sqlStateMarker + '\'' +
            ", sqlState=" + (null == sqlState ? "<null>" : new String(sqlState)) +
            ", errorMessage=" + (null == errorMessage ? "<null>" : new String(errorMessage)) +
            '}';
    }
}
