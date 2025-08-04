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
import com.alibaba.polardbx.proxy.utils.BytesTools;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

@Getter
@Setter
public class OkPacket implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
     * <p>
     * Type	Name	Description
     * int<1>	header	0x00 or 0xFE the OK packet header
     * int<lenenc>	affected_rows	affected rows
     * int<lenenc>	last_insert_id	last insert-id
     * if capabilities & CLIENT_PROTOCOL_41 {
     * int<2>	status_flags	SERVER_STATUS_flags_enum
     * int<2>	warnings	number of warnings
     * } else if capabilities & CLIENT_TRANSACTIONS {
     * int<2>	status_flags	SERVER_STATUS_flags_enum
     * }
     * if capabilities & CLIENT_SESSION_TRACK
     * string<lenenc>	info	human readable status information
     * if status_flags & SERVER_SESSION_STATE_CHANGED {
     * string<lenenc>	session state info	Session State Information
     * }
     * } else {
     * string<EOF>	info	human readable status information
     * }
     */
    private boolean isEOF;
    private long affectedRows;
    private long lastInsertId;
    private int statusFlags;
    private int warnings;
    private byte[] info;
    private byte[] sessionStateInfo;

    public OkPacket() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.remaining() < 5) {
            throw new IllegalArgumentException("OK packet too short");
        }
        final int header = decoder.u8();
        if (header != 0 && header != 0xFE) {
            throw new IllegalArgumentException("OK packet header is not 0 or 0xFE");
        }
        this.isEOF = header == 0xFE;
        this.affectedRows = decoder.lei_s();
        this.lastInsertId = decoder.lei_s();
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            if (decoder.remaining() < 4) {
                throw new IllegalArgumentException("OK packet too short");
            }
            this.statusFlags = decoder.u16();
            this.warnings = decoder.u16();
        } else {
            this.statusFlags = decoder.u16_s();
            this.warnings = 0;
        }
        if ((capabilities & Capabilities.CLIENT_SESSION_TRACK) != 0) {
            this.info = decoder.le_str_s();
            if ((this.statusFlags & StatusFlags.SERVER_SESSION_STATE_CHANGED) != 0) {
                this.sessionStateInfo = decoder.le_str_s();
            } else {
                this.sessionStateInfo = null;
            }
        } else {
            this.info = decoder.str();
            this.sessionStateInfo = null;
        }
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        encoder.begin();

        encoder.u8(isEOF ? 0xFE : 0);
        encoder.lei(affectedRows);
        encoder.lei(lastInsertId);
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            encoder.u16(statusFlags);
            encoder.u16(warnings);
        } else {
            encoder.u16(statusFlags);
        }
        if ((capabilities & Capabilities.CLIENT_SESSION_TRACK) != 0) {
            if (null == info) {
                encoder.u8(0);
            } else {
                encoder.le_str(info);
            }
            if ((this.statusFlags & StatusFlags.SERVER_SESSION_STATE_CHANGED) != 0) {
                if (null == sessionStateInfo) {
                    encoder.u8(0);
                } else {
                    encoder.le_str(sessionStateInfo);
                }
            }
        } else {
            if (info != null) {
                encoder.str(info);
            }
        }

        encoder.end();
    }

    @Override
    public String toString() {
        return "OkPacket{" +
            "affectedRows=" + affectedRows +
            ", lastInsertId=" + lastInsertId +
            ", statusFlags=" + statusFlags +
            ", warnings=" + warnings +
            ", info=" + (null == info ? "<null>" : new String(info)) +
            ", sessionStateInfo=" + (null == sessionStateInfo ? "<null>" :
            '\n' + BytesTools.beautifulHex(sessionStateInfo, 0, sessionStateInfo.length) + '\n') +
            '}';
    }
}
