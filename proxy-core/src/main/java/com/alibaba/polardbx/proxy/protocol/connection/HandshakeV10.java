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
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.BytesTools;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

@Getter
@Setter
public class HandshakeV10 implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
     * <p>
     * Type	Name	Description
     * int<1>	protocol version	Always 10
     * string<NUL>	server version	human readable status information
     * int<4>	thread id	a.k.a. connection id
     * string[8]	auth-plugin-data-part-1	first 8 bytes of the plugin provided data (scramble)
     * int<1>	filler	0x00 byte, terminating the first part of a scramble
     * int<2>	capability_flags_1	The lower 2 bytes of the Capabilities Flags
     * int<1>	character_set	default server a_protocol_character_set, only the lower 8-bits
     * int<2>	status_flags	SERVER_STATUS_flags_enum
     * int<2>	capability_flags_2	The upper 2 bytes of the Capabilities Flags
     * if capabilities & CLIENT_PLUGIN_AUTH {
     * int<1>	auth_plugin_data_len	length of the combined auth_plugin_data (scramble), if auth_plugin_data_len is > 0
     * } else {
     * int<1>	00	constant 0x00
     * }
     * string[10]	reserved	reserved. All 0s.
     * $length	auth-plugin-data-part-2	Rest of the plugin provided data (scramble), $len=MAX(13, length of auth-plugin-data - 8)
     * if capabilities & CLIENT_PLUGIN_AUTH {
     * NULL	auth_plugin_name	name of the auth_method that the auth_plugin_data belongs to
     * }
     */
    public static final byte VERSION = 10;

    private byte[] version;
    private int connectionId;
    private int capabilityFlags;
    private byte characterSet;
    private short statusFlags;
    private byte[] authPluginData;
    private byte[] authPluginName;

    public HandshakeV10() {
    }

    @Override
    public void decode(Decoder decoder, int ignored) {
        if (decoder.remaining() < 46) {
            throw new IllegalArgumentException("invalid handshake packet, size: " + decoder.remaining());
        }
        if (decoder.u8() != VERSION) {
            throw new IllegalArgumentException("invalid version");
        }
        this.version = decoder.str();
        // check size again
        if (decoder.remaining() < 44) {
            throw new IllegalArgumentException("invalid handshake packet, after version size: " + decoder.remaining());
        }
        this.connectionId = (int) decoder.u32();
        final int pluginPart1Pos = decoder.getPos();
        decoder.skip(9);
        final int lw = decoder.u16();
        this.characterSet = (byte) decoder.u8();
        this.statusFlags = (short) decoder.u16();
        this.capabilityFlags = lw | (decoder.u16() << 16);
        final int authDataLen;
        if ((this.capabilityFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            authDataLen = decoder.u8();
            decoder.skip(10);
        } else {
            authDataLen = 0;
            decoder.skip(11);
        }
        // check size with plugin data
        final int rest = Math.max(13, authDataLen - 8);
        if (decoder.remaining() < rest) {
            throw new IllegalArgumentException(
                "invalid handshake packet, before auth data size: " + decoder.remaining());
        }
        final int nowPos = decoder.getPos();
        if (authDataLen > 0) {
            decoder.setPos(pluginPart1Pos);
            this.authPluginData = new byte[authDataLen];
            decoder.str(Math.min(8, authDataLen), this.authPluginData, 0);
            decoder.setPos(nowPos);
            if (authDataLen > 8) {
                decoder.str(authDataLen - 8, this.authPluginData, 8);
            }
        } else {
            this.authPluginData = null;
        }
        decoder.setPos(nowPos + rest);
        if ((this.capabilityFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            authPluginName = decoder.str();
        } else {
            authPluginName = null;
        }
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        encoder.begin();

        encoder.u8(VERSION);
        if (version == null) {
            encoder.u8(0);
        } else {
            encoder.nt_str(version);
        }
        encoder.u32(connectionId);
        if (authPluginData == null) {
            // skip this
            encoder.u64(0);
        } else if (authPluginData.length >= 8) {
            encoder.str(authPluginData, 0, 8);
        } else {
            encoder.str(authPluginData, 0, authPluginData.length);
            encoder.zeros(8 - authPluginData.length);
        }
        encoder.u8(0);
        encoder.u16(capabilityFlags);
        encoder.u8(characterSet);
        encoder.u16(statusFlags);
        encoder.u16(capabilityFlags >>> 16);
        if ((capabilityFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            encoder.u8(authPluginData.length);
        } else {
            encoder.u8(0);
        }
        encoder.zeros(10);
        if (authPluginData != null && authPluginData.length > 8) {
            final int len = Math.min(13, authPluginData.length - 8);
            encoder.str(authPluginData, 8, len);
            if (len < 13) {
                encoder.zeros(13 - len);
            }
        } else {
            encoder.zeros(13);
        }
        if ((capabilityFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            if (null == authPluginName) {
                encoder.u8(0);
            } else {
                encoder.nt_str(authPluginName);
            }
        }

        encoder.end();
    }

    @Override
    public String toString() {
        return "HandshakeV10{" +
            "version=" + (null == version ? "<null>" : new String(version)) +
            ", connectionId=" + connectionId +
            ", capabilityFlags=0x" + Integer.toHexString(capabilityFlags) +
            ", characterSet=" + characterSet +
            ", statusFlags=0x" + Integer.toHexString(statusFlags) +
            ", authPluginData=" + (null == authPluginData ? "<null>" :
            '\n' + BytesTools.beautifulHex(authPluginData, 0, authPluginData.length) + '\n') +
            ", authPluginName=" + (null == authPluginName ? "<null>" : new String(authPluginName)) +
            '}';
    }
}
