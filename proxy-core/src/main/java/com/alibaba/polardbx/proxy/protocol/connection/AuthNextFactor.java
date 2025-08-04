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
import com.alibaba.polardbx.proxy.utils.BytesTools;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AuthNextFactor implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_next_factor_request.html
     * <p>
     * Type	Name	Description
     * int<1>	0x02	packet type
     * string[NUL]	plugin name	name of the client authentication plugin
     * string[EOF]	plugin provided data	Initial authentication data for that client plugin
     */
    private byte[] pluginName;
    private byte[] pluginData;

    public AuthNextFactor() {
    }

    @Override
    public void decode(Decoder decoder, int ignored) {
        if (decoder.remaining() < 2) {
            throw new IllegalArgumentException("invalid packet length");
        }
        if (decoder.u8() != 0x02) {
            throw new IllegalArgumentException("invalid packet type");
        }
        this.pluginName = decoder.str();
        this.pluginData = decoder.str();
    }

    @Override
    public String toString() {
        return "AuthNextFactor{" +
            "pluginName=" + (pluginName == null ? "<null>" : new String(pluginName)) +
            ", pluginData=" + (pluginData == null ? "<null>" :
            '\n' + BytesTools.beautifulHex(pluginData, 0, pluginData.length) + '\n') +
            '}';
    }
}
