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
public class AuthSwitchResponse implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
     * <p>
     * Type	Name	Description
     * string<EOF>	data	authentication response data
     */
    private byte[] data;

    public AuthSwitchResponse() {
    }

    @Override
    public void decode(Decoder decoder, int ignored) {
        this.data = decoder.str();
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        encoder.begin();
        encoder.str(data);
        encoder.end();
    }

    @Override
    public String toString() {
        return "AuthSwitchResponse{" +
            "data=" + (null == data ? "<null>" : '\n' + BytesTools.beautifulHex(data, 0, data.length) + '\n') +
            '}';
    }
}
