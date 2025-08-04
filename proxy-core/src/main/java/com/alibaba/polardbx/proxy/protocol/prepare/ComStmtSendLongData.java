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

package com.alibaba.polardbx.proxy.protocol.prepare;

import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.utils.BytesTools;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ComStmtSendLongData implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
     * <p>
     * Type	Name	Description
     * int<1>	status	[0x18] COM_STMT_SEND_LONG_DATA
     * int<4>	statement_id	ID of the statement
     * int<2>	param_id	The parameter to supply data to
     * binary<var>	data	The actual payload to send
     */
    private int statementId;
    private int paramId;
    private byte[] data;

    public ComStmtSendLongData() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.remaining() < 7) {
            throw new IllegalArgumentException("Stmt send long data packet too short");
        }
        if (decoder.u8() != 0x18) {
            throw new IllegalArgumentException("invalid status");
        }
        this.statementId = (int) decoder.u32();
        this.paramId = decoder.u16();
        this.data = decoder.str();
    }

    @Override
    public String toString() {
        return "ComStmtSendLongData{" +
            "statementId=" + statementId +
            ", paramId=" + paramId +
            ", data=" + (null == data ? null :
            '\n' + BytesTools.beautifulHex(data, 0, data.length) + '\n') +
            '}';
    }
}
