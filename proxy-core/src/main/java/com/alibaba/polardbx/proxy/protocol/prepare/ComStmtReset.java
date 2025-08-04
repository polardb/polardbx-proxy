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
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ComStmtReset implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html
     * <p>
     * Type	Name	Description
     * int<1>	status	[0x1A] COM_STMT_RESET
     * int<4>	statement_id	ID of the prepared statement to reset
     */
    private int statementId;

    public ComStmtReset() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.remaining() < 5) {
            throw new IllegalArgumentException("Stmt reset packet too short");
        }
        if (decoder.u8() != 0x1A) {
            throw new IllegalArgumentException("invalid status");
        }
        this.statementId = (int) decoder.u32();
    }

    @Override
    public String toString() {
        return "ComStmtReset{" +
            "statementId=" + statementId +
            '}';
    }
}
