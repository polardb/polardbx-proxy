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
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ComInitDb implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_init_db.html
     * <p>
     * Type	Name	Description
     * int<1>	command	0x02: COM_INIT_DB
     * string<EOF>	schema name	name of the schema to change to
     */
    private byte[] schemaName;

    public ComInitDb() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.u8_s() != 0x02) {
            throw new IllegalArgumentException("invalid command");
        }
        this.schemaName = decoder.str();
    }

    @Override
    public String toString() {
        return "ComInitDb{" +
            "schemaName=" + (null == schemaName ? "<null>" : new String(schemaName)) +
            '}';
    }
}
