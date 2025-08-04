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

import com.alibaba.polardbx.proxy.protocol.command.ResultsetMetaData;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

@Getter
@Setter
public class ComStmtPrepareOk implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
     * <p>
     * Type	Name	Description
     * int<1>	status	0x00: OK: Ignored by cli_read_prepare_result
     * int<4>	statement_id	statement ID
     * int<2>	num_columns	Number of columns
     * int<2>	num_params	Number of parameters
     * int<1>	reserved_1	[00] filler
     * if (packet_length > 12) {
     * int<2>	warning_count	Number of warnings
     * if capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA {
     * int<1>	metadata_follows	Flag specifying if metadata are skipped or not. See enum_resultset_metadata
     * } – CLIENT_OPTIONAL_RESULTSET_METADATA
     * } – packet_length > 12
     */
    private int statementId;
    private int numColumns;
    private int numParams;
    private int warningCount;
    private boolean metadataFollows = true;

    public ComStmtPrepareOk() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.remaining() < 10) {
            throw new IllegalArgumentException("Stmt prepare packet too short");
        }
        if (decoder.u8() != 0) {
            throw new IllegalArgumentException("invalid status");
        }
        this.statementId = (int) decoder.u32();
        this.numColumns = decoder.u16();
        this.numParams = decoder.u16();
        decoder.skip_s(1);
        if (decoder.remaining() >= 2) {
            this.warningCount = decoder.u16();
            if (decoder.remaining() > 0 && (capabilities & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
                this.metadataFollows = ResultsetMetaData.RESULTSET_METADATA_FULL == decoder.u8();
            }
        }
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        encoder.begin();

        encoder.u8(0);
        encoder.u32(statementId);
        encoder.u16(numColumns);
        encoder.u16(numParams);
        encoder.u8(0);
        encoder.u16(warningCount);
        if ((capabilities & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            encoder.u8(metadataFollows ? ResultsetMetaData.RESULTSET_METADATA_FULL :
                ResultsetMetaData.RESULTSET_METADATA_NONE);
        }

        encoder.end();
    }

    @Override
    public String toString() {
        return "ComStmtPrepareOk{" +
            "statementId=" + statementId +
            ", numColumns=" + numColumns +
            ", numParams=" + numParams +
            ", warningCount=" + warningCount +
            ", metadataFollows=" + metadataFollows +
            '}';
    }
}
