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
import java.util.Arrays;
import java.util.stream.Collectors;

@Getter
@Setter
public class ComQuery implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html
     * <p>
     * Type	Name	Description
     * int<1>	command	0x03: COM_QUERY
     * if CLIENT_QUERY_ATTRIBUTES is set {
     * int<lenenc>	parameter_count	Number of parameters
     * int<lenenc>	parameter_set_count	Number of parameter sets. Currently always 1
     * if parameter_count > 0 {
     * binary<var>	null_bitmap	NULL bitmap, length= (num_params + 7) / 8
     * int<1>	new_params_bind_flag	Always 1. Malformed packet error if not 1
     * if new_params_bind_flag, for each parameter {
     * int<2>	param_type_and_flag	Parameter type (2 bytes). The MSB is reserved for unsigned flag
     * string<lenenc>	parameter name	String
     * }
     * binary<var>	parameter_values	value of each parameter: Binary Protocol Value
     * }
     * }
     * string<EOF>	query	the text of the SQL query to execute
     */
    private int parameterCount;
    private int parameterSetCount;
    private byte[] nullBitmap;

    private BinaryValue[] parameters;
    private byte[] query;

    public ComQuery() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.u8_s() != 0x03) {
            throw new IllegalArgumentException("invalid command");
        }
        if ((capabilities & Capabilities.CLIENT_QUERY_ATTRIBUTES) != 0) {
            this.parameterCount = (int) decoder.lei_s();
            this.parameterSetCount = (int) decoder.lei_s();
            if (this.parameterCount > 0) {
                final int nullBitmapLen = (this.parameterCount + 7) / 8;
                if (decoder.remaining() < nullBitmapLen + 1) {
                    throw new IllegalArgumentException("invalid length for null bitmap and new_params_bind_flag");
                }
                this.nullBitmap = decoder.str(nullBitmapLen);
                if (decoder.u8() != 1) {
                    throw new IllegalArgumentException("invalid new_params_bind_flag");
                }
                // decode parameter names and types(new_params_bind_flag always 1)
                this.parameters = new BinaryValue[this.parameterCount];
                for (int i = 0; i < this.parameterCount; i++) {
                    this.parameters[i] = new BinaryValue(decoder, true);
                }
                // decode parameter values
                for (int i = 0; i < this.parameterCount; i++) {
                    if ((this.nullBitmap[i / 8] & (1 << (i % 8))) != 0) {
                        continue;
                    }
                    this.parameters[i].decodeValue(decoder);
                }
            } else {
                this.nullBitmap = null;
                this.parameters = null;
            }
        } else {
            this.parameterCount = 0;
            this.parameterSetCount = 0;
            this.nullBitmap = null;
            this.parameters = null;
        }
        this.query = decoder.str();
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        // fix parameter
        parameterSetCount = 1;

        // then build
        encoder.begin();

        encoder.u8(0x03);
        if ((capabilities & Capabilities.CLIENT_QUERY_ATTRIBUTES) != 0) {
            encoder.lei(parameterCount);
            encoder.lei(parameterSetCount);
            if (parameterCount > 0) {
                assert nullBitmap != null;
                if (nullBitmap.length != (parameterCount + 7) / 8) {
                    throw new IllegalArgumentException("invalid length for null bitmap");
                }
                encoder.str(nullBitmap);
                encoder.u8(1);
                assert parameters != null;
                if (parameters.length != parameterCount) {
                    throw new IllegalArgumentException("invalid length for parameters");
                }
                for (BinaryValue parameter : parameters) {
                    parameter.encodeName(encoder, true);
                }
                for (int i = 0; i < this.parameterCount; i++) {
                    if ((this.nullBitmap[i / 8] & (1 << (i % 8))) != 0) {
                        continue;
                    }
                    this.parameters[i].encodeValue(encoder);
                }
            }
        }
        if (query != null) {
            encoder.str(query);
        }

        encoder.end();
    }

    @Override
    public String toString() {
        return "ComQuery{" +
            "parameterCount=" + parameterCount +
            ", parameterSetCount=" + parameterSetCount +
            ", nullBitmap=" + (nullBitmap == null ? null :
            '\n' + BytesTools.beautifulHex(nullBitmap, 0, nullBitmap.length) + '\n') +
            ", parameters=" + (null == parameters ? "<null>" :
            Arrays.stream(parameters).map(BinaryValue::toString).collect(Collectors.joining(","))) +
            ", query=" + (null == query ? "<null>" : new String(query)) +
            '}';
    }
}
