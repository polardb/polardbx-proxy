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

import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.context.help.PreparedStatementContext;
import com.alibaba.polardbx.proxy.protocol.command.BinaryValue;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.BytesTools;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@Setter
public class ComStmtExecute implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
     * <p>
     * Type	Name	Description
     * int<1>	status	[0x17] COM_STMT_EXECUTE
     * int<4>	statement_id	ID of the prepared statement to execute
     * int<1>	flags	Flags. See enum_cursor_type
     * int<4>	iteration_count	Number of times to execute the statement. Currently always 1.
     * if (num_params > 0 || (CLIENT_QUERY_ATTRIBUTES && (flags & PARAMETER_COUNT_AVAILABLE)) {
     * if CLIENT_QUERY_ATTRIBUTES is on {
     * int<lenenc>	parameter_count	The number of parameter metadata and values supplied. Overrides the count coming from prepare (num_params) if present.
     * } – if CLIENT_QUERY_ATTRIBUTES is on
     * if (parameter_count > 0) {
     * binary<var>	null_bitmap	NULL bitmap, length= (paramater_count + 7) / 8
     * int<1>	new_params_bind_flag	Flag if parameters must be re-bound
     * if new_params_bind_flag, for each parameter {
     * int<2>	parameter_type	Type of the parameter value. See enum_field_type
     * if CLIENT_QUERY_ATTRIBUTES is on {
     * string<lenenc>	parameter_name	Name of the parameter or empty if not present
     * } – if CLIENT_QUERY_ATTRIBUTES is on
     * } – if new_params_bind_flag is on
     * binary<var>	parameter_values	value of each parameter
     * } – if (parameter_count > 0)
     * } – if (num_params > 0 || (CLIENT_QUERY_ATTRIBUTES && (flags & PARAMETER_COUNT_AVAILABLE))
     */
    private int statementId;
    private byte flags;
    private int parameterCount;
    private byte[] nullBitmap;
    private boolean newParamsBindFlag;
    private BinaryValue[] parameters;

    private Function<Integer, PreparedStatementContext> preparedStatementContextGetter; // stmtId -> PS
    private BiFunction<Integer, Integer, Boolean> preparedStatementParameterSkipper; // <stmtId, paramIdx> -> skip

    public ComStmtExecute() {
    }

    @Override
    public void decode(Decoder decoder, int capabilities) {
        if (decoder.remaining() < 10) {
            throw new IllegalArgumentException("Stmt execute packet too short");
        }
        if (decoder.u8() != 0x17) {
            throw new IllegalArgumentException("invalid status");
        }
        this.statementId = (int) decoder.u32();
        this.flags = (byte) decoder.u8();
        final int iterationCount = (int) decoder.u32();
        if (iterationCount != 1) {
            throw new IllegalArgumentException("invalid iteration count");
        }
        final PreparedStatementContext preparedStatementContext = preparedStatementContextGetter.apply(statementId);
        this.parameterCount = preparedStatementContext.getOk().getNumParams();
        if (this.parameterCount > 0 || ((capabilities & Capabilities.CLIENT_QUERY_ATTRIBUTES) != 0
            && (flags & CursorType.PARAMETER_COUNT_AVAILABLE) != 0)) {
            if ((capabilities & Capabilities.CLIENT_QUERY_ATTRIBUTES) != 0) {
                this.parameterCount = (int) decoder.lei_s();
            }
            if (this.parameterCount > 0) {
                final int nullBitmapLen = (this.parameterCount + 7) / 8;
                if (decoder.remaining() < nullBitmapLen + 1) {
                    throw new IllegalArgumentException("invalid length for null bitmap and new_params_bind_flag");
                }
                this.nullBitmap = decoder.str(nullBitmapLen);
                this.newParamsBindFlag = 1 == decoder.u8();
                this.parameters = new BinaryValue[this.parameterCount];
                if (this.newParamsBindFlag) {
                    // decode parameter names and types
                    for (int i = 0; i < this.parameterCount; ++i) {
                        this.parameters[i] =
                            new BinaryValue(decoder, (capabilities & Capabilities.CLIENT_QUERY_ATTRIBUTES) != 0);
                    }
                } else {
                    // load parameter names and types
                    final ParameterRebind[] rebinds = preparedStatementContext.getRebindParameters().getAcquire();
                    if (rebinds != null) {
                        for (int i = 0; i < this.parameterCount; ++i) {
                            if (i >= rebinds.length) {
                                throw new IllegalArgumentException("invalid prepared statement parameter index");
                            }
                            this.parameters[i] = new BinaryValue(rebinds[i].getType(), rebinds[i].getName());
                        }
                    } else {
                        for (int i = 0; i < this.parameterCount; ++i) {
                            if (i >= preparedStatementContext.getParameters().size()) {
                                throw new IllegalArgumentException("invalid prepared statement parameter index");
                            }
                            final ColumnDefinition41 definition = preparedStatementContext.getParameters().get(i);
                            this.parameters[i] = new BinaryValue(definition.getType() & 0xFF, definition.getName());
                        }
                    }
                }
                // decode parameter values
                for (int i = 0; i < this.parameterCount; i++) {
                    if ((this.nullBitmap[i / 8] & (1 << (i % 8))) != 0) {
                        continue;
                    }
                    if (null == preparedStatementParameterSkipper || !preparedStatementParameterSkipper.apply(
                        statementId, i)) {
                        this.parameters[i].decodeValue(decoder);
                    }
                }
            } else {
                this.nullBitmap = null;
                this.newParamsBindFlag = false;
                this.parameters = null;
            }
        } else {
            this.nullBitmap = null;
            this.newParamsBindFlag = false;
            this.parameters = null;
        }
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        encoder.begin();

        encoder.u8(0x17);
        encoder.u32(this.statementId);
        encoder.u8(this.flags);
        encoder.u32(1);
        if (this.parameterCount > 0 || ((capabilities & Capabilities.CLIENT_QUERY_ATTRIBUTES) != 0
            && (flags & CursorType.PARAMETER_COUNT_AVAILABLE) != 0)) {
            if ((capabilities & Capabilities.CLIENT_QUERY_ATTRIBUTES) != 0) {
                encoder.lei(parameterCount);
            }
            if (this.parameterCount > 0) {
                if (this.nullBitmap.length != (this.parameterCount + 7) / 8) {
                    throw new IllegalArgumentException("invalid length for null bitmap");
                }
                encoder.str(this.nullBitmap);
                encoder.u8(this.newParamsBindFlag ? 1 : 0);
                if (this.newParamsBindFlag) {
                    for (final BinaryValue parameter : this.parameters) {
                        parameter.encodeName(encoder, (capabilities & Capabilities.CLIENT_QUERY_ATTRIBUTES) != 0);
                    }
                }
                for (int i = 0; i < this.parameterCount; i++) {
                    if ((this.nullBitmap[i / 8] & (1 << (i % 8))) != 0) {
                        continue;
                    }
                    this.parameters[i].encodeValue(encoder);
                }
            }
        }

        encoder.end();
    }

    public String parametersLogString() {
        final StringBuilder builder = new StringBuilder();
        builder.append('[');
        for (int i = 0; i < parameterCount; ++i) {
            if (i > 0) {
                builder.append(',');
            }
            final boolean isNull = (this.nullBitmap[i / 8] & (1 << (i % 8))) != 0;
            builder.append('@').append(i + 1).append('=').append(isNull ? "null" : parameters[i].toLogString());
            if (builder.length() > FastConfig.logSqlParamMaxLength && i < parameterCount - 1) {
                builder.append(", ...");
                break;
            }
        }
        builder.append(']');
        return builder.toString();
    }

    @Override
    public String toString() {
        return "ComStmtExecute{" +
            "statementId=" + statementId +
            ", flags=" + Integer.toHexString(flags & 0xFF) +
            ", parameterCount=" + parameterCount +
            ", nullBitmap=" + (nullBitmap == null ? null :
            '\n' + BytesTools.beautifulHex(nullBitmap, 0, nullBitmap.length) + '\n') +
            ", newParamsBindFlag=" + newParamsBindFlag +
            ", parameters=" + (null == parameters ? "<null>" :
            Arrays.stream(parameters).map(BinaryValue::toString).collect(Collectors.joining(",")))
            + '}';
    }
}
