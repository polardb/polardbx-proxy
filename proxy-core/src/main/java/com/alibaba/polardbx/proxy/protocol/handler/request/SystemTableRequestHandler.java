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

package com.alibaba.polardbx.proxy.protocol.handler.request;

import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.protocol.command.EofPacket;
import com.alibaba.polardbx.proxy.protocol.command.OkPacket;
import com.alibaba.polardbx.proxy.protocol.command.ResultsetMetaData;
import com.alibaba.polardbx.proxy.protocol.command.StatusFlags;
import com.alibaba.polardbx.proxy.protocol.common.MysqlProtocolHandler;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.Slice;

import java.io.IOException;

public abstract class SystemTableRequestHandler extends MysqlProtocolHandler {
    private final FrontendContext context;

    public SystemTableRequestHandler(FrontendContext context) {
        setTag("SystemTableRequestHandler");
        this.context = context;
    }

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        throw new UnsupportedOperationException("Encoder must be provided when handling show cluster.");
    }

    protected abstract ColumnDefinition41[] getFields();

    protected interface RowConsumer {
        void accept(byte[][] row) throws IOException;
    }

    protected abstract void emitRows(RowConsumer consumer) throws IOException;

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder, Encoder encoder) throws IOException {
        // send fields first
        final ColumnDefinition41[] fields = getFields();

        encoder.begin();
        if (context.hasCapability(Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA)) {
            encoder.u8(ResultsetMetaData.RESULTSET_METADATA_FULL);
        }
        encoder.lei(fields.length);
        encoder.end();

        for (final ColumnDefinition41 field : fields) {
            encoder.addSequence();
            field.encode(encoder, context.getCapabilities());
        }

        if (!context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
            encoder.addSequence();
            final EofPacket eof = new EofPacket();
            eof.setStatusFlags(context.genStatusFlags(false));
            eof.encode(encoder, context.getCapabilities());
        }

        // now send rows
        emitRows(row -> {
            // build row
            if (null == row || row.length != fields.length) {
                throw new IllegalArgumentException("row length must be equal to field length");
            }

            encoder.addSequence();
            encoder.begin();
            for (final byte[] val : row) {
                if (null == val) {
                    encoder.u8(0xFB);
                } else {
                    encoder.le_str(val);
                }
            }
            encoder.end();
        });

        // end eof
        encoder.addSequence();
        if (context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
            // build ok packet
            final OkPacket ok = new OkPacket();
            ok.setEOF(true);
            ok.setStatusFlags(context.genStatusFlags(false));
            ok.encode(encoder, context.getCapabilities());
        } else {
            // build eof packet
            final EofPacket eof = new EofPacket();
            eof.setStatusFlags(context.genStatusFlags(false));
            eof.encode(encoder, context.getCapabilities());
        }
        return false; // not taken
    }
}
