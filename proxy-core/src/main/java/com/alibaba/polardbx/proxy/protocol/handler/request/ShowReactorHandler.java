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

import com.alibaba.polardbx.proxy.ProxyServer;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.net.NIOProcessor;
import com.alibaba.polardbx.proxy.perf.ReactorPerfItem;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ShowReactorHandler extends SystemTableRequestHandler {
    public ShowReactorHandler(FrontendContext context) {
        super(context);
        setTag("ShowReactorHandler");
    }

    private static final ColumnDefinition41[] fields = new ColumnDefinition41[] {
        new ColumnDefinition41().fieldVarchar("name".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldLong("sockets".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("events".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("registers".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("reads".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("writes".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("connects".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("buffer".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("block".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("total".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true),
        new ColumnDefinition41().fieldLong("idle".getBytes(StandardCharsets.UTF_8))
            .setNotNull(true).setBinary(true)
    };

    @Override
    protected ColumnDefinition41[] getFields() {
        return fields;
    }

    @Override
    protected void emitRows(RowConsumer consumer) throws IOException {
        final NIOProcessor[] processors = ProxyServer.getInstance().getWorker().getProcessors();
        for (final NIOProcessor processor : processors) {
            final ReactorPerfItem item = processor.getPerfItem();
            // build row
            final byte[][] row = new byte[fields.length][];
            row[0] = processor.getName().getBytes(StandardCharsets.UTF_8);
            row[1] = String.valueOf(item.getSocketCount()).getBytes(StandardCharsets.UTF_8);
            row[2] = String.valueOf(item.getEventLoopCount()).getBytes(StandardCharsets.UTF_8);
            row[3] = String.valueOf(item.getRegisterCount()).getBytes(StandardCharsets.UTF_8);
            row[4] = String.valueOf(item.getReadCount()).getBytes(StandardCharsets.UTF_8);
            row[5] = String.valueOf(item.getWriteCount()).getBytes(StandardCharsets.UTF_8);
            row[6] = String.valueOf(item.getConnectCount()).getBytes(StandardCharsets.UTF_8);
            row[7] = String.valueOf(item.getBufferSize()).getBytes(StandardCharsets.UTF_8);
            row[8] = String.valueOf(item.getBufferBlockSize()).getBytes(StandardCharsets.UTF_8);
            row[9] = String.valueOf(item.getBufferSize() / item.getBufferBlockSize()).getBytes(StandardCharsets.UTF_8);
            row[10] = String.valueOf(item.getIdleBufferCount()).getBytes(StandardCharsets.UTF_8);

            consumer.accept(row);
        }
    }
}
