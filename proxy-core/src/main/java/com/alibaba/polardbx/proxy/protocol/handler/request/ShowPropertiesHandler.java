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

import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ShowPropertiesHandler extends SystemTableRequestHandler {
    public ShowPropertiesHandler(FrontendContext context) {
        super(context);
        setTag("ShowProperties");
    }

    private static final ColumnDefinition41[] fields = new ColumnDefinition41[] {
        new ColumnDefinition41().fieldVarchar("key".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024).setNotNull(true),
        new ColumnDefinition41().fieldVarchar("value".getBytes(StandardCharsets.UTF_8),
            CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci, 1024)
    };

    @Override
    protected ColumnDefinition41[] getFields() {
        return fields;
    }

    @Override
    protected void emitRows(RowConsumer consumer) throws IOException {
        final HashMap<Object, Object> map = new HashMap<>(ConfigProps.DEFAULT_PROPS);
        map.putAll(ConfigLoader.PROPERTIES);

        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            if (null == entry.getKey()) {
                continue;
            }
            final byte[][] row = new byte[fields.length][];
            row[0] = entry.getKey().toString().getBytes(StandardCharsets.UTF_8);
            final Object v = entry.getValue();
            if (null == v || null == v.toString()) {
                row[1] = null;
            } else {
                if (entry.getKey().toString().toLowerCase().contains("password")) {
                    row[1] = "******".getBytes(StandardCharsets.UTF_8);
                } else {
                    row[1] = v.toString().getBytes(StandardCharsets.UTF_8);
                }
            }

            consumer.accept(row);
        }
    }
}
