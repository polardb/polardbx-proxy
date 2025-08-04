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
import com.alibaba.polardbx.proxy.utils.BytesTools;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LocalInfileData implements MysqlPacket {
    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_local_infile_data.html
     * <p>
     * Type	Name	Description
     * string<EOF>	file content	raw file data
     */
    private byte[] data;

    public LocalInfileData() {
    }

    @Override
    public void decode(Decoder decoder, int ignored) {
        this.data = decoder.str();
    }

    @Override
    public String toString() {
        return "LocalInfileData{" +
            "data=" + (null == data ? "<null>" : '\n' + BytesTools.beautifulHex(data, 0, data.length) + '\n') +
            '}';
    }
}
