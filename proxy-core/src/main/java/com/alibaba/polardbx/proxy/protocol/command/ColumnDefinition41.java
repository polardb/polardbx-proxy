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
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Getter
@Setter
public class ColumnDefinition41 implements MysqlPacket {
    private final byte[] DEFAULT_CATALOG = "def".getBytes(StandardCharsets.UTF_8);

    /**
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
     * <p>
     * Type	Name	Description
     * string<lenenc>	catalog	The catalog used. Currently always "def"
     * string<lenenc>	schema	schema name
     * string<lenenc>	table	virtual table name
     * string<lenenc>	org_table	physical table name
     * string<lenenc>	name	virtual column name
     * string<lenenc>	org_name	physical column name
     * int<lenenc>	length of fixed length fields	[0x0c]
     * int<2>	character_set	the column character set as defined in Character Set
     * int<4>	column_length	maximum length of the field
     * int<1>	type	type of the column as defined in enum_field_types
     * int<2>	flags	Flags as defined in Column Definition Flags
     * int<1>	decimals	max shown decimal digits:
     * 0x00 for integers and static strings
     * 0x1f for dynamic strings, double, float
     * 0x00 to 0x51 for decimals
     */
    private byte[] catalog;
    private byte[] schema;
    private byte[] table;
    private byte[] orgTable;
    private byte[] name;
    private byte[] orgName;
    private int characterSet;
    private int columnLength;
    private byte type;
    private int flags;
    private byte decimals;

    public ColumnDefinition41() {
    }

    public ColumnDefinition41 setNotNull(boolean notNull) {
        if (notNull) {
            this.flags |= FieldFlags.NOT_NULL_FLAG;
        } else {
            this.flags &= ~FieldFlags.NOT_NULL_FLAG;
        }
        return this;
    }

    public ColumnDefinition41 setBinary(boolean binary) {
        if (binary) {
            this.flags |= FieldFlags.BINARY_FLAG;
        } else {
            this.flags &= ~FieldFlags.BINARY_FLAG;
        }
        return this;
    }

    public ColumnDefinition41 fieldVarchar(byte[] name, int charset, int len) {
        this.catalog = DEFAULT_CATALOG;
        this.name = name;
        this.characterSet = charset;
        this.columnLength = len;
        this.type = (byte) FieldType.MYSQL_TYPE_VAR_STRING;
        this.decimals = 0x1F;
        return this;
    }

    public ColumnDefinition41 fieldFloat(byte[] name) {
        this.catalog = DEFAULT_CATALOG;
        this.name = name;
        this.characterSet = CharsetMapping.MYSQL_COLLATION_INDEX_binary;
        this.columnLength = 14;
        this.type = (byte) FieldType.MYSQL_TYPE_FLOAT;
        this.decimals = 0x1F;
        return this;
    }

    public ColumnDefinition41 fieldDouble(byte[] name) {
        this.catalog = DEFAULT_CATALOG;
        this.name = name;
        this.characterSet = CharsetMapping.MYSQL_COLLATION_INDEX_binary;
        this.columnLength = 25;
        this.type = (byte) FieldType.MYSQL_TYPE_DOUBLE;
        this.decimals = 0x1F;
        return this;
    }

    public ColumnDefinition41 fieldLong(byte[] name) {
        this.catalog = DEFAULT_CATALOG;
        this.name = name;
        this.characterSet = CharsetMapping.MYSQL_COLLATION_INDEX_binary;
        this.columnLength = 20;
        this.type = (byte) FieldType.MYSQL_TYPE_LONGLONG;
        return this;
    }

    @Override
    public void decode(Decoder decoder, int ignored) {
        if (decoder.remaining() < 17) {
            throw new IllegalArgumentException("ColumnDefinition too short");
        }
        this.catalog = decoder.le_str_s();
        this.schema = decoder.le_str_s();
        this.table = decoder.le_str_s();
        this.orgTable = decoder.le_str_s();
        this.name = decoder.le_str_s();
        this.orgName = decoder.le_str_s();
        if (decoder.lei_s() != 0x0c) {
            throw new IllegalArgumentException("ColumnDefinition fixed length field is not 0x0c");
        }
        if (decoder.remaining() < 10) {
            throw new IllegalArgumentException("ColumnDefinition too short");
        }
        this.characterSet = decoder.u16();
        this.columnLength = (int) decoder.u32();
        this.type = (byte) decoder.u8();
        this.flags = decoder.u16();
        this.decimals = (byte) decoder.u8();
    }

    @Override
    public void encode(Encoder encoder, int capabilities) throws IOException {
        encoder.begin();

        if (null == catalog) {
            encoder.u8(0);
        } else {
            encoder.le_str(catalog);
        }
        if (null == schema) {
            encoder.u8(0);
        } else {
            encoder.le_str(schema);
        }
        if (null == table) {
            encoder.u8(0);
        } else {
            encoder.le_str(table);
        }
        if (null == orgTable) {
            encoder.u8(0);
        } else {
            encoder.le_str(orgTable);
        }
        if (null == name) {
            encoder.u8(0);
        } else {
            encoder.le_str(name);
        }
        if (null == orgName) {
            encoder.u8(0);
        } else {
            encoder.le_str(orgName);
        }
        encoder.lei(0x0C);
        encoder.u16(characterSet);
        encoder.u32(columnLength);
        encoder.u8(type);
        encoder.u16(flags);
        encoder.u8(decimals);
        encoder.u16(0); // extra padding for len 0x0C

        encoder.end();
    }

    public enum SimpleType {
        STRING, // should be '...'
        BYTES, // should be x'...'
        RAW_STRING // should be ...
    }

    public SimpleType simpleType() {
        switch (type & 0xFF) {
        case FieldType.MYSQL_TYPE_DATE:
        case FieldType.MYSQL_TYPE_DATETIME:
        case FieldType.MYSQL_TYPE_TIMESTAMP:
        case FieldType.MYSQL_TYPE_TIME:
            return SimpleType.STRING;

        case FieldType.MYSQL_TYPE_STRING:
        case FieldType.MYSQL_TYPE_VARCHAR:
        case FieldType.MYSQL_TYPE_VAR_STRING:
        case FieldType.MYSQL_TYPE_ENUM:
        case FieldType.MYSQL_TYPE_SET:
        case FieldType.MYSQL_TYPE_LONG_BLOB:
        case FieldType.MYSQL_TYPE_MEDIUM_BLOB:
        case FieldType.MYSQL_TYPE_BLOB:
        case FieldType.MYSQL_TYPE_TINY_BLOB:
        case FieldType.MYSQL_TYPE_GEOMETRY:
        case FieldType.MYSQL_TYPE_BIT:
            return characterSet != CharsetMapping.MYSQL_COLLATION_INDEX_binary ? SimpleType.STRING : SimpleType.BYTES;

        case FieldType.MYSQL_TYPE_NULL:
            return null;

        default:
            return SimpleType.RAW_STRING;
        }
    }

    @Override
    public String toString() {
        return "ColumnDefinition41{" +
            "catalog=" + (null == catalog ? "<null>" : new String(catalog)) +
            ", schema=" + (null == schema ? "<null>" : new String(schema)) +
            ", table=" + (null == table ? "<null>" : new String(table)) +
            ", orgTable=" + (null == orgTable ? "<null>" : new String(orgTable)) +
            ", name=" + (null == name ? "<null>" : new String(name)) +
            ", orgName=" + (null == orgName ? "<null>" : new String(orgName)) +
            ", characterSet=" + characterSet +
            ", columnLength=" + columnLength +
            ", type=" + (type & 0xFF) +
            ", flags=0x" + Integer.toHexString(flags) +
            ",decimals=0x" + Integer.toHexString(decimals) +
            '}';
    }
}
