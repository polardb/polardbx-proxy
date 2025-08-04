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

import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.BytesTools;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

@Getter
@Setter
public class BinaryValue {
    private final int type;
    private final byte[] name;

    private byte[] bytes;
    private long l;
    private float f;
    private double d;
    private boolean negative;
    private int year, month, day, hour, minute, second, micro;

    private boolean validValue = false;

    public BinaryValue(int type, byte[] name) {
        this.type = type;
        this.name = name;
    }

    public BinaryValue(Decoder decoder, boolean withName) {
        if (decoder.remaining() < (withName ? 3 : 2)) {
            throw new IllegalArgumentException("invalid length for parameter");
        }
        this.type = decoder.u16();
        if (withName) {
            this.name = decoder.le_str_s();
        } else {
            this.name = null;
        }
    }

    public void decodeValue(Decoder decoder) {
        switch (type) {
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
        case FieldType.MYSQL_TYPE_DECIMAL:
        case FieldType.MYSQL_TYPE_NEWDECIMAL:
            this.bytes = decoder.le_str_s();
            break;

        case FieldType.MYSQL_TYPE_LONGLONG:
            this.l = decoder.i64_s();
            break;

        case FieldType.MYSQL_TYPE_LONG:
        case FieldType.MYSQL_TYPE_INT24:
            this.l = decoder.u32_s();
            break;

        case FieldType.MYSQL_TYPE_SHORT:
        case FieldType.MYSQL_TYPE_YEAR:
            this.l = decoder.u16_s();
            break;

        case FieldType.MYSQL_TYPE_TINY:
            this.l = decoder.u8_s();
            break;

        case FieldType.MYSQL_TYPE_DOUBLE:
            this.d = decoder.d_s();
            break;

        case FieldType.MYSQL_TYPE_FLOAT:
            this.f = decoder.f_s();
            break;

        case FieldType.MYSQL_TYPE_DATE:
        case FieldType.MYSQL_TYPE_DATETIME:
        case FieldType.MYSQL_TYPE_TIMESTAMP: {
            final int len = decoder.u8_s();
            if (len == 0) {
                // all zero
            } else if (len == 4) {
                if (decoder.remaining() < 4) {
                    throw new IllegalArgumentException("invalid length for date/datetime/timestamp");
                }
                this.year = decoder.u16();
                this.month = decoder.u8();
                this.day = decoder.u8();
            } else if (len == 7) {
                if (decoder.remaining() < 7) {
                    throw new IllegalArgumentException("invalid length for date/datetime/timestamp");
                }
                this.year = decoder.u16();
                this.month = decoder.u8();
                this.day = decoder.u8();
                this.hour = decoder.u8();
                this.minute = decoder.u8();
                this.second = decoder.u8();
            } else if (len == 11) {
                if (decoder.remaining() < 11) {
                    throw new IllegalArgumentException("invalid length for date/datetime/timestamp");
                }
                this.year = decoder.u16();
                this.month = decoder.u8();
                this.day = decoder.u8();
                this.hour = decoder.u8();
                this.minute = decoder.u8();
                this.second = decoder.u8();
                this.micro = (int) decoder.u32();
            } else {
                throw new IllegalArgumentException("invalid length for date/datetime/timestamp");
            }
        }
        break;

        case FieldType.MYSQL_TYPE_TIME: {
            final int len = decoder.u8_s();
            if (len == 0) {
                // all zero
            } else if (len == 8) {
                if (decoder.remaining() < 8) {
                    throw new IllegalArgumentException("invalid length for time");
                }
                this.negative = 1 == decoder.u8();
                this.day = (int) decoder.u32();
                this.hour = decoder.u8();
                this.minute = decoder.u8();
                this.second = decoder.u8();
            } else if (len == 12) {
                if (decoder.remaining() < 12) {
                    throw new IllegalArgumentException("invalid length for time");
                }
                this.negative = 1 == decoder.u8();
                this.day = (int) decoder.u32();
                this.hour = decoder.u8();
                this.minute = decoder.u8();
                this.second = decoder.u8();
                this.micro = (int) decoder.u32();
            } else {
                throw new IllegalArgumentException("invalid length for time");
            }
        }
        break;

        case FieldType.MYSQL_TYPE_NULL:
            // stored in the NULL-Bitmap only
            break;

        default:
            throw new IllegalArgumentException("invalid type " + type + " for parameter");
        }

        // value loaded
        this.validValue = true;
    }

    public void encodeName(Encoder encoder, boolean withName) throws IOException {
        encoder.u16(type);
        if (withName) {
            if (name != null) {
                encoder.le_str(name);
            } else {
                encoder.u8(0);
            }
        }
    }

    public void encodeValue(Encoder encoder) throws IOException {
        // skip if no valid value
        if (!validValue) {
            return;
        }

        switch (type) {
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
        case FieldType.MYSQL_TYPE_DECIMAL:
        case FieldType.MYSQL_TYPE_NEWDECIMAL:
            encoder.le_str(bytes);
            break;

        case FieldType.MYSQL_TYPE_LONGLONG:
            encoder.u64(l);
            break;

        case FieldType.MYSQL_TYPE_LONG:
        case FieldType.MYSQL_TYPE_INT24:
            encoder.u32(l);
            break;

        case FieldType.MYSQL_TYPE_SHORT:
        case FieldType.MYSQL_TYPE_YEAR:
            encoder.u16((int) l);
            break;

        case FieldType.MYSQL_TYPE_TINY:
            encoder.u8((int) l);
            break;

        case FieldType.MYSQL_TYPE_DOUBLE:
            encoder.d(d);
            break;

        case FieldType.MYSQL_TYPE_FLOAT:
            encoder.f(f);
            break;

        case FieldType.MYSQL_TYPE_DATE:
        case FieldType.MYSQL_TYPE_DATETIME:
        case FieldType.MYSQL_TYPE_TIMESTAMP: {
            if (0 == micro) {
                if (0 == hour && 0 == minute && 0 == second) {
                    if (0 == year && 0 == month && 0 == day) {
                        // all zero
                        encoder.u8(0);
                    } else {
                        encoder.u8(4);
                        encoder.u16(year);
                        encoder.u8(month);
                        encoder.u8(day);
                    }
                } else {
                    encoder.u8(7);
                    encoder.u16(year);
                    encoder.u8(month);
                    encoder.u8(day);
                    encoder.u8(hour);
                    encoder.u8(minute);
                    encoder.u8(second);
                }
            } else {
                encoder.u8(11);
                encoder.u16(year);
                encoder.u8(month);
                encoder.u8(day);
                encoder.u8(hour);
                encoder.u8(minute);
                encoder.u8(second);
                encoder.u32(micro);
            }
        }
        break;

        case FieldType.MYSQL_TYPE_TIME: {
            if (0 == micro) {
                if (0 == day && 0 == hour && 0 == minute && 0 == second) {
                    // all zero
                    encoder.u8(0);
                } else {
                    encoder.u8(8);
                    encoder.u8(negative ? 1 : 0);
                    encoder.u32(day);
                    encoder.u8(hour);
                    encoder.u8(minute);
                    encoder.u8(second);
                }
            } else {
                encoder.u8(12);
                encoder.u8(negative ? 1 : 0);
                encoder.u32(day);
                encoder.u8(hour);
                encoder.u8(minute);
                encoder.u8(second);
                encoder.u32(micro);
            }
        }
        break;

        case FieldType.MYSQL_TYPE_NULL:
            // stored in the NULL-Bitmap only
            break;

        default:
            throw new IllegalArgumentException("invalid type for parameter");
        }
    }

    public byte[] toBytes() {
        if (!validValue) {
            return null;
        }

        switch (type) {
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
        case FieldType.MYSQL_TYPE_DECIMAL:
        case FieldType.MYSQL_TYPE_NEWDECIMAL:
            return bytes;

        case FieldType.MYSQL_TYPE_LONGLONG:
        case FieldType.MYSQL_TYPE_LONG:
        case FieldType.MYSQL_TYPE_INT24:
        case FieldType.MYSQL_TYPE_SHORT:
        case FieldType.MYSQL_TYPE_YEAR:
            return Long.toString(l).getBytes();

        case FieldType.MYSQL_TYPE_DOUBLE:
            return Double.toString(d).getBytes();

        case FieldType.MYSQL_TYPE_FLOAT:
            return Float.toString(f).getBytes();

        case FieldType.MYSQL_TYPE_DATE:
        case FieldType.MYSQL_TYPE_DATETIME:
        case FieldType.MYSQL_TYPE_TIMESTAMP: {
            final String val =
                String.valueOf(year) + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second + '.' + micro;
            return val.getBytes();
        }

        case FieldType.MYSQL_TYPE_TIME: {
            final String val = (negative ? "-" : "") + day + "d " + hour + ':' + minute + ':' + second + '.' + micro;
            return val.getBytes();
        }

        case FieldType.MYSQL_TYPE_NULL:
            return null;

        default:
            throw new IllegalArgumentException("invalid type for parameter");
        }
    }

    public String toLogString() {
        if (!validValue) {
            return "<empty>";
        }

        switch (type) {
        case FieldType.MYSQL_TYPE_STRING:
        case FieldType.MYSQL_TYPE_VARCHAR:
        case FieldType.MYSQL_TYPE_VAR_STRING:
        case FieldType.MYSQL_TYPE_ENUM:
        case FieldType.MYSQL_TYPE_SET:
        case FieldType.MYSQL_TYPE_DECIMAL:
        case FieldType.MYSQL_TYPE_NEWDECIMAL:
            if (bytes.length > FastConfig.logSqlParamMaxLength) {
                return '\'' + new String(bytes, 0, FastConfig.logSqlParamMaxLength).replaceAll("'", "\\\\'") + "'...";
            }
            return '\'' + new String(bytes).replaceAll("'", "\\\\'") + '\'';

        case FieldType.MYSQL_TYPE_LONG_BLOB:
        case FieldType.MYSQL_TYPE_MEDIUM_BLOB:
        case FieldType.MYSQL_TYPE_BLOB:
        case FieldType.MYSQL_TYPE_TINY_BLOB:
        case FieldType.MYSQL_TYPE_GEOMETRY:
        case FieldType.MYSQL_TYPE_BIT:
            if (bytes.length > FastConfig.logSqlParamMaxLength) {
                return "x'" + BytesTools.bytes2Hex(bytes, 0, FastConfig.logSqlParamMaxLength) + "'...";
            }
            return "x'" + BytesTools.bytes2Hex(bytes) + '\'';

        case FieldType.MYSQL_TYPE_LONGLONG:
        case FieldType.MYSQL_TYPE_LONG:
        case FieldType.MYSQL_TYPE_INT24:
        case FieldType.MYSQL_TYPE_SHORT:
        case FieldType.MYSQL_TYPE_YEAR:
            return Long.toString(l);

        case FieldType.MYSQL_TYPE_DOUBLE:
            return Double.toString(d);

        case FieldType.MYSQL_TYPE_FLOAT:
            return Float.toString(f);

        case FieldType.MYSQL_TYPE_DATE:
        case FieldType.MYSQL_TYPE_DATETIME:
        case FieldType.MYSQL_TYPE_TIMESTAMP:
            return '\'' + String.valueOf(year) + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second
                + '.' + micro + '\'';

        case FieldType.MYSQL_TYPE_TIME:
            return '\'' + (negative ? "-" : "") + day + "d " + hour + ':' + minute + ':' + second + '.' + micro + '\'';

        case FieldType.MYSQL_TYPE_NULL:
            return "null";

        default:
            throw new IllegalArgumentException("invalid type for parameter");
        }
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        if (validValue) {
            switch (type) {
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
            case FieldType.MYSQL_TYPE_DECIMAL:
            case FieldType.MYSQL_TYPE_NEWDECIMAL:
                if (null == bytes) {
                    builder.append("<null>");
                } else {
                    builder.append('\n').append(BytesTools.beautifulHex(bytes, 0, bytes.length)).append('\n');
                }
                break;

            case FieldType.MYSQL_TYPE_LONGLONG:
            case FieldType.MYSQL_TYPE_LONG:
            case FieldType.MYSQL_TYPE_INT24:
            case FieldType.MYSQL_TYPE_SHORT:
            case FieldType.MYSQL_TYPE_YEAR:
                builder.append(l);
                break;

            case FieldType.MYSQL_TYPE_DOUBLE:
                builder.append(d);
                break;

            case FieldType.MYSQL_TYPE_FLOAT:
                builder.append(f);
                break;

            case FieldType.MYSQL_TYPE_DATE:
            case FieldType.MYSQL_TYPE_DATETIME:
            case FieldType.MYSQL_TYPE_TIMESTAMP:
                builder.append(year).append('-').append(month).append('-').append(day).append(' ').append(hour)
                    .append(':')
                    .append(minute).append(':').append(second).append('.').append(micro);
                break;

            case FieldType.MYSQL_TYPE_TIME:
                builder.append(negative ? "-" : "").append(day).append("d ").append(hour).append(':').append(minute)
                    .append(':').append(second).append('.').append(micro);
                break;

            case FieldType.MYSQL_TYPE_NULL:
                builder.append("<null>");
                break;

            default:
                throw new IllegalArgumentException("invalid type for parameter");
            }
        } else {
            builder.append("<empty>");
        }

        return "BinaryParameter{" +
            "type=" + type +
            ", name=" + (null == name ? "<null>" : new String(name)) +
            ", value=" + builder + '}';
    }
}
