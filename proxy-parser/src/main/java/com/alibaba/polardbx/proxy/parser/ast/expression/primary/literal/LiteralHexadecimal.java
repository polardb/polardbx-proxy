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
/**
 * (created at 2011-1-21)
 */
package com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal;

import com.alibaba.polardbx.proxy.parser.util.ParseString;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author QIU Shuo
 */
public class LiteralHexadecimal extends Literal {

    private byte[] bytes;
    private final String introducer;
    private final byte[] sql;
    private final int offset;
    private final int size;

    /**
     * @param introducer e.g. "_latin1"
     * @param sql e.g. "select x'89df'"
     * @param offset e.g. 9
     * @param size e.g. 4
     */
    public LiteralHexadecimal(String introducer, byte[] sql, int offset, int size) {
        super();
        if (sql == null || offset + size > sql.length) {
            throw new IllegalArgumentException("hex text is invalid");
        }
        this.introducer = introducer;
        this.sql = sql;
        this.offset = offset;
        this.size = size;
    }

    public String getText() {
        return new String(sql, offset, size, StandardCharsets.UTF_8);
    }

    public String getIntroducer() {
        return introducer;
    }

    public void appendTo(StringBuilder sb) {
        sb.append(new String(sql, offset, size, StandardCharsets.UTF_8));
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        // try {
        this.bytes = ParseString.hexString2Bytes(sql, offset, size);
        return this.bytes;
        // return new String(bytes, introducer == null ? charset :
        // introducer.substring(1));
        // } catch (UnsupportedEncodingException e) {
        // throw new RuntimeException("", e);
        // }
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

}
