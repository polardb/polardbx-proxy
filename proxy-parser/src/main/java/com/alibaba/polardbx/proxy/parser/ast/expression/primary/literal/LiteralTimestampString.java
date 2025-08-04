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
package com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal;

/**
 * Created by chuanqin on 17/9/13.
 */
public class LiteralTimestampString extends LiteralString {

    /**
     * @param string content of string, excluded of head and tail "'". e.g. for
     */
    public LiteralTimestampString(String introducer, String string, boolean nchars) {
        super(introducer, string, nchars);
    }

    @Override
    public LiteralStringType getType() {
        return LiteralStringType.TIMESTAMP;
    }
}
