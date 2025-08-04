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
package com.alibaba.polardbx.proxy.parser.recognizer.mysql.syntax;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.Kill;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.lexer.MySQLLexer;

import java.sql.SQLSyntaxErrorException;

public class KillParser extends MySQLParser {
    public KillParser(MySQLLexer lexer) {
        super(lexer);
    }

    /**
     * reload schema
     */
    public SQLStatement kill() throws SQLSyntaxErrorException {
        final MySQLToken token = lexer.nextToken();
        final boolean killConnection;
        if (token == MySQLToken.KW_CONNECTION) {
            killConnection = true;
            lexer.nextToken();
        } else if (token == MySQLToken.KW_QUERY) {
            killConnection = false;
            lexer.nextToken();
        } else {
            killConnection = true;
        }
        final Number id = lexer.integerValue();
        lexer.nextToken();
        return new Kill(killConnection, new LiteralNumber(id));
    }
}
