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
 * (created at 2011-5-19)
 */
package com.alibaba.polardbx.proxy.parser.recognizer.mysql.syntax;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.SysVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.UsrDefVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.VariableExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralBoolean;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.polardbx.proxy.parser.ast.fragment.VariableScope;
import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetCharacterSetStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetNamesStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALShowStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowBackend;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowCluster;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowFrontend;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowProperties;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowRO;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowRW;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowReactor;
import com.alibaba.polardbx.proxy.parser.ast.stmt.mts.MTSSetTransactionStatement;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.lexer.MySQLLexer;
import com.alibaba.polardbx.proxy.parser.util.Pair;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.IDENTIFIER;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_CHARACTER;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_CHARSET;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_COLLATE;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_DEFAULT;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_FULL;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_LEVEL;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_NAMES;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_OPTION;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_READ;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_SET;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_SHOW;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_TRANSACTION;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.OP_ASSIGN;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.OP_EQUALS;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.PUNC_COMMA;

/**
 * @author QIU Shuo
 */
public class MySQLDALParser extends MySQLParser {
    protected MySQLExprParser exprParser;

    public MySQLDALParser(MySQLLexer lexer, MySQLExprParser exprParser) {
        super(lexer);
        this.exprParser = exprParser;
    }

    private enum SpecialIdentifier {
        CLUSTER, RO, RW, PROPERTIES, REACTOR, FRONTEND, BACKEND
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers = new HashMap<>();

    static {
        specialIdentifiers.put("CLUSTER", SpecialIdentifier.CLUSTER);
        specialIdentifiers.put("RO", SpecialIdentifier.RO);
        specialIdentifiers.put("RW", SpecialIdentifier.RW);
        specialIdentifiers.put("PROPERTIES", SpecialIdentifier.PROPERTIES);
        specialIdentifiers.put("REACTOR", SpecialIdentifier.REACTOR);
        specialIdentifiers.put("FRONTEND", SpecialIdentifier.FRONTEND);
        specialIdentifiers.put("BACKEND", SpecialIdentifier.BACKEND);
    }

    public DALShowStatement show() throws SQLSyntaxErrorException {
        match(KW_SHOW);
        if (IDENTIFIER == lexer.token()) {
            final SpecialIdentifier tempSi = specialIdentifiers.get(lexer.stringValueUppercase());
            if (tempSi != null) {
                switch (tempSi) {
                case CLUSTER:
                    lexer.nextToken();
                    return new ShowCluster();
                case RW:
                    lexer.nextToken();
                    return new ShowRW();
                case RO:
                    lexer.nextToken();
                    return new ShowRO();
                case PROPERTIES:
                    lexer.nextToken();
                    return new ShowProperties();
                case REACTOR:
                    lexer.nextToken();
                    return new ShowReactor();
                case FRONTEND:
                    lexer.nextToken();
                    return new ShowFrontend();
                case BACKEND:
                    lexer.nextToken();
                    return new ShowBackend();
                }
            }
        } else if (KW_FULL == lexer.token()) {
            lexer.nextToken();
            if (IDENTIFIER == lexer.token()) {
                final SpecialIdentifier tempSi = specialIdentifiers.get(lexer.stringValueUppercase());
                if (tempSi != null) {
                    switch (tempSi) {
                    case FRONTEND:
                        lexer.nextToken();
                        return new ShowFrontend(true);
                    case BACKEND:
                        lexer.nextToken();
                        return new ShowBackend(true);
                    }
                }
            }
        }

        // or consume until eof or ';'
        while (lexer.token() != MySQLToken.EOF && lexer.token() != MySQLToken.PUNC_SEMICOLON) {
            lexer.nextToken();
        }
        return null;
    }

    private String getStringValue() throws SQLSyntaxErrorException {
        String name;
        switch (lexer.token()) {
        case IDENTIFIER:
            name = Identifier.unescapeName(lexer.stringValue());
            lexer.nextToken();
            return name;
        case LITERAL_CHARS:
            name = lexer.stringValue();
            name = LiteralString.getUnescapedString(name.substring(1, name.length() - 1));
            lexer.nextToken();
            return name;
        default:
            throw err("unexpected token: " + lexer.token());
        }
    }

    /**
     * @return {@link DALSetStatement} or {@link MTSSetTransactionStatement}
     */
    @SuppressWarnings("unchecked")
    public SQLStatement set() throws SQLSyntaxErrorException {
        match(KW_SET);
        if (lexer.token() == KW_OPTION) {
            lexer.nextToken();
        }

        List<Pair<VariableExpression, Expression>> assignmentList;
        if (lexer.token() == KW_CHARACTER) {
            lexer.nextToken();
            match(KW_SET);
            if (lexer.token() == KW_DEFAULT) {
                lexer.nextToken();
                return new DALSetCharacterSetStatement();
            }
            String charsetName = getStringValue();
            return new DALSetCharacterSetStatement(charsetName);
        } else if (lexer.token() == KW_CHARSET) {
            lexer.nextToken();
            if (lexer.token() == KW_DEFAULT) {
                lexer.nextToken();
                return new DALSetCharacterSetStatement();
            }
            String charsetName = getStringValue();
            return new DALSetCharacterSetStatement(charsetName);
        } else if (lexer.token() == KW_NAMES) {
            lexer.nextToken();
            if (lexer.token() == KW_DEFAULT) {
                lexer.nextToken();
                return new DALSetNamesStatement();
            }
            final String charsetName = getStringValue();
            final String collationName;
            if (lexer.token() == KW_COLLATE) {
                lexer.nextToken();
                collationName = getStringValue();
            } else {
                collationName = null;
            }
            return new DALSetNamesStatement(charsetName, collationName);
        }

        Object obj = varAssign();
        if (obj instanceof MTSSetTransactionStatement) {
            return (MTSSetTransactionStatement) obj;
        }

        Pair<VariableExpression, Expression> pair;
        assignmentList = new ArrayList<>();

        if (obj instanceof Pair) {
            pair = (Pair<VariableExpression, Expression>) obj;
            assignmentList.add(pair);
        } else {
            assignmentList.addAll((List) obj);
        }

        for (; lexer.token() == PUNC_COMMA; ) {
            lexer.nextToken();
            obj = varAssign();

            if (obj instanceof Pair) {
                pair = (Pair<VariableExpression, Expression>) obj;
                assignmentList.add(pair);
            } else {
                assignmentList.addAll((List) obj);
            }
        }
        return new DALSetStatement(assignmentList);
    }

    /**
     * first token is <code>TRANSACTION</code>
     */
    private Object setMTSSetTransactionStatement(VariableScope scope) throws SQLSyntaxErrorException {
        lexer.nextToken();
        switch (lexer.token()) {
        case KW_READ: {
            final SysVarPrimary read = new SysVarPrimary(VariableScope.SESSION,
                lexer.stringValue(),
                lexer.stringValueUppercase());
            lexer.nextToken();
            switch (lexer.token()) {
            case KW_WRITE:
            case KW_ONLY: {
                final Expression write = new LiteralString(null, lexer.stringValue(), false);
                lexer.nextToken();
                return new Pair<VariableExpression, Expression>(read, write);
            }
            }
            throw err("unexpected token for SET TRANSACTION statement");
        }

        case IDENTIFIER:
            lexer.nextToken();
            switch (lexer.getLastToken()) {
            case KW_ISOLATION:
                match(KW_LEVEL);
                switch (lexer.token()) {
                case KW_READ:
                    lexer.nextToken();
                    switch (lexer.token()) {
                    case KW_COMMITTED:
                        lexer.nextToken();
                        return new MTSSetTransactionStatement(scope,
                            MTSSetTransactionStatement.IsolationLevel.READ_COMMITTED);
                    case KW_UNCOMMITTED:
                        lexer.nextToken();
                        return new MTSSetTransactionStatement(scope,
                            MTSSetTransactionStatement.IsolationLevel.READ_UNCOMMITTED);
                    }
                    throw err("unknown isolation read level: " + lexer.stringValue());
                case KW_REPEATABLE:
                    lexer.nextToken();
                    match(KW_READ);
                    return new MTSSetTransactionStatement(scope,
                        MTSSetTransactionStatement.IsolationLevel.REPEATABLE_READ);
                case KW_SERIALIZABLE:
                    lexer.nextToken();
                    return new MTSSetTransactionStatement(scope,
                        MTSSetTransactionStatement.IsolationLevel.SERIALIZABLE);
                }
                throw err("unknown isolation level: " + lexer.stringValue());

            case IDENTIFIER:
                if (lexer.stringValueUppercase().equals("POLICY")) {
                    final SysVarPrimary transactionPolicy = new SysVarPrimary(VariableScope.SESSION,
                        "transaction policy",
                        "TRANSACTION POLICY");

                    final Expression policy = new LiteralNumber(lexer.integerValue());
                    lexer.nextToken();
                    return new Pair<VariableExpression, Expression>(transactionPolicy, policy);
                }
            default:
                throw err("unexpected token for SET TRANSACTION statement");
            }
        default:
            throw err("unexpected token for SET TRANSACTION statement");
        }
    }

    private Object varAssign() throws SQLSyntaxErrorException {
        VariableExpression var;
        Expression expr;
        VariableScope scope = VariableScope.SESSION;
        switch (lexer.token()) {
        case KW_NAMES: {
            final List<Pair<VariableExpression, Expression>> assignmentList = new ArrayList<>();
            final String charsetName;
            String collationName = null;
            lexer.nextToken();
            if (lexer.token() == KW_DEFAULT) {
                lexer.nextToken();
                charsetName = "default";
            } else if (lexer.token() == MySQLToken.KW_BINARY) {
                lexer.nextToken();
                charsetName = "binary";
            } else {
                charsetName = getStringValue();
                if (lexer.token() == KW_COLLATE) {
                    lexer.nextToken();
                    collationName = getStringValue();
                }
            }

            final SysVarPrimary charset = new SysVarPrimary(VariableScope.SESSION, "names", "NAMES");
            final Expression charsetValue = new LiteralString(null, charsetName, false);
            assignmentList.add(new Pair<>(charset, charsetValue));
            if (collationName != null) {
                SysVarPrimary collate = new SysVarPrimary(VariableScope.SESSION, "collate", "COLLATE");
                Expression collateValue = new LiteralString(null, collationName, false);
                assignmentList.add(new Pair<>(collate, collateValue));
            }
            return assignmentList;
        }

        case KW_TRANSACTION:
            return setMTSSetTransactionStatement(null);

        case IDENTIFIER: {
            final Identifier key = identifier();
            var = new SysVarPrimary(scope, key.getIdTextUnescape(), key.getIdTextUpUnescape());
        }
        break;

        case SYS_VAR:
            var = systemVariale();
            break;

        case USR_VAR:
            var = new UsrDefVarPrimary(lexer.stringValue());
            lexer.nextToken();
            break;

        case KW_GLOBAL:
            scope = VariableScope.GLOBAL;
        case KW_SESSION:
        case KW_LOCAL:
            if (KW_TRANSACTION == lexer.nextToken()) {
                return setMTSSetTransactionStatement(scope);
            }
            Identifier key = identifier();
            var = new SysVarPrimary(scope, key.getIdTextUnescape(), key.getIdTextUpUnescape());
            break;

        default:
            throw err("unexpected token for SET statement");
        }
        match(OP_EQUALS, OP_ASSIGN);

        switch (lexer.token()) {
        case KW_BINARY:
            expr = new LiteralString(null, "binary", false);
            lexer.nextToken();
            break;
        case KW_ON:
            expr = new LiteralBoolean(true);
            lexer.nextToken();
            break;
        default:
            expr = exprParser.expression();
            break;
        }
        return new Pair<>(var, expr);
    }
}
