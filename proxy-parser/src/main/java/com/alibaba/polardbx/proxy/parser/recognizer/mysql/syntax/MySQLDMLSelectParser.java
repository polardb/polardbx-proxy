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
 * (created at 2011-5-13)
 */
package com.alibaba.polardbx.proxy.parser.recognizer.mysql.syntax;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.fragment.GroupBy;
import com.alibaba.polardbx.proxy.parser.ast.fragment.Limit;
import com.alibaba.polardbx.proxy.parser.ast.fragment.OrderBy;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.Dual;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.TableReference;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLQueryStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectFromUpdateStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectFromUpdateStatement.SelectFromUpdateOption;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectUnionStatement;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.lexer.MySQLLexer;
import com.alibaba.polardbx.proxy.parser.util.Pair;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_DUAL;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_FROM;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_HAVING;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_IN;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_SELECT;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_SET;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_UPDATE;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.KW_WHERE;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.OP_ASSIGN;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.OP_EQUALS;
import static com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken.PUNC_COMMA;

/**
 * @author QIU Shuo
 */
public class MySQLDMLSelectParser extends MySQLDMLParser {

    public MySQLDMLSelectParser(MySQLLexer lexer, MySQLExprParser exprParser) {
        super(lexer, exprParser);
        this.exprParser.setSelectParser(this);
    }

    private static enum SpecialIdentifier {
        SQL_BUFFER_RESULT, SQL_CACHE, SQL_NO_CACHE, COMMIT_ON_SUCCESS, ROLLBACK_ON_FAIL, QUEUE_ON_PK, TARGET_AFFECT_ROW
    }

    private static final Map<String, SpecialIdentifier> specialIdentifiers = new HashMap<String, SpecialIdentifier>();

    static {
        specialIdentifiers.put("SQL_BUFFER_RESULT", SpecialIdentifier.SQL_BUFFER_RESULT);
        specialIdentifiers.put("SQL_CACHE", SpecialIdentifier.SQL_CACHE);
        specialIdentifiers.put("SQL_NO_CACHE", SpecialIdentifier.SQL_NO_CACHE);
        specialIdentifiers.put("COMMIT_ON_SUCCESS", SpecialIdentifier.COMMIT_ON_SUCCESS);
        specialIdentifiers.put("ROLLBACK_ON_FAIL", SpecialIdentifier.ROLLBACK_ON_FAIL);
        specialIdentifiers.put("QUEUE_ON_PK", SpecialIdentifier.QUEUE_ON_PK);
        specialIdentifiers.put("TARGET_AFFECT_ROW", SpecialIdentifier.TARGET_AFFECT_ROW);
    }

    private DMLSelectStatement.SelectOption selectOption() throws SQLSyntaxErrorException {
        for (DMLSelectStatement.SelectOption option = new DMLSelectStatement.SelectOption(); ; lexer.nextToken()) {
            outer:
            switch (lexer.token()) {
            case KW_ALL:
                option.resultDup = DMLSelectStatement.SelectDuplicationStrategy.ALL;
                break;
            case KW_DISTINCT:
                option.resultDup = DMLSelectStatement.SelectDuplicationStrategy.DISTINCT;
                break;
            case KW_DISTINCTROW:
                option.resultDup = DMLSelectStatement.SelectDuplicationStrategy.DISTINCTROW;
                break;
            case KW_HIGH_PRIORITY:
                option.highPriority = true;
                break;
            case KW_STRAIGHT_JOIN:
                option.straightJoin = true;
                break;
            case KW_SQL_SMALL_RESULT:
                option.resultSize = DMLSelectStatement.SmallOrBigResult.SQL_SMALL_RESULT;
                break;
            case KW_SQL_BIG_RESULT:
                option.resultSize = DMLSelectStatement.SmallOrBigResult.SQL_BIG_RESULT;
                break;
            case KW_SQL_CALC_FOUND_ROWS:
                option.sqlCalcFoundRows = true;
                break;
            case IDENTIFIER:
                String optionStringUp = lexer.stringValueUppercase();
                SpecialIdentifier specialId = specialIdentifiers.get(optionStringUp);
                if (specialId != null) {
                    switch (specialId) {
                    case SQL_BUFFER_RESULT:
                        if (option.sqlBufferResult) {
                            return option;
                        }
                        option.sqlBufferResult = true;
                        break outer;
                    case SQL_CACHE:
                        if (option.queryCache != DMLSelectStatement.QueryCacheStrategy.UNDEF) {
                            return option;
                        }
                        option.queryCache = DMLSelectStatement.QueryCacheStrategy.SQL_CACHE;
                        break outer;
                    case SQL_NO_CACHE:
                        if (option.queryCache != DMLSelectStatement.QueryCacheStrategy.UNDEF) {
                            return option;
                        }
                        option.queryCache = DMLSelectStatement.QueryCacheStrategy.SQL_NO_CACHE;
                        break outer;
                    }
                }
            default:
                return option;
            }
        }
    }

    private List<Pair<Expression, String>> selectExprList() throws SQLSyntaxErrorException {

        int beginOffset = lexer.getLastTokenIndex();
        Expression expr = exprParser.expression();
        int endOffset = lexer.getLastTokenIndex();

        String str = lexer.subSql(beginOffset, endOffset - beginOffset).trim();
        expr.setOriginStr(str);
        String alias = as();
        List<Pair<Expression, String>> list;
        if (lexer.token() == PUNC_COMMA) {
            list = new ArrayList<>();
            list.add(new Pair<>(expr, alias));
        } else {
            list = new ArrayList<>(1);
            list.add(new Pair<>(expr, alias));
            return list;
        }
        for (; lexer.token() == PUNC_COMMA; list.add(new Pair<>(expr, alias))) {
            lexer.nextToken();
            beginOffset = lexer.getLastTokenIndex();
            expr = exprParser.expression();
            endOffset = lexer.getLastTokenIndex();
            str = lexer.subSql(beginOffset, endOffset - beginOffset).trim();
            expr.setOriginStr(str);
            alias = as();
        }

        return list;
    }

    @Override
    public DMLSelectStatement select(boolean ignoreOrderByAndLimit) throws SQLSyntaxErrorException {
        match(KW_SELECT);
        DMLSelectStatement.SelectOption option = selectOption();
        List<Pair<Expression, String>> exprList = selectExprList();
        TableReferences tables = null;
        Expression where = null;
        GroupBy group = null;
        Expression having = null;
        OrderBy order = null;
        Limit limit = null;

        boolean dual = false;
        if (lexer.token() == KW_FROM) {
            if (lexer.nextToken() == KW_DUAL) {
                lexer.nextToken();
                dual = true;
                List<TableReference> trs = new ArrayList<>(1);
                trs.add(new Dual());
                tables = new TableReferences(trs);
            } else if (lexer.token() == KW_UPDATE) {
                match(KW_UPDATE);
                SelectFromUpdateOption selectFromUpdateOption = selectFromUpdateOption();
                Identifier table = identifier();
                match(KW_SET);
                List<Pair<Identifier, Expression>> values;
                Identifier col = identifier();
                match(OP_EQUALS, OP_ASSIGN);
                Expression expr = exprParser.expression();
                if (lexer.token() == PUNC_COMMA) {
                    values = new ArrayList<>();
                    values.add(new Pair<>(col, expr));
                    for (; lexer.token() == PUNC_COMMA; ) {
                        lexer.nextToken();
                        col = identifier();
                        match(OP_EQUALS, OP_ASSIGN);
                        expr = exprParser.expression();
                        values.add(new Pair<>(col, expr));
                    }
                } else {
                    values = new ArrayList<>(1);
                    values.add(new Pair<>(col, expr));
                }
                if (lexer.token() == KW_WHERE) {
                    lexer.nextToken();
                    where = exprParser.expression();
                }
                OrderBy orderBy = orderBy();
                limit = dmlLimit();
                return new DMLSelectFromUpdateStatement(selectFromUpdateOption,
                    exprList,
                    table,
                    values,
                    where,
                    orderBy,
                    limit);
            } else {
                tables = tableRefs();
            }

        }
        if (lexer.token() == KW_WHERE) {
            lexer.nextToken();
            where = exprParser.expression();
        }
        if (!dual) {
            group = groupBy();
            if (lexer.token() == KW_HAVING) {
                lexer.nextToken();
                having = exprParser.expression();
            }

            /**
             * 是没带括号的union子句，剩下的全部算到union层
             */
            if (ignoreOrderByAndLimit) {
                return new DMLSelectStatement(option, exprList, tables, where, group, having, order, limit);
            }
            order = orderBy();
        }
        limit = limit();
        if (!dual) {
            switch (lexer.token()) {
            case KW_FOR:
                lexer.nextToken();
                match(KW_UPDATE);
                option.lockMode = DMLSelectStatement.LockMode.FOR_UPDATE;
                break;
            case KW_LOCK:
                lexer.nextToken();
                match(KW_IN);
                matchIdentifier("SHARE");
                matchIdentifier("MODE");
                option.lockMode = DMLSelectStatement.LockMode.LOCK_IN_SHARE_MODE;
                break;
            }
        }
        return new DMLSelectStatement(option, exprList, tables, where, group, having, order, limit);
    }

    private SelectFromUpdateOption selectFromUpdateOption() throws SQLSyntaxErrorException {
        int paramIndex1;
        Number num1;
        for (SelectFromUpdateOption option = new SelectFromUpdateOption(); ; lexer.nextToken()) {
            outer:
            switch (lexer.token()) {
            case KW_LOW_PRIORITY:
                option.lowPriority = true;
                break;
            case KW_IGNORE:
                option.ignore = true;
                break;
            case IDENTIFIER:
                String optionStringUp = lexer.stringValueUppercase();
                SpecialIdentifier specialId = specialIdentifiers.get(optionStringUp);
                if (specialId != null) {
                    switch (specialId) {
                    case COMMIT_ON_SUCCESS:
                        option.commitOnSuccess = true;
                        break outer;
                    case ROLLBACK_ON_FAIL:
                        option.rollbackOnFail = true;
                        break outer;
                    case QUEUE_ON_PK:
                        option.queueOnPk = true;
                        switch (lexer.nextToken()) {
                        case LITERAL_NUM_PURE_DIGIT:
                            num1 = lexer.integerValue();
                            option.queueOnPkNum = num1;
                            break outer;
                        case QUESTION_MARK:
                            paramIndex1 = lexer.paramIndex();
                            option.queueOnPkNumP = createParam(paramIndex1);
                            break outer;
                        default:
                            throw err("expect digit or ? after QUEUE_ON_PK");
                        }

                    case TARGET_AFFECT_ROW:
                        option.targetAffectRow = true;

                        switch (lexer.nextToken()) {
                        case LITERAL_NUM_PURE_DIGIT:
                            num1 = lexer.integerValue();
                            option.num = num1;
                            break outer;
                        case QUESTION_MARK:
                            paramIndex1 = lexer.paramIndex();
                            option.numP = createParam(paramIndex1);
                            break outer;
                        default:
                            throw err("expect digit or ? after TARGET_AFFECT_ROW");
                        }
                    }
                }
            default:
                return option;
            }
        }
    }

    @Override
    public DMLSelectStatement select() throws SQLSyntaxErrorException {
        return this.select(false);
    }

    /**
     * first token is either {@link MySQLToken#KW_SELECT} or
     * {@link MySQLToken#PUNC_LEFT_PAREN} which has been scanned but not yet
     * consumed
     *
     * @return {@link DMLSelectStatement} or {@link DMLSelectUnionStatement}
     */
    public DMLQueryStatement selectUnion() throws SQLSyntaxErrorException {
        DMLSelectStatement select = selectPrimary();
        DMLQueryStatement query = buildUnionSelect(select);
        return query;
    }

}
