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

package com.alibaba.polardbx.proxy.parser.recognizer;

import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.lexer.MySQLLexer;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.syntax.KillParser;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.syntax.MySQLDALParser;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.syntax.MySQLExprParser;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

public class SQLParser {
    private final MySQLLexer lexer;
    @Getter
    private final MySQLToken firstToken;

    public SQLParser(@NotNull final String sql) throws SQLSyntaxErrorException {
        this(sql.getBytes(StandardCharsets.UTF_8), 0, sql.length(), StandardCharsets.UTF_8, null,
            MySQLLexer.DEFAULT_VERSION);
    }

    public SQLParser(final byte @NotNull [] sql, final int offset, final int length, @NotNull final Charset charset,
                     String sqlMode, int version) throws SQLSyntaxErrorException {
        final boolean noBackslashEscapes = sqlMode != null && sqlMode.contains("NO_BACKSLASH_ESCAPES");
        this.lexer = new MySQLLexer(sql, charset, noBackslashEscapes, version, offset, length);
        this.firstToken = this.lexer.nextToken();
    }

    public boolean isMultiStatement() throws SQLSyntaxErrorException {
        lexer.reset();
        MySQLToken token;
        while ((token = lexer.nextToken()) != MySQLToken.EOF) {
            if (MySQLToken.PUNC_SEMICOLON == token) {
                return MySQLToken.EOF != lexer.nextToken();
            }
        }
        return false;
    }

    public boolean canSlaveRead() throws SQLSyntaxErrorException {
        lexer.reset();
        MySQLToken token = lexer.nextToken();
        // first should be select
        while (MySQLToken.KW_SELECT == token) {
            token = lexer.nextToken();

            // SELECT with PROCEDURE syntax is deprecated as of MySQL 5.7.18, and is removed in MySQL 8.0.
            // So ignore the procedure syntax.

            /*
             * Check lock syntax.
             *
             * [FOR {UPDATE | SHARE}
             *     [OF tbl_name [, tbl_name] ...]
             *     [NOWAIT | SKIP LOCKED]
             *   | LOCK IN SHARE MODE]
             */
            while (token != MySQLToken.EOF && token != MySQLToken.PUNC_SEMICOLON) {
                if (MySQLToken.KW_FOR == token) {
                    token = lexer.nextToken();
                    if (MySQLToken.KW_UPDATE == token || MySQLToken.KW_SHARE == token) {
                        return false;
                    }
                    continue;
                } else if (MySQLToken.KW_LOCK == token) {
                    token = lexer.nextToken();
                    if (MySQLToken.KW_IN == token) {
                        token = lexer.nextToken();
                        if (MySQLToken.KW_SHARE == token) {
                            token = lexer.nextToken();
                            if (MySQLToken.KW_MODE == token) {
                                return false;
                            }
                        }
                    }
                    continue;
                }
                token = lexer.nextToken();
            }

            if (token == MySQLToken.EOF) {
                return true;
            }
            // else continue check next statement
            token = lexer.nextToken();
        }
        return false;
    }

    public boolean isReadOnly() throws SQLSyntaxErrorException {
        lexer.reset();
        MySQLToken token = lexer.nextToken();
        while (MySQLToken.KW_SELECT == token) {
            // first should be select
            token = lexer.nextToken();

            // SELECT with PROCEDURE syntax is deprecated as of MySQL 5.7.18, and is removed in MySQL 8.0.
            // So ignore the procedure syntax.

            // consume to end of statement
            while (token != MySQLToken.EOF && token != MySQLToken.PUNC_SEMICOLON) {
                token = lexer.nextToken();
            }

            if (token == MySQLToken.EOF) {
                return true;
            }
            // else continue check next statement
            token = lexer.nextToken();
        }
        return false;
    }

    public boolean isPrivilegeDatabaseChanged() throws SQLSyntaxErrorException {
        lexer.reset();
        MySQLToken token = lexer.nextToken();
        while (true) {
            // check first token
            if (MySQLToken.KW_FLUSH == token) {
                token = lexer.nextToken();
                if (MySQLToken.KW_PRIVILEGES == token) {
                    return true;
                }
            } else if (MySQLToken.KW_CREATE == token) {
                token = lexer.nextToken();
                if (MySQLToken.KW_DATABASE == token || MySQLToken.KW_SCHEMA == token || MySQLToken.KW_USER == token) {
                    return true;
                }
            } else if (MySQLToken.KW_DROP == token) {
                token = lexer.nextToken();
                if (MySQLToken.KW_DATABASE == token || MySQLToken.KW_SCHEMA == token || MySQLToken.KW_USER == token) {
                    return true;
                }
            } else if (MySQLToken.KW_ALTER == token) {
                token = lexer.nextToken();
                if (MySQLToken.KW_USER == token) {
                    return true;
                }
            } else if (MySQLToken.KW_RENAME == token) {
                token = lexer.nextToken();
                if (MySQLToken.KW_USER == token) {
                    return true;
                }
            } else if (MySQLToken.KW_SET == token) {
                token = lexer.nextToken();
                if (MySQLToken.KW_PASSWORD == token) {
                    return true;
                }
            }

            // consume to end of statement
            while (token != MySQLToken.EOF && token != MySQLToken.PUNC_SEMICOLON) {
                token = lexer.nextToken();
            }

            if (MySQLToken.EOF == token) {
                return false;
            }
            // else continue check next statement
            token = lexer.nextToken();
        }
    }

    public String applyDatabase(String nowDatabase, boolean lowerCase) throws SQLSyntaxErrorException {
        return applyDatabase(nowDatabase, lowerCase, null);
    }

    public String applyDatabase(String nowDatabase, boolean lowerCase, boolean[] stmtGood)
        throws SQLSyntaxErrorException {
        lexer.reset();
        MySQLToken token = lexer.nextToken();
        int stmtId = 0;
        while (true) {
            do {
                if (stmtGood != null && stmtId < stmtGood.length && !stmtGood[stmtId]) {
                    break; // ignore stmt which is not success
                }

                if (MySQLToken.KW_DROP == token) {
                    token = lexer.nextToken();
                    if (MySQLToken.KW_DATABASE == token || MySQLToken.KW_SCHEMA == token) {
                        token = lexer.nextToken();
                        if (MySQLToken.KW_IF == token) {
                            token = lexer.nextToken();
                            if (MySQLToken.KW_EXISTS == token) {
                                lexer.nextToken();
                            } else {
                                break;
                            }
                        }
                        String db = lexer.originalStringValue();
                        if (db.length() >= 2 && db.charAt(0) == '`' && db.charAt(db.length() - 1) == '`') {
                            db = db.substring(1, db.length() - 1).replace("``", "`");
                        }

                        // clear current db if same
                        if (lowerCase) {
                            if (db.equalsIgnoreCase(nowDatabase)) {
                                nowDatabase = null;
                            }
                        } else if (db.equals(nowDatabase)) {
                            nowDatabase = null;
                        }
                    }
                } else if (MySQLToken.KW_USE == token) {
                    lexer.nextToken();
                    String db = lexer.originalStringValue();
                    if (db.length() >= 2 && db.charAt(0) == '`' && db.charAt(db.length() - 1) == '`') {
                        db = db.substring(1, db.length() - 1).replace("``", "`");
                    }

                    // switch db
                    nowDatabase = db;
                }
            } while (false);

            // consume to end of statement
            while (token != MySQLToken.EOF && token != MySQLToken.PUNC_SEMICOLON) {
                token = lexer.nextToken();
            }

            if (MySQLToken.EOF == token) {
                break;
            }
            // else continue check next statement
            token = lexer.nextToken();
            ++stmtId;
        }
        return nowDatabase;
    }

    public static String buildErrorMsg(Exception e, MySQLLexer lexer) {
        final StringBuilder sb =
            new StringBuilder("You have an error in your SQL syntax; Error occurs around this fragment: ");
        final int ch = lexer.getPos();
        int from = ch - 16;
        if (from < 0) {
            from = 0;
        }
        int to = ch + 9;
        if (to >= lexer.getSql().length) {
            to = lexer.getSql().length - 1;
        }
        final String fragment = lexer.subSql(from, to + 1 - from);
        sb.append('{').append(fragment).append('}');
        if (e != null) {
            sb.append(". Error cause: ").append(e.getMessage());
        }
        return sb.toString();
    }

    // todo more check and fix on syntax parser
    public List<SQLStatement> parseMultiStatements() throws SQLSyntaxErrorException {
        final List<SQLStatement> stmtList = new ArrayList<>();

        lexer.reset();
        lexer.nextToken();
        while (true) {
            try {
                MySQLToken lasToken = lexer.getLastToken();
                if (lasToken != null && lasToken != MySQLToken.PUNC_SEMICOLON) {
                    throw new SQLSyntaxErrorException(
                        "sql is not a supported statement, maybe you have an error in your SQL syntax");
                }

                SQLStatement stmt = null;
                final MySQLExprParser exprParser = new MySQLExprParser(lexer);
                boolean explain = false;

                if (MySQLToken.KW_EXPLAIN == lexer.token()) {
                    lexer.nextToken();
                    explain = true;
                }

                switch (lexer.token()) {
                case KW_KILL:
                    stmt = new KillParser(lexer).kill();
                    break;

                case KW_SET:
                    stmt = new MySQLDALParser(lexer, exprParser).set();
                    break;

                case KW_SHOW:
                    stmt = new MySQLDALParser(lexer, exprParser).show();
                    break;

                case PUNC_SEMICOLON:
                    break;

                default:
                    // todo: throw if not parser or consume until ';' or EOF
                    while (lexer.token() != MySQLToken.EOF && lexer.token() != MySQLToken.PUNC_SEMICOLON) {
                        lexer.nextToken();
                    }
                }

                stmtList.add(stmt);
                if (lexer.token() == MySQLToken.EOF) {
                    break;
                } else if (lexer.token() == MySQLToken.PUNC_SEMICOLON) {
                    lexer.nextToken();
                }
            } catch (Exception e) {
                throw new SQLSyntaxErrorException(buildErrorMsg(e, lexer), e);
            }
        }

        return stmtList;
    }
}
