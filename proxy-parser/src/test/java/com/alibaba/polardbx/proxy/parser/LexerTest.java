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

package com.alibaba.polardbx.proxy.parser;

import com.alibaba.polardbx.proxy.parser.recognizer.mysql.lexer.MySQLLexer;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

public class LexerTest {
    private String parse(final String sql) throws SQLSyntaxErrorException {
        final MySQLLexer lexer = new MySQLLexer(sql.getBytes(), StandardCharsets.UTF_8, false);
        lexer.setRecordComments(true);
        final StringBuilder builder = new StringBuilder();
        MySQLToken token;
        while ((token = lexer.nextToken()) != MySQLToken.EOF) {
            final List<byte[]> comments = lexer.getComments();
            if (comments != null) {
                for (byte[] comment : comments) {
                    builder.append("comment:").append(new String(comment, lexer.getCharset()).trim()).append("\n");
                }
            }
            builder.append(token.name()).append(":").append(lexer.originalStringValue()).append("\n");
        }
        // final comments
        final List<byte[]> comments = lexer.getComments();
        if (comments != null) {
            for (byte[] comment : comments) {
                builder.append("comment:").append(new String(comment, lexer.getCharset()).trim()).append("\n");
            }
        }
        return builder.toString();
    }

    @Test
    public void simple0Test() throws Exception {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "\t\t\tselect\n"
            + "col,  count(*),   /*+ HINT_NAME([argument_list]) */       /*!50714 col1 */  count(*),  col2  -- comment\n"
            + ", col3    from tt /* comment */    where /*! id= */3 # comment";
        Assert.assertEquals("comment:-- comment\n"
            + "comment:# hehe\n"
            + "comment:/* hahaha select 123, drds */\n"
            + "comment:/*+TDDL: scan()*/\n"
            + "KW_SELECT:select\n"
            + "IDENTIFIER:col\n"
            + "PUNC_COMMA:\n"
            + "IDENTIFIER:count\n"
            + "PUNC_LEFT_PAREN:\n"
            + "OP_ASTERISK:\n"
            + "PUNC_RIGHT_PAREN:\n"
            + "PUNC_COMMA:\n"
            + "comment:/*+ HINT_NAME([argument_list]) */\n"
            + "IDENTIFIER:col1\n"
            + "IDENTIFIER:count\n"
            + "PUNC_LEFT_PAREN:\n"
            + "OP_ASTERISK:\n"
            + "PUNC_RIGHT_PAREN:\n"
            + "PUNC_COMMA:\n"
            + "IDENTIFIER:col2\n"
            + "comment:-- comment\n"
            + "PUNC_COMMA:\n"
            + "IDENTIFIER:col3\n"
            + "KW_FROM:from\n"
            + "IDENTIFIER:tt\n"
            + "comment:/* comment */\n"
            + "KW_WHERE:where\n"
            + "IDENTIFIER:id\n"
            + "OP_EQUALS:\n"
            + "LITERAL_NUM_PURE_DIGIT:3\n"
            + "comment:# comment\n", parse(sql));
    }

    @Test
    public void simple1Test() throws Exception {
        final String sql = "-- for update\n"
            + "# for update\n"
            + "/* for update */\n"
            + "/*+for update*/\n"
            + "select `for update`, /*+ for update */count('for update'), \"for update\"  -- for update\n"
            + ", col3 from `for update` /* for update */    where /*! id= */3 # for update";
        Assert.assertEquals("comment:-- for update\n"
            + "comment:# for update\n"
            + "comment:/* for update */\n"
            + "comment:/*+for update*/\n"
            + "KW_SELECT:select\n"
            + "IDENTIFIER:`for update`\n"
            + "PUNC_COMMA:\n"
            + "comment:/*+ for update */\n"
            + "IDENTIFIER:count\n"
            + "PUNC_LEFT_PAREN:\n"
            + "LITERAL_CHARS:'for update'\n"
            + "PUNC_RIGHT_PAREN:\n"
            + "PUNC_COMMA:\n"
            + "LITERAL_CHARS:\"for update\"\n"
            + "comment:-- for update\n"
            + "PUNC_COMMA:\n"
            + "IDENTIFIER:col3\n"
            + "KW_FROM:from\n"
            + "IDENTIFIER:`for update`\n"
            + "comment:/* for update */\n"
            + "KW_WHERE:where\n"
            + "IDENTIFIER:id\n"
            + "OP_EQUALS:\n"
            + "LITERAL_NUM_PURE_DIGIT:3\n"
            + "comment:# for update\n", parse(sql));
    }

    @Test
    public void testUserDefVar() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer("@abc  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@abc.d  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@abc.d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@abc_$.d");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@abc_$.d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@abc_$_.");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@abc_$_.", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@'\\''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@'\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@''''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@''''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@'\"'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@'\"'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@'\"\"'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@'\"\"'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@'\\\"'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@'\\\"'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@'ac\\''  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@'ac\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@'''ac\\''  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@'''ac\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@'abc'''ac\\''  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@'abc'''", sut.originalStringValue());

        sut = new MySQLLexer("@''abc''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@\"\"  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@\"\"\"abc\"  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@\"\"\"abc\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@\"\\\"\"\"abc\"  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@\"\\\"\"\"abc\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@\"\\\"\"  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@\"\\\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@\"\"\"\\\"d\"  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@\"\"\"\\\"d\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@\"'\"  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@\"'\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@`` ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@``", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@````");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@````", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@` `");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@` `", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@`abv```");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@`abv```", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@`````abc`");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@`````abc`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@`````abc```");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@`````abc```", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@``abc");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@``", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@`abc`````abc``");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@`abc`````", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("``", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(" -- \n  @  #abc\n\r\t\"\"@\"abc\\\\''-- abc\n'''\\\"\"\"\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@\"abc\\\\''-- abc\n'''\\\"\"\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("/**/@a #@abc\n@.\r\t");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@a", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@.", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("  #@abc\n@\"1a_-@#!''`=\\a\"-- @\r\n@'-_1a/**/\\\"\\''/*@abc*/@`_1@\\''\"`");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@\"1a_-@#!''`=\\a\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@'-_1a/**/\\\"\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@`_1@\\''\"`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("  /*! */@._a$ @_a.b$c.\r@1_a.$#\n@A.a_/@-- \n@_--@.[]'\"@#abc'@a,@;@~#@abc");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@._a$", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@_a.b$c.", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@1_a.$", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@A.a_", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_SLASH, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@_", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_MINUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_MINUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@.", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_LEFT_BRACKET, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_RIGHT_BRACKET, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'\"@#abc'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@a", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_COMMA, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_SEMICOLON, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_TILDE, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testSystemVar() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer("@@abc  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@@`abc`  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("`abc`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@@```abc`  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("```abc`", sut.originalStringValue());

        sut = new MySQLLexer("@@``  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("``", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@@`a```  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("`a```", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@@````  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("````", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@@`~!````@#$%^&*()``_+=-1{}[]\";:'<>,./?|\\`  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("`~!````@#$%^&*()``_+=-1{}[]\";:'<>,./?|\\`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@@global.var1  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("global", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("var1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@@'abc'  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'abc'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("@@\"abc\"  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"abc\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(
            "@@.  /*@@abc*/@@`abc''\"\\@@!%*&+_abcQ`//@@_1.  @@$#\n@@$var.-- @@a\t\n@@system_var:@@a`b`?");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("`abc''\"\\@@!%*&+_abcQ`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_SLASH, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_SLASH, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("_1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("$", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("$var", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("system_var", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_COLON, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("a", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("`b`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.QUESTION_MARK, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testPlaceHolder() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer(" ${abc}. ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PLACE_HOLDER, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(" ${abc");
        try {
            sut.nextToken();
            Assert.fail("should throw");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals("unclosed placeholder", e.getMessage());
        }

        sut = new MySQLLexer(" ${abc}");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PLACE_HOLDER, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(" ${abc}abn");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PLACE_HOLDER, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("abn", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(" ${abc12@,,.~`*-_$}}}}");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PLACE_HOLDER, sut.token());
        Assert.assertEquals("abc12@,,.~`*-_$", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_RIGHT_BRACE, sut.token());
        sut.nextToken();
        sut.nextToken();
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(" #${abc\n,${abc12@,,.~`*-_$}");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_COMMA, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PLACE_HOLDER, sut.token());
        Assert.assertEquals("abc12@,,.~`*-_$", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("${abc(123,345)} ,");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PLACE_HOLDER, sut.token());
        Assert.assertEquals("abc(123,345)", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_COMMA, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testId1() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer("id . 12e3f /***/`12\\3```-- d\n \r#\r  ##\n\t123d");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e3f", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("`12\\3```", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("123d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("`ab``c`");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("`ab``c`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("`,\"'\\//*$#\nab``c  -`");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("`,\"'\\//*$#\nab``c  -`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("`ab````c```");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("`ab````c```", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("`ab`````c``````");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("`ab`````", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("c", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("``````", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("n123 \t b123 x123");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("n123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("b123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("x123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("n邱 硕");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("n邱", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("硕", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("n邱硕");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("n邱硕", sut.originalStringValue());
        sut.nextToken();

        sut = new MySQLLexer(" $abc");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("$abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("$abc  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("$abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(" $abc  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("$abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(" 123d +=_&*_1a^abc-- $123");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("123d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_PLUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_EQUALS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("_", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_AMPERSAND, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_ASTERISK, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("_1a", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_CARET, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(" $abc  ,#$abc\n{`_``12`(123a)_abcnd; //x123");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("$abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_COMMA, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_LEFT_BRACE, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("`_``12`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_LEFT_PAREN, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("123a", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_RIGHT_PAREN, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("_abcnd", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_SEMICOLON, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_SLASH, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_SLASH, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("x123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testString() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer("''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("'''\\''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'''\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\'\'\'\'\'\\''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'''''\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("''''''/'abc\\''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("''''''", sut.originalStringValue());
        sut.nextToken();
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'abc\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\'abc\\\'\'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'abc\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("'\\\\\\\"\"\"'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'\\\\\\\"\"\"'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("'\'\''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("''''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("''''");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("''''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("'he\"\"\"llo'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'he\"\"\"llo'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("'he'\''\'llo'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'he''''llo'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("'\''hello'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'''hello'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"abc'\\d\"\"ef\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"abc'\\d\"\"ef\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"abc'\\\\\"\"\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"abc'\\\\\"\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"\\'\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"\\'\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"\"\"\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"\"\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"abc\" '\\'s'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"abc\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'\\'s'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"\"\"'\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"\"\"'\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"\\\"\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"\\\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"\\\\\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"\\\\\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\"   hello '''/**/#\n-- \n~=+\"\"\"\"\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"   hello '''/**/#\n"
            + "-- \n"
            + "~=+\"\"\"\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\r--\t\n\"abc\"");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"abc\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("N'ab\\'c'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NCHARS, sut.token());
        Assert.assertEquals("'ab\\'c'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("  \'abc\\\\\\'\' 'abc\\a\\'\''\"\"'/\"abc\\\"\".\"\"\"abc\"\"\"\"'\''\"n'ab\\'c'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'abc\\\\\\''", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'abc\\a\\'''\"\"'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_SLASH, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"abc\\\"\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("\"\"\"abc\"\"\"\"'''\"", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NCHARS, sut.token());
        Assert.assertEquals("'ab\\'c'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testHexBit() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer("0x123  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0x123");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0x123aDef");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("123aDef", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0x0");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("0", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0xABC");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("ABC", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0xA01aBC");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("A01aBC", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0x123re2  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("0x123re2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("x'123'e  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("x'123'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("x'102AaeF3'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("102AaeF3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0b10");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_BIT, sut.token());
        Assert.assertEquals("10", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0b101101");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_BIT, sut.token());
        Assert.assertEquals("101101", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0b103  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("0b103", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("b'10'b  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_BIT, sut.token());
        Assert.assertEquals("10", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("b", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("\r 0xabc.123;x'e'a0x1.3x'a2w'--\t0b11\n0b12*b '123' b'101'");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("abc", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals(".123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_SEMICOLON, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("a0x1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("3x", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'a2w'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("0b12", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_ASTERISK, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("b", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'123'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_BIT, sut.token());
        Assert.assertEquals("101", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testNumber() throws SQLSyntaxErrorException {
        MySQLLexer sut =
            new MySQLLexer(" . 12e3/***/.12e3#/**\n.123ee123.1--  \r\t\n.12e/*a*//* !*/.12e_a/12e-- \r\t.12e-1");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("12e3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals(".12e3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("123ee123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals(".1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e_a", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_SLASH, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".12e-1  ");
        sut.nextToken();
        Assert.assertEquals(".12e-1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12e000000000000000  ");
        sut.nextToken();
        Assert.assertEquals("12e000000000000000", sut.originalStringValue());

        sut = new MySQLLexer(".12e-  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_MINUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".12e-1d  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_MINUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("1d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12.e+1d  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("12.e+1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12.f ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("12.", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("f", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".12f ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12f", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("1.2f ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("1.2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("f", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        try {
            sut = new MySQLLexer("12.e ");
            sut.nextToken();
            Assert.assertFalse("should not reach here", true);
        } catch (SQLSyntaxErrorException e) {
        }

        sut = new MySQLLexer("0e  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("0e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12. e  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("12.", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12. e+1  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("12.", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_PLUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_PURE_DIGIT, sut.token());
        Assert.assertEquals("1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12.e+1  ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("12.e+1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12.");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("12.", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".12");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals(".12", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12e");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12ef");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12ef", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".12e");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("1.0e0");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("1.0e0", sut.originalStringValue());

        sut = new MySQLLexer("1.01e0,");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("1.01e0", sut.originalStringValue());

        sut = new MySQLLexer(".12e-");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_MINUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".12e-d");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("12e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_MINUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("123E2.*");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("123E2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_ASTERISK, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("1e-1  ");
        sut.nextToken();
        Assert.assertEquals("1e-1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".E5");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("E5", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0E5d");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("0E5d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("0E10");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("0E10", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".   ");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("12345678901234567890123 1234567890 1234567890123456789");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_PURE_DIGIT, sut.token());
        Assert.assertEquals("12345678901234567890123", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_PURE_DIGIT, sut.token());
        Assert.assertEquals("1234567890", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_PURE_DIGIT, sut.token());
        Assert.assertEquals("1234567890123456789", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer(".");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testSkipSeparator() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer("  /**//***/ \t\n\r\n -- \n#\n/*/*-- \n*/");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testCStyleComment() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer("id1 /*!id2 */ id3");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*! id2 */ id3");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!*/ id3");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40001id2*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000id2*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!4000id2*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("4000id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!400001id2*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("1id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!400011id2*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!4000*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_PURE_DIGIT, sut.token());
        Assert.assertEquals("4000", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40001*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!400001*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_PURE_DIGIT, sut.token());
        Assert.assertEquals("1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000 -- id2\n*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000 /* id2*/*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000/* id2*/*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000id2*/* id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_ASTERISK, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000/*/*/id2*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40001/*/*/id2*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000/*/*/id2*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000/*/*/id2*/ /*!40000 id4*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id4", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40001/*/*/id2*/ /*!40000 id4*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id4", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000/*/*/id2*/ /*!40001 id4*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000/*/*/id2*//*!40001 id4*//*!40001 id5*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());

        sut = new MySQLLexer("id1 /*!40000/*/*/id2*//*!40001 id4*//*!40000id5*/ id3", 40000);
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id2", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id5", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("id3", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }

    @Test
    public void testLexer() throws SQLSyntaxErrorException {
        MySQLLexer sut = new MySQLLexer(" @a.1_$ .1e+1a%x'a1e'*0b11a \r#\"\"\n@@`123`@@'abc'1.e-1d`/`1.1e1.1e1");
        sut.nextToken();
        Assert.assertEquals(MySQLToken.USR_VAR, sut.token());
        Assert.assertEquals("@a.1_$", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.PUNC_DOT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("1e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_PLUS, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("1a", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_PERCENT, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_HEX, sut.token());
        Assert.assertEquals("a1e", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.OP_ASTERISK, sut.token());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("0b11a", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("`123`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.SYS_VAR, sut.token());
        Assert.assertEquals("", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_CHARS, sut.token());
        Assert.assertEquals("'abc'", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("1.e-1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("d", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.IDENTIFIER, sut.token());
        Assert.assertEquals("`/`", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals("1.1e1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.LITERAL_NUM_MIX_DIGIT, sut.token());
        Assert.assertEquals(".1e1", sut.originalStringValue());
        sut.nextToken();
        Assert.assertEquals(MySQLToken.EOF, sut.token());
    }
}
