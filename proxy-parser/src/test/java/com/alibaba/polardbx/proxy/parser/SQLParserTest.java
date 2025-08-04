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

import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.recognizer.SQLParser;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SQLParserTest {
    @Test
    public void selectTest() throws Exception {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "\t\t\tselect\n"
            + "col,  count(*),   /*+ HINT_NAME([argument_list]) */       /*!1234 col1 */  count(*),  col2  -- comment\n"
            + ", col3    from tt /* comment */    where /*! id= */3 # comment";
        final SQLParser parser = new SQLParser(sql);
        Assert.assertSame(MySQLToken.KW_SELECT, parser.getFirstToken());
        Assert.assertTrue(parser.canSlaveRead());
        Assert.assertNotSame(MySQLToken.KW_SET, parser.getFirstToken());
    }

    @Test
    public void notSelectTest() throws Exception {
        final String sql = "-- select 1;\n"
            + "# select 1\n"
            + "/* select 1*/\n"
            + "/*+ select 1*/\n"
            + "insert `select 1` (`select 1`) values ('select 1'), (\"select 1\")";
        final SQLParser parser = new SQLParser(sql);
        Assert.assertNotEquals(MySQLToken.KW_SELECT, parser.getFirstToken());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertNotEquals(MySQLToken.KW_SET, parser.getFirstToken());
    }

    @Test
    public void selectWithForUpdateTest() throws Exception {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "select\n"
            + "col,  count(*),   /*+ HINT_NAME([argument_list]) */       /*!1234 col1 */  count(*),  col2  -- comment\n"
            + ", col3    from tt /* comment */    where /*! id= */3 # comment\n"
            + "\tfor     \t\n"
            + "   \tupdate";
        final SQLParser parser = new SQLParser(sql);
        Assert.assertEquals(MySQLToken.KW_SELECT, parser.getFirstToken());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertNotEquals(MySQLToken.KW_SET, parser.getFirstToken());
    }

    @Test
    public void selectWithLockInShareModeTest() throws Exception {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "select\n"
            + "col,  count(*),   /*+ HINT_NAME([argument_list]) */       /*!1234 col1 */  count(*),  col2  -- comment\n"
            + ", col3    from tt /* comment */    where /*! id= */3 # comment\n"
            + "\tlock  /*+ HINT_NAME([argument_list]) */ in  \t\n"
            + "   \tshare/* comment */mode";
        final SQLParser parser = new SQLParser(sql);
        Assert.assertEquals(MySQLToken.KW_SELECT, parser.getFirstToken());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertNotEquals(MySQLToken.KW_SET, parser.getFirstToken());
    }

    @Test
    public void selectNotLockTest() throws Exception {
        final String sql = "-- for update\n"
            + "# for update\n"
            + "/* for update */\n"
            + "/*+for update*/\n"
            + "select `for update`, /*+ for update */count('for update'), \"for update\"  -- for update\n"
            + ", col3 from `for update` /* for update */    where /*! id= */3 # for update";
        final SQLParser parser = new SQLParser(sql);
        Assert.assertEquals(MySQLToken.KW_SELECT, parser.getFirstToken());
        Assert.assertTrue(parser.canSlaveRead());
        Assert.assertNotEquals(MySQLToken.KW_SET, parser.getFirstToken());
    }

    @Test
    public void setTest() throws Exception {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "\t\t\tset\n"
            + "a\t= /*+ HINT_NAME([argument_list]) */       /*!1234 col1 */ 1  -- comment\n";
        final SQLParser parser = new SQLParser(sql);
        Assert.assertNotEquals(MySQLToken.KW_SELECT, parser.getFirstToken());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertEquals(MySQLToken.KW_SET, parser.getFirstToken());
    }

    @Test
    public void notSetTest() throws Exception {
        final String sql = "-- set a=1;\n"
            + "# set a=1\n"
            + "/* set a=1*/\n"
            + "/*+ set a=1*/\n"
            + "insert `set a=1` (`set a=1`) values ('set a=1'), (\"set a=1\")";
        final SQLParser parser = new SQLParser(sql);
        Assert.assertNotEquals(MySQLToken.KW_SELECT, parser.getFirstToken());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertNotEquals(MySQLToken.KW_SET, parser.getFirstToken());
    }

    @Test
    public void multiStatementTest() throws Exception {
        final String sql0 = "select 1;select 2";
        Assert.assertTrue(new SQLParser(sql0).isMultiStatement());
        final String sql1 = "select 1;";
        Assert.assertFalse(new SQLParser(sql1).isMultiStatement());
        final String sql2 = "select 1";
        Assert.assertFalse(new SQLParser(sql2).isMultiStatement());
    }

    @Test
    public void testParseMultiStatements() throws Exception {
        final String sql = "kill 1;select 2;";
        final SQLParser parser = new SQLParser(sql);
        final List<SQLStatement> sqlStatements = parser.parseMultiStatements();
        Assert.assertEquals(3, sqlStatements.size());
    }

    @Test
    public void testSlaveReadCheck() throws Exception {
        String sql = "select 1;select 2";
        SQLParser parser = new SQLParser(sql);
        Assert.assertTrue(parser.canSlaveRead());
        Assert.assertTrue(parser.isReadOnly());
        sql = "select 1;update tt set a=1";
        parser = new SQLParser(sql);
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertFalse(parser.isReadOnly());
        sql = "select 1;select 2 for update";
        parser = new SQLParser(sql);
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertTrue(parser.isReadOnly());
        sql = "select 1 lock in share mode;select 2";
        parser = new SQLParser(sql);
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertTrue(parser.isReadOnly());
    }

    @Test
    public void testPrivilege() throws Exception {
        String sql = "select 1;flush privileges;select 2";
        SQLParser parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());

        sql = "select 1;create database xxx;select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());
        sql = "select 1;create schema xxx;select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());
        sql = "select 1;create user 'newuser'@'localhost' IDENTIFIED BY 'password';select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());

        sql = "select 1;drop database xxx;select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());
        sql = "select 1;drop schema xxx;select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());
        sql = "select 1;drop user 'newuser'@'localhost';select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());

        sql = "select 1;ALTER USER 'jeffrey'@'localhost'\n"
            + "  IDENTIFIED BY 'new_password' PASSWORD EXPIRE;;select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());

        sql = "select 1;RENAME USER 'jeffrey'@'localhost' TO 'jeff'@'127.0.0.1';select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());

        sql = "select 1;SET PASSWORD FOR 'jeffrey'@'localhost' = 'auth_string';select 2";
        parser = new SQLParser(sql);
        Assert.assertTrue(parser.isPrivilegeDatabaseChanged());

        sql = "select 1;select 2";
        parser = new SQLParser(sql);
        Assert.assertFalse(parser.isPrivilegeDatabaseChanged());
    }

    @Test
    public void testIsDropDatabase() throws Exception {
        String sql = "drop database XXX";
        SQLParser parser = new SQLParser(sql);
        Assert.assertEquals("a", parser.applyDatabase("a", false));
        Assert.assertEquals("xxx", parser.applyDatabase("xxx", false));
        Assert.assertNull(parser.applyDatabase("xxx", true));

        sql = "drop schema `xxx`";
        parser = new SQLParser(sql);
        Assert.assertNull(parser.applyDatabase("xxx", false));

        sql = "drop database if exists xxx";
        parser = new SQLParser(sql);
        Assert.assertNull(parser.applyDatabase("xxx", false));

        sql = "drop schema if exists `xxx`";
        parser = new SQLParser(sql);
        Assert.assertNull(parser.applyDatabase("xxx", false));

        sql = "drop database `x``xx`";
        parser = new SQLParser(sql);
        Assert.assertNull(parser.applyDatabase("x`xx", false));

        // keyword
        sql = "drop database show";
        parser = new SQLParser(sql);
        Assert.assertNull(parser.applyDatabase("show", false));

        sql = "drop user";
        parser = new SQLParser(sql);
        Assert.assertEquals("a", parser.applyDatabase("a", false));

        // multi stmt
        sql = "drop database `x``xx`; select 1;";
        parser = new SQLParser(sql);
        Assert.assertNull(parser.applyDatabase("x`xx", false));

        // multi drops
        sql = "select 1; drop database a;drop database `x``xx`; select 1;";
        parser = new SQLParser(sql);
        Assert.assertNull(parser.applyDatabase("a", false));
        Assert.assertNull(parser.applyDatabase("x`xx", false));

        sql = "drop schema `xxx`; use `x``xx`; drop database a;";
        parser = new SQLParser(sql);
        Assert.assertEquals("x`xx", parser.applyDatabase("xxx", false));

        sql = "drop schema `xxx`; use `x``xx`; drop database a;";
        parser = new SQLParser(sql);
        Assert.assertNull(parser.applyDatabase("xxx", false, new boolean[] {true, false, true}));
    }
}
