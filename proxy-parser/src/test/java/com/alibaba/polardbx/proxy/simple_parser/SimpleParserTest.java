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

package com.alibaba.polardbx.proxy.simple_parser;

import org.junit.Assert;
import org.junit.Test;

public class SimpleParserTest {
    @Test
    public void selectTest() {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "\t\t\tselect\n"
            + "col,  count(*),   /*+ HINT_NAME([argument_list]) */       /*!1234 col1 */  count(*),  col2  -- comment\n"
            + ", col3    from tt /* comment */    where /*! id= */3 # comment";
        final SimpleParser parser = new SimpleParser(sql, "");
        Assert.assertTrue(parser.isSelect());
        Assert.assertEquals("\n"
                + "\n"
                + "\t\t\tselect\n"
                + "col,  count(*),          /*!1234 col1 */  count(*),  col2  , col3    from tt     where /*! id= */3 ",
            parser.getSanitizedSql());
        Assert.assertTrue(parser.canSlaveRead());
        Assert.assertFalse(parser.isSet());
    }

    @Test
    public void notSelectTest() {
        final String sql = "-- select 1;\n"
            + "# select 1\n"
            + "/* select 1*/\n"
            + "/*+ select 1*/\n"
            + "insert `select 1` (`select 1`) values ('select 1'), (\"select 1\")";
        final SimpleParser parser = new SimpleParser(sql, "");
        Assert.assertFalse(parser.isSelect());
        Assert.assertEquals("\n"
            + "\n"
            + "insert `select 1` (`select 1`) values ('select 1'), (\"select 1\")", parser.getSanitizedSql());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertFalse(parser.isSet());
    }

    @Test
    public void selectWithForUpdateTest() {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "select\n"
            + "col,  count(*),   /*+ HINT_NAME([argument_list]) */       /*!1234 col1 */  count(*),  col2  -- comment\n"
            + ", col3    from tt /* comment */    where /*! id= */3 # comment\n"
            + "\tfor     \t\n"
            + "   \tupdate";
        final SimpleParser parser = new SimpleParser(sql, "");
        Assert.assertTrue(parser.isSelect());
        Assert.assertEquals("\n"
            + "\n"
            + "select\n"
            + "col,  count(*),          /*!1234 col1 */  count(*),  col2  , col3    from tt     where /*! id= */3 \tfor     \t\n"
            + "   \tupdate", parser.getSanitizedSql());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertFalse(parser.isSet());
    }

    @Test
    public void selectWithLockInShareModeTest() {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "select\n"
            + "col,  count(*),   /*+ HINT_NAME([argument_list]) */       /*!1234 col1 */  count(*),  col2  -- comment\n"
            + ", col3    from tt /* comment */    where /*! id= */3 # comment\n"
            + "\tlock  /*+ HINT_NAME([argument_list]) */ in  \t\n"
            + "   \tshare/* comment */mode";
        final SimpleParser parser = new SimpleParser(sql, "");
        Assert.assertTrue(parser.isSelect());
        Assert.assertEquals("\n"
            + "\n"
            + "select\n"
            + "col,  count(*),          /*!1234 col1 */  count(*),  col2  , col3    from tt     where /*! id= */3 \tlock   in  \t\n"
            + "   \tshare mode", parser.getSanitizedSql());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertFalse(parser.isSet());
    }

    @Test
    public void selectNotLockTest() {
        final String sql = "-- for update\n"
            + "# for update\n"
            + "/* for update */\n"
            + "/*+for update*/\n"
            + "select `for update`, /*+ for update */count('for update'), \"for update\"  -- for update\n"
            + ", col3 from `for update` /* for update */    where /*! id= */3 # for update";
        final SimpleParser parser = new SimpleParser(sql, "");
        Assert.assertTrue(parser.isSelect());
        Assert.assertEquals("\n"
                + "\n"
                + "select `for update`, count('for update'), \"for update\"  , col3 from `for update`     where /*! id= */3 ",
            parser.getSanitizedSql());
        Assert.assertTrue(parser.canSlaveRead());
        Assert.assertFalse(parser.isSet());
    }

    @Test
    public void setTest() {
        final String sql = "-- comment\n"
            + "# hehe\n"
            + "/* hahaha select 123, drds */\n"
            + "/*+TDDL: scan()*/\n"
            + "\t\t\tset\n"
            + "a\t= /*+ HINT_NAME([argument_list]) */       /*!1234 col1 */ 1  -- comment\n";
        final SimpleParser parser = new SimpleParser(sql, "");
        Assert.assertFalse(parser.isSelect());
        Assert.assertEquals("\n"
            + "\n"
            + "\t\t\tset\na\t=        /*!1234 col1 */ 1  ", parser.getSanitizedSql());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertTrue(parser.isSet());
    }

    @Test
    public void notSetTest() {
        final String sql = "-- set a=1;\n"
            + "# set a=1\n"
            + "/* set a=1*/\n"
            + "/*+ set a=1*/\n"
            + "insert `set a=1` (`set a=1`) values ('set a=1'), (\"set a=1\")";
        final SimpleParser parser = new SimpleParser(sql, "");
        Assert.assertFalse(parser.isSelect());
        Assert.assertEquals("\n"
            + "\n"
            + "insert `set a=1` (`set a=1`) values ('set a=1'), (\"set a=1\")", parser.getSanitizedSql());
        Assert.assertFalse(parser.canSlaveRead());
        Assert.assertFalse(parser.isSet());
    }
}
