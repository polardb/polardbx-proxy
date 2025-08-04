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

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.SysVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.UsrDefVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.polardbx.proxy.parser.ast.fragment.VariableScope;
import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetCharacterSetStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetNamesStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetStatement;
import com.alibaba.polardbx.proxy.parser.recognizer.SQLParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SetStmtTest {
    @Test
    public void setVarTest() throws Exception {
        String sql = "set @a=1";
        SQLParser parser = new SQLParser(sql);
        List<SQLStatement> stmts = parser.parseMultiStatements();
        Assert.assertEquals(1, stmts.size());
        Assert.assertEquals("DALSetStatement", stmts.get(0).getClass().getSimpleName());
        DALSetStatement stmt = (DALSetStatement) stmts.get(0);
        Assert.assertEquals(1, stmt.getAssignmentList().size());
        Assert.assertEquals("UsrDefVarPrimary", stmt.getAssignmentList().get(0).getKey().getClass().getSimpleName());
        Assert.assertEquals("LiteralNumber", stmt.getAssignmentList().get(0).getValue().getClass().getSimpleName());
        Assert.assertEquals("@a", ((UsrDefVarPrimary) stmt.getAssignmentList().get(0).getKey()).getVarText());
        Assert.assertEquals(1, ((LiteralNumber) stmt.getAssignmentList().get(0).getValue()).getNumber());

        sql = "set @@a=1";
        parser = new SQLParser(sql);
        stmts = parser.parseMultiStatements();
        Assert.assertEquals(1, stmts.size());
        Assert.assertEquals("DALSetStatement", stmts.get(0).getClass().getSimpleName());
        stmt = (DALSetStatement) stmts.get(0);
        Assert.assertEquals(1, stmt.getAssignmentList().size());
        Assert.assertEquals("SysVarPrimary", stmt.getAssignmentList().get(0).getKey().getClass().getSimpleName());
        Assert.assertEquals("LiteralNumber", stmt.getAssignmentList().get(0).getValue().getClass().getSimpleName());
        Assert.assertEquals("a", ((SysVarPrimary) stmt.getAssignmentList().get(0).getKey()).getVarText());
        Assert.assertEquals(VariableScope.SESSION,
            ((SysVarPrimary) stmt.getAssignmentList().get(0).getKey()).getScope());
        Assert.assertEquals(1, ((LiteralNumber) stmt.getAssignmentList().get(0).getValue()).getNumber());
    }

    @Test
    public void setCharset() throws Exception {
        String sql = "set character set 'utf8'";
        SQLParser parser = new SQLParser(sql);
        List<SQLStatement> stmts = parser.parseMultiStatements();
        Assert.assertEquals(1, stmts.size());
        Assert.assertEquals("DALSetCharacterSetStatement", stmts.get(0).getClass().getSimpleName());
        DALSetCharacterSetStatement stmt = (DALSetCharacterSetStatement) stmts.get(0);
        Assert.assertEquals("utf8", stmt.getCharset());

        sql = "set charset default";
        parser = new SQLParser(sql);
        stmts = parser.parseMultiStatements();
        Assert.assertEquals(1, stmts.size());
        Assert.assertEquals("DALSetCharacterSetStatement", stmts.get(0).getClass().getSimpleName());
        stmt = (DALSetCharacterSetStatement) stmts.get(0);
        Assert.assertTrue(stmt.isDefault());
    }

    @Test
    public void setNamesTest() throws Exception {
        String sql = "set names utf8";
        SQLParser parser = new SQLParser(sql);
        List<SQLStatement> stmts = parser.parseMultiStatements();
        Assert.assertEquals(1, stmts.size());
        Assert.assertEquals("DALSetNamesStatement", stmts.get(0).getClass().getSimpleName());
        DALSetNamesStatement stmt = (DALSetNamesStatement) stmts.get(0);
        Assert.assertEquals("utf8", stmt.getCharsetName());
        Assert.assertNull(stmt.getCollationName());

        sql = "set names 'utf8' COLLATE 'utf8mb4'";
        parser = new SQLParser(sql);
        stmts = parser.parseMultiStatements();
        Assert.assertEquals(1, stmts.size());
        stmt = (DALSetNamesStatement) stmts.get(0);
        Assert.assertEquals("utf8", stmt.getCharsetName());
        Assert.assertEquals("utf8mb4", stmt.getCollationName());

        sql = "set names default";
        parser = new SQLParser(sql);
        stmts = parser.parseMultiStatements();
        Assert.assertEquals(1, stmts.size());
        stmt = (DALSetNamesStatement) stmts.get(0);
        Assert.assertNull(stmt.getCharsetName());
        Assert.assertNull(stmt.getCollationName());
        Assert.assertTrue(stmt.isDefault());
    }
}
