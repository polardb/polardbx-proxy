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
package com.alibaba.polardbx.proxy.parser.ast.stmt.dal;

import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * Created by simiao.zw on 2014/7/31.
 */
public class DALPrepareStatement implements SQLStatement {

    private String stmt_id;
    private String stmt_define;
    private MySQLToken stmt_token;

    /**
     * stmt_define should be set later
     */
    public DALPrepareStatement(String stmt_id) {
        this.stmt_id = stmt_id;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public String getStmt_id() {
        return stmt_id;
    }

    public void setStmt_id(String stmt_id) {
        this.stmt_id = stmt_id;
    }

    public String getStmt_define() {
        return stmt_define;
    }

    public void setStmt_define(String stmt_define) {
        this.stmt_define = stmt_define;
    }

    public MySQLToken getStmt_token() {
        return stmt_token;
    }

    public void setStmt_token(MySQLToken stmt_token) {
        this.stmt_token = stmt_token;
    }
}
