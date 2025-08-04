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

package com.alibaba.polardbx.proxy.scheduler;

import com.alibaba.polardbx.proxy.callback.PostGatherCallback;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.SysVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.UsrDefVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.VariableExpression;
import com.alibaba.polardbx.proxy.parser.ast.fragment.VariableScope;
import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetCharacterSetStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetNamesStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetStatement;
import com.alibaba.polardbx.proxy.parser.recognizer.SQLParser;
import com.alibaba.polardbx.proxy.parser.util.Pair;
import com.alibaba.polardbx.proxy.protocol.command.ComQuery;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtExecute;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VariablesPostGatherTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(VariablesPostGatherTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        if (scheduler.getPostOperationSql() != null || scheduler.getPostOperationCallback() != null) {
            throw new IllegalStateException("Post operation is not null when build variables post gather.");
        }
        final MysqlPacket request = scheduler.getRequest();
        if (null == request) {
            throw new IllegalStateException("Request is null when build variables post gather.");
        }

        final FrontendContext context = scheduler.getContext();
        final Set<String> userVariables = new HashSet<>(), systemVariables = new HashSet<>();

        try {
            final SQLParser parser;
            if (request instanceof ComQuery) {
                final byte[] query = ((ComQuery) request).getQuery();
                parser = new SQLParser(query, 0, query.length, context.getClientJavaCharset(), context.getSqlMode(),
                    HaManager.getInstance().getVersion());
            } else if (request instanceof ComStmtExecute) {
                final Charset defaultCharset = Charset.defaultCharset();
                final byte[] bytes = scheduler.getPreparedStatement().getPrepareSql().getBytes(defaultCharset);
                parser = new SQLParser(bytes, 0, bytes.length, defaultCharset, context.getSqlMode(),
                    HaManager.getInstance().getVersion());
            } else {
                return null;
            }

            // gather all variables which may changed with post query
            final List<SQLStatement> statements = parser.parseMultiStatements();
            for (SQLStatement stmt : statements) {
                if (stmt instanceof DALSetStatement) {
                    // SET variable = expr [, variable = expr] ...
                    //
                    // variable: {
                    //     user_var_name
                    //   | param_name
                    //   | local_var_name
                    //   | {GLOBAL | @@GLOBAL.} system_var_name
                    //   | {PERSIST | @@PERSIST.} system_var_name
                    //   | {PERSIST_ONLY | @@PERSIST_ONLY.} system_var_name
                    //   | [SESSION | @@SESSION. | @@] system_var_name
                    // }
                    final DALSetStatement dalSet = (DALSetStatement) stmt;
                    for (final Pair<VariableExpression, Expression> assignment : dalSet.getAssignmentList()) {
                        if (assignment.getKey() instanceof UsrDefVarPrimary) {
                            final UsrDefVarPrimary key = (UsrDefVarPrimary) assignment.getKey();
                            userVariables.add(key.getVarText());
                        } else {
                            assert assignment.getKey() instanceof SysVarPrimary;
                            final SysVarPrimary key = (SysVarPrimary) assignment.getKey();
                            if (VariableScope.GLOBAL == key.getScope()) {
                                continue; // ignore set global
                            }
                            final String varName = key.getVarText();
                            if (varName.equalsIgnoreCase("autocommit")) {
                                continue; // ignore autocommit(maintained via server state)
                            }
                            systemVariables.add(varName);
                        }
                    }
                } else if (stmt instanceof DALSetCharacterSetStatement) {
                    // SET {CHARACTER SET | CHARSET} {'charset_name' | DEFAULT}
                    systemVariables.add("character_set_client");
                    systemVariables.add("character_set_results");
                    systemVariables.add("character_set_connection");
                } else if (stmt instanceof DALSetNamesStatement) {
                    // SET NAMES {'charset_name' [COLLATE 'collation_name'] | DEFAULT}
                    systemVariables.add("character_set_client");
                    systemVariables.add("character_set_results");
                    systemVariables.add("character_set_connection");
                    systemVariables.add("collation_connection");
                }
            }
        } catch (Throwable t) {
            LOGGER.error("error when parse for post gather", t);
            return null;
        }

        if (!userVariables.isEmpty() || !systemVariables.isEmpty()) {
            // generate for gather
            final StringBuilder sql = new StringBuilder();
            sql.append("/* PolarDB-X-Proxy PostFetcher */ select ");
            boolean first = true;
            for (final String var : userVariables) {
                if (first) {
                    first = false;
                } else {
                    sql.append(",");
                }
                sql.append(var);
            }
            for (final String var : systemVariables) {
                if (first) {
                    first = false;
                } else {
                    sql.append(",");
                }
                sql.append("@@").append(var);
            }

            scheduler.setPostOperationSql(sql.toString());
            scheduler.setPostOperationCallback(new PostGatherCallback(scheduler.getFrontend(), context, scheduler));
        }
        return null;
    }
}
