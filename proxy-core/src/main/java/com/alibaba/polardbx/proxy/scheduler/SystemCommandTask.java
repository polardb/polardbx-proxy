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

import com.alibaba.polardbx.proxy.cluster.GlobalMock;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.SysVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.VariableExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.polardbx.proxy.parser.ast.fragment.VariableScope;
import com.alibaba.polardbx.proxy.parser.ast.stmt.SQLStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.Kill;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowBackend;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowCluster;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowFrontend;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowProperties;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowRO;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowRW;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowReactor;
import com.alibaba.polardbx.proxy.parser.recognizer.SQLParser;
import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.parser.util.Pair;
import com.alibaba.polardbx.proxy.protocol.command.ComQuery;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.protocol.handler.request.ShowBackendHandler;
import com.alibaba.polardbx.proxy.protocol.handler.request.ShowClusterHandler;
import com.alibaba.polardbx.proxy.protocol.handler.request.ShowFrontendHandler;
import com.alibaba.polardbx.proxy.protocol.handler.request.ShowPropertiesHandler;
import com.alibaba.polardbx.proxy.protocol.handler.request.ShowReactorHandler;
import com.alibaba.polardbx.proxy.protocol.handler.request.ShowRoHandler;
import com.alibaba.polardbx.proxy.protocol.handler.request.ShowRwHandler;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import com.alibaba.polardbx.proxy.sync.SyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SystemCommandTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemCommandTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        // try to decode and run system command
        if (null == scheduler.getEncoder()) {
            // ignore if reschedule
            return null;
        }
        if (!(scheduler.getRequest() instanceof ComQuery)) {
            // only support system command via ComQuery
            return null;
        }

        final ComQuery comQuery = (ComQuery) scheduler.getRequest();
        final FrontendContext context = scheduler.getContext();
        final List<SQLStatement> statements;
        try {
            final byte[] query = comQuery.getQuery();
            final SQLParser parser =
                new SQLParser(query, 0, query.length, context.getClientJavaCharset(), context.getSqlMode(),
                    HaManager.getInstance().getVersion());

            // only care about show/kill/set, and only deal with single statement
            final MySQLToken firstToken = parser.getFirstToken();
            if (firstToken != MySQLToken.KW_SHOW && firstToken != MySQLToken.KW_KILL
                && firstToken != MySQLToken.KW_SET) {
                return null;
            }

            statements = parser.parseMultiStatements();
        } catch (Throwable t) {
            LOGGER.error("error when parse for system views", t);
            return null;
        }

        final Encoder encoder = scheduler.getEncoder();
        if (1 == statements.size() && statements.get(0) != null) {
            final SQLStatement statement = statements.get(0);
            if (statement instanceof ShowCluster) {
                try (final ShowClusterHandler handler = new ShowClusterHandler(context)) {
                    return handler.handleAndTakePacket(null, null, encoder);
                }
            } else if (statement instanceof ShowRO) {
                try (final ShowRoHandler handler = new ShowRoHandler(context)) {
                    return handler.handleAndTakePacket(null, null, encoder);
                }
            } else if (statement instanceof ShowRW) {
                try (final ShowRwHandler handler = new ShowRwHandler(context)) {
                    return handler.handleAndTakePacket(null, null, encoder);
                }
            } else if (statement instanceof ShowProperties) {
                try (final ShowPropertiesHandler handler = new ShowPropertiesHandler(context)) {
                    return handler.handleAndTakePacket(null, null, encoder);
                }
            } else if (statement instanceof ShowReactor) {
                try (final ShowReactorHandler handler = new ShowReactorHandler(context)) {
                    return handler.handleAndTakePacket(null, null, encoder);
                }
            } else if (statement instanceof ShowFrontend) {
                try (final ShowFrontendHandler handler = new ShowFrontendHandler(context,
                    ((ShowFrontend) statement).isFull())) {
                    return handler.handleAndTakePacket(null, null, encoder);
                }
            } else if (statement instanceof ShowBackend) {
                try (final ShowBackendHandler handler = new ShowBackendHandler(context,
                    ((ShowBackend) statement).isFull())) {
                    return handler.handleAndTakePacket(null, null, encoder);
                }
            } else if (statement instanceof Kill) {
                final Kill kill = (Kill) statement;
                SyncService.kill(kill.getProcessId().getNumber().intValue(), kill.isConnection());
                context.sendOk(encoder, false);
                return false; // not taken
            } else if (statement instanceof DALSetStatement) {
                final DALSetStatement set = (DALSetStatement) statement;
                boolean hasUnknown = false;
                for (Pair<VariableExpression, Expression> assignment : set.getAssignmentList()) {
                    final VariableExpression variable = assignment.getKey();
                    final Expression value = assignment.getValue();
                    if (variable instanceof SysVarPrimary && ((SysVarPrimary) variable).getVarTextUp()
                        .equals("MOCK") && value instanceof LiteralString) {
                        final String mock = ((LiteralString) value).getString().trim();
                        if (((SysVarPrimary) variable).getScope() == VariableScope.SESSION) {
                            context.setMock(mock.isEmpty() ? null : mock);
                            LOGGER.info("set session mock to '{}'", mock);
                        } else {
                            GlobalMock.MOCK = mock.isEmpty() ? null : mock;
                            LOGGER.info("set global mock to '{}'", mock);
                        }
                    } else {
                        hasUnknown = true;
                    }
                }
                if (!hasUnknown) {
                    context.sendOk(encoder, false);
                    return false; // not taken
                }
            }
        }
        return null;
    }
}
