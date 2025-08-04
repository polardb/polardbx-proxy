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

import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.utils.CaseInsensitiveString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class VariablesRestoreTask implements ScheduleTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(VariablesRestoreTask.class);

    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        final BackendConnectionWrapper backend = scheduler.getBackend();
        if (null == backend) {
            throw new IllegalStateException("Backend is null when restore variables.");
        }

        final StringBuilder builder = new StringBuilder();
        final FrontendContext context = scheduler.getContext();
        final BackendContext backendContext = backend.getContextReference().getAcquire();

        // first dealing user variables(remove then add)
        if (backendContext != null) {
            for (final Map.Entry<CaseInsensitiveString, String> entry : backendContext.getUserVariables().entrySet()) {
                final CaseInsensitiveString varName = entry.getKey();
                if (!context.getUserVariables().containsKey(varName)) {
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    builder.append('@').append(varName).append("=null");
                }
            }
        }
        for (final Map.Entry<CaseInsensitiveString, String> entry : context.getUserVariables().entrySet()) {
            final CaseInsensitiveString varName = entry.getKey();
            final String varValue = entry.getValue();
            if (null == backendContext || !Objects.equals(backendContext.getUserVariables().get(varName), varValue)) {
                if (builder.length() > 0) {
                    builder.append(',');
                }
                builder.append('@').append(varName).append('=').append(varValue);
            }
        }

        // then deal with system variables(remove then add)
        if (backendContext != null) {
            for (final Map.Entry<CaseInsensitiveString, String> entry : backendContext.getSystemVariables()
                .entrySet()) {
                final CaseInsensitiveString varName = entry.getKey();
                if (!context.getSystemVariables().containsKey(varName)) {
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    final Map<CaseInsensitiveString, String> globalVariables = backendContext.getGlobalVariables();
                    builder.append("@@").append(varName).append('=')
                        .append(null == globalVariables ? "null" : globalVariables.get(varName));
                }
            }
        }
        for (final Map.Entry<CaseInsensitiveString, String> entry : context.getSystemVariables().entrySet()) {
            final CaseInsensitiveString varName = entry.getKey();
            final String varValue = entry.getValue();
            if (null == backendContext || !Objects.equals(backendContext.getSystemVariables().get(varName), varValue)) {
                if (builder.length() > 0) {
                    builder.append(",");
                }
                builder.append("@@").append(varName).append('=').append(varValue);
            }
        }

        if (builder.length() > 0) {
            // need send SET statement
            final String sql = "SET " + builder;
            LOGGER.debug("send variables restore sql: {}", sql);
            backend.sendQuery(sql, context.getClientJavaCharset(),
                context.hasCapability(Capabilities.CLIENT_QUERY_ATTRIBUTES), (h, b, s) -> {
                    if (s.isDone()) {
                        if (null == h) {
                            throw new RuntimeException("Failed to set variables with early abort.");
                        }
                        final BackendContext c = h.getContextReference().getAcquire();
                        assert c != null;
                        if (s.isOK()) {
                            // set all backend context variables
                            c.getUserVariables().putAll(context.getUserVariables());
                            c.getUserVariables().keySet().removeIf(key -> !context.getUserVariables().containsKey(key));
                            c.getSystemVariables().putAll(context.getSystemVariables());
                            c.getSystemVariables().keySet()
                                .removeIf(key -> !context.getSystemVariables().containsKey(key));
                        } else {
                            final String err = s.isError() ?
                                c.decodeStringResults(((QueryResultHandler) h).getErr().getErrorMessage()) : s.name();
                            throw new RuntimeException(
                                this + ". Restore variables failed, sql: " + sql + ", error: " + (null == err ?
                                    "unknown" : err) + '.');
                        }
                    }
                });
        }
        return null;
    }
}
