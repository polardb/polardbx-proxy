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

package com.alibaba.polardbx.proxy.callback;

import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.BytesTools;
import com.alibaba.polardbx.proxy.utils.CaseInsensitiveString;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PostGatherCallback extends ResultCallbackBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGatherCallback.class);

    public PostGatherCallback(@NotNull FrontendConnection frontend, @NotNull FrontendContext context,
                              @NotNull Scheduler scheduler) {
        super(frontend, context, scheduler);
    }

    @Override
    public void onStateChangeWithinLock(ResultHandler handler, ResultState before, ResultState state) {
        assert handler instanceof QueryResultHandler;
        // record trx state change in lock to prevent any reorder outside the lock
        if (state.isDone()) {
            final QueryResultHandler nowHandler = (QueryResultHandler) handler;
            if (nowHandler.hasMore() != null) {
                assert !state.isAbort();
                return; // ignore more result, keep the trx reference(skip super invoke)
            }

            // gather information
            if (!state.isError() && !state.isAbort()) {
                try {
                    final BackendContext backendContext = handler.getContextReference().getAcquire();
                    assert backendContext != null;
                    byte[][] row;
                    while ((row = nowHandler.next(0, TimeUnit.MILLISECONDS)) != null) {
                        for (int col = 0; col < row.length; ++col) {
                            final ColumnDefinition41 def = nowHandler.getFields().get(col);
                            final String colName = context.decodeStringResults(def.getName());
                            final String colData;
                            final ColumnDefinition41.SimpleType simpleType = def.simpleType();
                            if (simpleType != null) {
                                switch (simpleType) {
                                case STRING:
                                    if (null == row[col]) {
                                        colData = null;
                                    } else if (0 == row[col].length) {
                                        colData = "''";
                                    } else {
                                        colData = '\'' + new String(row[col],
                                            CharsetMapping.getStaticJavaEncodingForCollationIndex(
                                                def.getCharacterSet())).replaceAll("'", "''") + '\'';
                                    }
                                    break;
                                case BYTES:
                                    colData = null == row[col] ? null : "x'" + BytesTools.bytes2Hex(row[col]) + '\'';
                                    break;
                                case RAW_STRING:
                                    colData = new String(row[col],
                                        CharsetMapping.getStaticJavaEncodingForCollationIndex(def.getCharacterSet()));
                                    break;
                                default:
                                    colData = null;
                                    break;
                                }
                            } else {
                                colData = null;
                            }
                            if (colName != null) {
                                if (colName.startsWith("@@")) {
                                    // sys var
                                    final CaseInsensitiveString pureName =
                                        new CaseInsensitiveString(colName.substring(2));
                                    final String nonNullData = null == colData ? "null" : colData;
                                    context.getSystemVariables().put(pureName, nonNullData);
                                    backendContext.getSystemVariables().put(pureName, nonNullData);
                                    if (pureName.equals("sql_mode")) {
                                        context.setSqlMode(nonNullData);
                                        backendContext.setSqlMode(nonNullData);
                                    }
                                    // todo record charset changes
                                } else {
                                    // user var
                                    assert colName.startsWith("@");
                                    final CaseInsensitiveString pureName =
                                        new CaseInsensitiveString(colName.substring(1));
                                    // force compare object reference
                                    if (colData != null) {
                                        context.getUserVariables().put(pureName, colData);
                                        backendContext.getUserVariables().put(pureName, colData);
                                    } else {
                                        context.getUserVariables().remove(pureName);
                                        backendContext.getUserVariables().remove(pureName);
                                    }
                                }
                            }
                        }
                    }
                } catch (Throwable t) {
                    LOGGER.error("error when gather result", t);
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Now usr vars: [{}] Now sys vars: [{}]",
                        context.getUserVariables().entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
                            .collect(Collectors.joining(",")),
                        context.getSystemVariables().entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
                            .collect(Collectors.joining(",")));
                }
            }

            // todo dealing abort when gather data
            // todo dealing missing warning when previous sql may raise warning
        }

        // finish trx deference
        super.onStateChangeWithinLock(handler, before, state);
    }
}
