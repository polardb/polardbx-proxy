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

package com.alibaba.polardbx.proxy.protocol.handler;

import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.protocol.command.Commands;
import com.alibaba.polardbx.proxy.protocol.common.MysqlError;
import com.alibaba.polardbx.proxy.protocol.common.MysqlProtocolHandler;
import com.alibaba.polardbx.proxy.protocol.common.MysqlServerState;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.scheduler.Pipelines;
import com.alibaba.polardbx.proxy.scheduler.ScheduleTask;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FrontendCommandHandler extends MysqlProtocolHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendCommandHandler.class);

    private final FrontendConnection connection;
    private final FrontendContext context;

    public FrontendCommandHandler(FrontendConnection connection, FrontendContext context) {
        setTag("FrontendCommandHandler");
        this.connection = connection;
        this.context = context;
    }

    private boolean setOption(Slice packet, Decoder decoder, Encoder encoder) throws IOException {
        decoder.skip();
        final int val = decoder.u16_s();
        if (0 == val) {
            LOGGER.debug("set option: CLIENT_MULTI_STATEMENTS");
            context.addCapability(Capabilities.CLIENT_MULTI_STATEMENTS);
        } else if (1 == val) {
            LOGGER.debug("clear option: CLIENT_MULTI_STATEMENTS");
            context.removeCapability(Capabilities.CLIENT_MULTI_STATEMENTS);
        } else {
            context.sendErr(encoder, MysqlError.ER_NOT_SUPPORTED_YET, MysqlError.GENERAL_STATE, "Unknown option value");
            return false;
        }
        context.sendOk(encoder, false);
        return false; // not taken
    }

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        throw new UnsupportedOperationException("Encoder must be provided when handling frontend command.");
    }

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder, Encoder encoder) throws IOException {
        final String tag;
        final ScheduleTask[] tasks;

        final byte peek = decoder.peek_s();
        switch (peek) {
        case Commands.COM_QUIT:
            context.setState(MysqlServerState.Closed);
            return false;

        case Commands.COM_INIT_DB:
            tag = "init db";
            tasks = Pipelines.COM_INIT_DB_TASKS;
            break;

        case Commands.COM_QUERY:
            tag = "query";
            tasks = Pipelines.COM_QUERY_TASKS;
            break;

        case Commands.COM_FIELD_LIST:
            tag = "field list";
            tasks = Pipelines.COM_FIELD_LIST_TASKS;
            break;

        case Commands.COM_STATISTICS:
            tag = "statistics";
            tasks = Pipelines.COM_STATISTICS_TASKS;
            break;

        case Commands.COM_DEBUG:
            tag = "debug";
            tasks = Pipelines.OK_ERR_MASTER_TASKS;
            break;

        case Commands.COM_PING:
            tag = "ping";
            tasks = Pipelines.OK_ERR_STALE_SLAVE_TASKS;
            break;

        case Commands.COM_RESET_CONNECTION:
            // todo dealing reset and set all variables
            tag = "reset";
            tasks = Pipelines.OK_ERR_STALE_SLAVE_TASKS;
            break;

        case Commands.COM_CHANGE_USER:
            // denied and close connection
            context.sendErr(encoder, MysqlError.ER_ACCESS_DENIED_ERROR, MysqlError.GENERAL_STATE, "Access denied.");
            context.setState(MysqlServerState.Closed);
            return false;

        case Commands.COM_SET_OPTION:
            return setOption(packet, decoder, encoder);

        case Commands.COM_STMT_PREPARE:
            tag = "stmt prepare";
            tasks = Pipelines.COM_STMT_PREPARE_TASKS;
            break;

        case Commands.COM_STMT_EXECUTE:
            tag = "stmt execute";
            tasks = Pipelines.COM_STMT_EXECUTE_TASKS;
            break;

        case Commands.COM_STMT_FETCH:
            tag = "stmt fetch";
            tasks = Pipelines.COM_STMT_FETCH_TASKS;
            break;

        case Commands.COM_STMT_CLOSE: {
            // no response for this message
            decoder.skip();
            final int stmtId = (int) decoder.u32_s();
            context.getPreparedStatementContexts().remove(stmtId);
            return false;
        }

        case Commands.COM_STMT_RESET:
            tag = "stmt reset";
            tasks = Pipelines.COM_STMT_RESET_TASKS;
            break;

        case Commands.COM_STMT_SEND_LONG_DATA:
            tag = "stmt send long data";
            tasks = Pipelines.COM_STMT_SEND_LONG_DATA_TASKS;
            break;

        default:
            throw new UnsupportedOperationException(
                "Unsupported command: 0x" + Integer.toHexString(peek & 0xFF) + " from " + connection);
        }

        // do task
        final Scheduler scheduler = new Scheduler(connection, context, tag, packet, tasks, decoder, encoder);
        return scheduler.forward();
    }
}
