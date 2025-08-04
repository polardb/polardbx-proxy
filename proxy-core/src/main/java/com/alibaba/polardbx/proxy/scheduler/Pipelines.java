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

public class Pipelines {
    // COM_INIT_DB
    public static final ScheduleTask[] COM_INIT_DB_TASKS = new ScheduleTask[] {
        new DecodeComInitDbTask(),
        new InitRetransmitTask(),
        new PreferSlaveReadTask(),
        new CheckLeaderTransferringTask(),
        new InitBackendTask(),
        new FetchLsnTask(),
        new SetLsnTask(),
        new ForwardComInitDbTask()
    };

    // COM_QUERY
    public static final ScheduleTask[] COM_QUERY_TASKS = new ScheduleTask[] {
        new DecodeComQueryTask(),
        new SystemCommandTask(),
        new InitRetransmitTask(),
        new CheckQuerySlaveReadTask(),
        new CheckLeaderTransferringTask(),
        new InitBackendTask(),
        new FetchLsnTask(),
        new VariablesRestoreTask(),
        new VariablesPostGatherTask(),
        new SetLsnTask(),
        new ForwardComQueryTask()
    };

    // COM_FIELD_LIST
    public static final ScheduleTask[] COM_FIELD_LIST_TASKS = new ScheduleTask[] {
        new DecodeComFieldListTask(),
        new InitRetransmitTask(),
        new PreferSlaveReadTask(),
        new CheckLeaderTransferringTask(),
        new InitBackendTask(),
        new FetchLsnTask(),
        new SetLsnTask(),
        new ForwardComFieldListTask()
    };

    // COM_STATISTICS
    public static final ScheduleTask[] COM_STATISTICS_TASKS = new ScheduleTask[] {
        new InitRetransmitTask(),
        new CheckLeaderTransferringTask(),
        new InitBackendTask(),
        new ForwardComStatisticsTask()
    };

    // COM_DEBUG, etc
    public static final ScheduleTask[] OK_ERR_MASTER_TASKS = new ScheduleTask[] {
        new InitRetransmitTask(),
        new CheckLeaderTransferringTask(),
        new InitBackendTask(),
        new ForwardOkErrRequestTask()
    };

    // COM_PING, etc
    public static final ScheduleTask[] OK_ERR_STALE_SLAVE_TASKS = new ScheduleTask[] {
        new InitRetransmitTask(),
        new PreferSlaveReadTask(),
        new CheckLeaderTransferringTask(),
        new InitBackendTask(),
        new ForwardOkErrRequestTask()
    };

    // COM_STMT_PREPARE
    public static final ScheduleTask[] COM_STMT_PREPARE_TASKS = new ScheduleTask[] {
        new DecodeComStmtPrepareTask(),
        new InitRetransmitTask(),
        // todo go slave if possible?
        new CheckLeaderTransferringTask(),
        new InitBackendTask(),
        new ForwardComStmtPrepareTask()
    };

    // COM_STMT_EXECUTE
    public static final ScheduleTask[] COM_STMT_EXECUTE_TASKS = new ScheduleTask[] {
        new DecodeComStmtExecuteTask(),
        new InitRetransmitTask(),
        new CheckQuerySlaveReadTask(),
        new CheckLeaderTransferringTask(),
        new InitBackendTask(),
        new FetchLsnTask(),
        new BackendPrepareTask(),
        new VariablesRestoreTask(),
        new VariablesPostGatherTask(),
        new SetLsnTask(),
        new ForwardComStmtExecuteTask()
    };

    // COM_STMT_FETCH
    public static final ScheduleTask[] COM_STMT_FETCH_TASKS = new ScheduleTask[] {
        new DecodeComStmtFetchTask(),
        new RestoreSlaveReadViaTrxTask(),
        new RestoreOnGoingStmtBackendTask(),
        new ForwardComStmtFetchTask()
    };

    // COM_STMT_RESET
    public static final ScheduleTask[] COM_STMT_RESET_TASKS = new ScheduleTask[] {
        new DecodeAndResetComStmtReset()
    };

    // COM_STMT_SEND_LONG_DATA
    public static final ScheduleTask[] COM_STMT_SEND_LONG_DATA_TASKS = new ScheduleTask[] {
        new DecodeAndStoreComStmtSendLongData()
    };
}
