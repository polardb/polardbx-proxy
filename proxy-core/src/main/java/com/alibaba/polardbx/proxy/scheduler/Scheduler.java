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

import com.alibaba.polardbx.proxy.ProxyExecutor;
import com.alibaba.polardbx.proxy.callback.ResultCallback;
import com.alibaba.polardbx.proxy.config.FastConfig;
import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.help.PreparedStatementContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;
import com.alibaba.polardbx.proxy.protocol.common.MysqlError;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.common.MysqlServerState;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@Getter
public class Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    /**
     * Basic infos.
     */

    private final FrontendConnection frontend;
    private final FrontendContext context;

    private final String tag;

    private final Slice packet;

    private final long startUTC;
    private final long startNanos;

    private final ScheduleTask[] tasks;

    private final long rescheduleCount;

    /**
     * Dynamic configs.
     */

    private Decoder decoder; // only valid when in first schedule
    private Encoder encoder; // only valid when in first schedule

    @Setter
    private MysqlPacket request;

    @Setter
    private Long retransmitLimitNanos;
    @Setter
    private byte[] retransmitData;

    @Setter
    private Boolean slaveRead;
    @Setter
    private Long specificLsn;

    @Setter
    private PreparedStatementContext preparedStatement;
    private BackendConnectionWrapper backend;
    private Boolean isSlaveConnection;
    @Setter
    private Integer backendPreparedId;

    @Setter
    private boolean dereference;
    @Setter
    private boolean sendError;

    private long retransmitDelayNanos;
    private long fetchLsnNanos;
    private long prepareNanos;
    private long scheduleNanos;
    private long waitLsnNanos;
    private long waitLeaderNanos;

    // post operation
    @Setter
    private String postOperationSql;
    @Setter
    private ResultCallback postOperationCallback;

    /**
     * Build first schedule context.
     */
    public Scheduler(@NotNull FrontendConnection frontend, @NotNull FrontendContext context, @NotNull String tag,
                     @NotNull Slice packet, @NotNull ScheduleTask[] tasks, @NotNull Decoder decoder,
                     @NotNull Encoder encoder) {
        this.frontend = frontend;
        this.context = context;
        this.tag = tag;
        this.packet = packet;
        this.startUTC = System.currentTimeMillis();
        this.startNanos = System.nanoTime();
        this.tasks = tasks;
        this.rescheduleCount = 0;

        // set dynamic configs
        this.decoder = decoder;
        this.encoder = encoder;
        this.request = null;
        this.retransmitLimitNanos = null;
        this.retransmitData = null;
        this.slaveRead = null;
        this.specificLsn = null;
        this.preparedStatement = null;
        this.backend = null;
        this.isSlaveConnection = null;
        this.backendPreparedId = null;
        this.dereference = false;
        this.sendError = true;
        this.retransmitDelayNanos = 0;
        this.fetchLsnNanos = 0;
        this.prepareNanos = 0;
        this.scheduleNanos = 0;
        this.waitLsnNanos = 0;
        this.waitLeaderNanos = 0;
        this.postOperationSql = null;
        this.postOperationCallback = null;
    }

    public void setBackend(BackendConnectionWrapper backend) {
        this.backend = backend;
        this.isSlaveConnection = backend.isSlave();
    }

    public void switchThread() {
        decoder = null;
        encoder = null;
    }

    public void addRetransmitDelayNanos(long nanos) {
        retransmitDelayNanos += nanos;
    }

    public void addFetchLsnNanos(long nanos) {
        fetchLsnNanos += nanos;
    }

    public void addPrepareNanos(long nanos) {
        prepareNanos += nanos;
    }

    public void addScheduleNanos(long nanos) {
        scheduleNanos += nanos;
    }

    public void addWaitLsnNanos(long nanos) {
        waitLsnNanos += nanos;
    }

    public void addWaitLeaderNanos(long nanos) {
        waitLeaderNanos += nanos;
    }

    /**
     * Build a new context for reschedule.
     */
    public Scheduler(@NotNull Scheduler old, @NotNull Slice packet) {
        this.frontend = old.frontend;
        this.context = old.context;
        this.tag = old.tag.endsWith(" retransmit") ? old.tag : old.tag + " retransmit";
        this.packet = packet;
        this.startUTC = old.startUTC;
        this.startNanos = old.startNanos;
        this.tasks = old.tasks;
        this.rescheduleCount = old.rescheduleCount + 1;

        this.decoder = null; // invalid if not in first schedule
        this.encoder = null; // invalid if not in first schedule
        this.request = old.request;

        // copy retransmit context
        this.retransmitLimitNanos = old.retransmitLimitNanos;
        this.retransmitData = old.retransmitData;

        // copy slave decision and fetched LSN
        this.slaveRead = old.slaveRead;
        this.specificLsn = old.specificLsn;

        // copy frontend prepare context
        this.preparedStatement = old.preparedStatement;
        // reset backend
        this.backend = null;
        this.isSlaveConnection = null;
        this.backendPreparedId = null;

        // reset status
        this.dereference = false;
        this.sendError = true;

        // accumulate delay
        this.retransmitDelayNanos = old.retransmitDelayNanos;
        this.fetchLsnNanos = old.fetchLsnNanos;
        this.prepareNanos = old.prepareNanos;
        this.scheduleNanos = old.scheduleNanos;
        this.waitLsnNanos = old.waitLsnNanos;
        this.waitLeaderNanos = old.waitLeaderNanos;

        // reset and rebuild post ops(because backend,scheduler is changed, and may have side effect)
        this.postOperationSql = null;
        this.postOperationCallback = null;
    }

    public boolean errorHandle(Throwable t) throws IOException {
        LOGGER.error("forward {} failed", tag, t);
        if (dereference) {
            dereference = false;
            backend = null; // and clear backend ref
            isSlaveConnection = null;
            final FrontendTransactionContext trx = context.dereferenceTransaction();
            if (trx != null) {
                try {
                    trx.close();
                } catch (Throwable t1) {
                    LOGGER.error("free transaction failed", t1);
                }
            }
        }
        if (sendError) {
            sendError = false;
            // error occurs before actual forward
            final long beforeRetransmitNanos = System.nanoTime();
            final boolean canRetransmit = retransmitLimitNanos != null // enable retransmit
                && null == context.getTransactionContext() // not trx
                && (retransmitLimitNanos - beforeRetransmitNanos) > 0 // still in time
                && MysqlServerState.Authenticated == context.getState(); // still valid
            if (canRetransmit) {
                ProxyExecutor.getInstance().getExecutor().schedule(() -> {
                    boolean needFree = true;
                    try {
                        switchThread(); // mark thread switched
                        LOGGER.info("do general retransmit {} caused by {}", tag, t.getMessage());

                        // record delay
                        final long nowNanos = System.nanoTime();
                        addRetransmitDelayNanos(nowNanos - beforeRetransmitNanos);

                        // then do retransmit within a totally new scheduler
                        final Scheduler newScheduler = new Scheduler(this, packet);
                        final boolean taken = newScheduler.forward();
                        if (taken) {
                            needFree = false;
                        }
                    } catch (Throwable t1) {
                        LOGGER.error("retransmit failed", t1);
                        frontend.close(); // close frontend connection
                    } finally {
                        if (needFree) {
                            packet.close();
                        }
                    }
                }, FastConfig.queryRetransmitFastRetryDelay, TimeUnit.MILLISECONDS);
                return true; // packet taken for retransmit
            } else {
                final int errCode =
                    t instanceof SQLException ? ((SQLException) t).getErrorCode() : MysqlError.ER_INTERNAL_ERROR;
                final String errState =
                    t instanceof SQLException ? ((SQLException) t).getSQLState() : MysqlError.GENERAL_STATE;
                if (encoder != null) {
                    context.sendErr(encoder, errCode, errState, t.getMessage());
                } else {
                    context.sendErr(frontend, errCode, errState, t.getMessage());
                }
            }
        }
        return false; // double free outside is allowed
    }

    // todo: check and add sql log in any step of exception, make sure sql log is recorded
    public boolean forward() throws IOException {
        try {
            for (final ScheduleTask task : tasks) {
                final Boolean result = task.forward(this);
                if (result != null) {
                    return result;
                }
            }
            retransmitLimitNanos = null; // disable retransmit
            throw new Exception("No task handle this request");
        } catch (Throwable t) {
            return errorHandle(t);
        }
    }
}
