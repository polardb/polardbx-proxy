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

package com.alibaba.polardbx.proxy.protocol.handler.result;

import com.alibaba.polardbx.proxy.callback.ResultCallback;
import com.alibaba.polardbx.proxy.connection.FrontendConnection;
import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.MysqlContext;
import com.alibaba.polardbx.proxy.protocol.command.BinaryValue;
import com.alibaba.polardbx.proxy.protocol.command.ColumnDefinition41;
import com.alibaba.polardbx.proxy.protocol.command.EofPacket;
import com.alibaba.polardbx.proxy.protocol.command.ErrPacket;
import com.alibaba.polardbx.proxy.protocol.command.OkPacket;
import com.alibaba.polardbx.proxy.protocol.command.StatusFlags;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.protocol.handler.MysqlForwarder;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.utils.NotifyQueue;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class QueryResultHandler extends ResultHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResultHandler.class);
    private static final byte[][] EOF_ROW = new byte[0][];

    // result info
    @Getter
    private long globalMetaVersion;
    @Getter
    private int columnCount;
    @Getter
    private List<ColumnDefinition41> fields; // may null when no filed meta
    @Getter
    private final boolean binaryProtocol;
    private final NotifyQueue<Object[]> rows = new NotifyQueue<>();
    @Getter
    private ErrPacket err; // valid when state is Error
    @Getter
    private EofPacket eof; // valid when state is EOF
    @Getter
    private OkPacket ok; // valid when state is Ok

    // blocking head & fields for forwarding(this enabled error packet when abort state)
    private final List<byte[]> pendingPackets;
    private int compatibleSeqPatch = 0;

    // result set id
    @Getter
    private final ResultHandler previous;

    // flow control read resume callback(strong ref here to prevent GC)
    private final Runnable writeResumeCallback = () -> {
        final BackendConnectionWrapper backend = scheduler.getBackend();
        if (backend != null && forwarder != null && !forwarder.getConnection().isWriteBlocking()) {
            backend.enableRead();
        }
    };

    public QueryResultHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                              MysqlForwarder forwarder, ResultCallback stateCallback) {
        super(contextReference, scheduler, forwarder, stateCallback);
        setTag("QueryResultHandler");
        this.binaryProtocol = false;
        this.pendingPackets = null == forwarder ? null : new ArrayList<>();
        this.previous = null;
    }

    // for binary protocol
    public QueryResultHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                              MysqlForwarder forwarder, ResultCallback stateCallback, boolean binaryProtocol) {
        super(contextReference, scheduler, forwarder, stateCallback);
        setTag("QueryResultHandler");
        this.binaryProtocol = binaryProtocol;
        this.pendingPackets = null == forwarder ? null : new ArrayList<>();
        this.previous = null;
    }

    // for multi-result query's sub results
    QueryResultHandler(@NotNull AtomicReference<BackendContext> contextReference, Scheduler scheduler,
                       MysqlForwarder forwarder, ResultCallback stateCallback, boolean packetForwarded,
                       boolean packetDroppedByLsn, boolean binaryProtocol, ResultHandler previous) {
        super(contextReference, scheduler, forwarder, stateCallback, packetForwarded, packetDroppedByLsn);
        setTag("QueryResultHandler sub result");
        this.binaryProtocol = binaryProtocol;
        this.pendingPackets = null == forwarder ? null : new ArrayList<>();
        this.previous = previous;
    }

    private enum PacketDealing {
        PENDING, // store packet into pending list
        DROP, // drop packet
        FORWARD, // ignore pending list, just forward packet
        FORWARD_AND_PUSH // forward packet first and then push pending list
    }

    private PacketDealing dealingPushFieldsEOF(Slice packet) {
        if (forwarder != null) {
            assert pendingPackets != null;
            // fields are ready, push it to forwarder if exists
            if (!pendingPackets.isEmpty()) {
                pushPackets(pendingPackets);
                pendingPackets.clear();
            }
            final FrontendContext forwardContext = forwarder.getContext();
            if (!forwardContext.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                // need add fields EOF and add seq of each packet following
                final ByteBuffer buffer = packet.duplicateBuffer();
                final int realSeqPosition = buffer.position() + 3;
                final int eofSeq = (buffer.get(realSeqPosition) & 0xFF) + 1;
                try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                    try (final Encoder encoder = Encoder.create(null, output)) {
                        encoder.setSeq(eofSeq);
                        final EofPacket tmp = new EofPacket();
                        tmp.setStatusFlags(forwardContext.genStatusFlags(false));
                        tmp.encode(encoder, forwardContext.getCapabilities());
                        encoder.flush();
                    }
                    pendingPackets.add(output.getBytes());
                } catch (IOException e) {
                    throw new RuntimeException("Error when construct fields EOF packet", e);
                }
                compatibleSeqPatch = 1;
            }
        }

        updateState(ResultState.Rows);
        assert 0 == compatibleSeqPatch || 1 == compatibleSeqPatch;
        return forwarder != null ?
            (compatibleSeqPatch > 0 ? PacketDealing.FORWARD_AND_PUSH : PacketDealing.FORWARD) :
            PacketDealing.DROP;
    }

    private void patchSequence(ByteBuffer duplicated, int nowSeq) {
        int pos = duplicated.position() + 3, limit = duplicated.limit();
        do {
            duplicated.put(pos, (byte) nowSeq++);
            pos += MysqlPacket.NORMAL_HEADER_SIZE + MysqlPacket.MAX_PAYLOAD_SIZE;
        } while (pos < limit);
    }

    // todo check capabilities one before actual forward packet
    @Override
    public synchronized boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        final BackendContext context = contextReference.getAcquire();
        assert context != null;

        // early abort on bad state
        if (ResultState.Abort == state) {
            throw new IllegalStateException("QueryResultHandler is in Abort state");
        }

        // patch seq for fields EOF compatible
        final int nowSeq;
        if (compatibleSeqPatch != 0) {
            final ByteBuffer buffer = packet.duplicateBuffer();
            final int realSeqPosition = buffer.position() + 3;
            assert forwarder != null;
            nowSeq = (buffer.get(realSeqPosition) & 0xFF) + compatibleSeqPatch;
            patchSequence(buffer, nowSeq);
        } else {
            nowSeq = 0;
        }

        // highest level error check
        final int peek = decoder.peek_s() & 0xFF;
        if (0xFF == peek) {
            // error
            final ErrPacket tmp = new ErrPacket();
            tmp.decode(decoder, context.getCapabilities());
            err = tmp;
            if (pendingPackets != null && !pendingPackets.isEmpty()) {
                pushPackets(pendingPackets);
                pendingPackets.clear();
            }
            rows.put(EOF_ROW); // end mark
            updateState(ResultState.Error);
            return forwardPacket(packet, decoder);
        }

        // or check with state

        final PacketDealing dealing;
        switch (state) {
        case Init:
            // check ok or column number
            if (0x00 == peek) {
                // ok
                final OkPacket tmp = new OkPacket();
                tmp.decode(decoder, context.getCapabilities());
                ok = tmp;
                // auto update status in context
                context.updateStatus(tmp.getWarnings(), tmp.getStatusFlags());
                if ((tmp.getStatusFlags() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) != 0) {
                    more = new QueryResultHandler(contextReference, scheduler, forwarder, stateCallback,
                        packetForwarded, packetDroppedByLsn, binaryProtocol, this);
                }
                assert null == pendingPackets || pendingPackets.isEmpty();
                rows.put(EOF_ROW); // end mark
                updateState(ResultState.OK);
                dealing = PacketDealing.FORWARD;
            } else {
                final long compressed = decoder.lei_s();
                final boolean carryMeta;
                if (context.hasCapability(Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA)) {
                    globalMetaVersion =
                        (compressed >> 24 & 255L) + (compressed >> 24 & 65280L) +
                            (compressed >> 24 & 16711680L) + (compressed >> 24 & 4278190080L);
                    carryMeta = ((compressed >> 56 & 255L) & 64L) != 0L;
                    columnCount = (int) ((compressed & 255L) + (compressed & 65280L) + (compressed & 16711680L));
                } else {
                    // not set means full metadata
                    globalMetaVersion = -1;
                    carryMeta = true;
                    columnCount = (int) compressed;
                }

                // when get here means columnCount always > 0
                assert columnCount > 0;

                if (carryMeta) {
                    fields = new ArrayList<>(columnCount);
                    updateState(ResultState.Fields);
                    dealing = forwarder != null ? PacketDealing.PENDING : PacketDealing.DROP;
                } else if (!context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                    // reference code in com.mysql.jdbc.MysqlIO.getResultSet
                    // always got eof even not carry meta
                    updateState(ResultState.FieldsEOF);
                    dealing = forwarder != null ? PacketDealing.PENDING : PacketDealing.DROP;
                } else {
                    // backend with CLIENT_DEPRECATE_EOF and not carry meta, check frontend
                    dealing = dealingPushFieldsEOF(packet);
                }
            }
            break;

        case Fields: {
            final ColumnDefinition41 definition = new ColumnDefinition41();
            definition.decode(decoder, context.getCapabilities());
            fields.add(definition);
            if (fields.size() >= columnCount) {
                if (!context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                    updateState(ResultState.FieldsEOF);
                    dealing = forwarder != null ? PacketDealing.PENDING : PacketDealing.DROP;
                } else {
                    dealing = dealingPushFieldsEOF(packet);
                }
            } else {
                dealing = forwarder != null ? PacketDealing.PENDING : PacketDealing.DROP;
            }
        }
        break;

        case FieldsEOF: {
            final EofPacket eof = new EofPacket();
            eof.decode(decoder, context.getCapabilities());
            // auto update status in context
            context.updateStatus(eof.getWarnings(), eof.getStatusFlags());
            // just ignore this packet
            updateState(ResultState.Rows);
            if (forwarder != null) {
                // following are rows, so push all pending packets
                if (!pendingPackets.isEmpty()) {
                    pushPackets(pendingPackets);
                    pendingPackets.clear();
                }
                if (forwarder.getContext().hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                    // drop it and each following packets' seq sub 1
                    dealing = PacketDealing.DROP;
                    compatibleSeqPatch = -1;
                } else {
                    // normal forward
                    assert 0 == compatibleSeqPatch;
                    dealing = PacketDealing.FORWARD;
                }
            } else {
                dealing = PacketDealing.DROP;
            }
        }
        break;

        case Rows: {
            // error checked before, now just check eof
            if (0xFE == peek && decoder.remaining() < 9) {
                // eof(ok packet of select result should less than 9 bytes)
                final int warnings, statusFlags;
                if (context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                    final OkPacket tmp = new OkPacket();
                    tmp.decode(decoder, context.getCapabilities());
                    warnings = tmp.getWarnings();
                    statusFlags = tmp.getStatusFlags();
                    ok = tmp;
                    // auto update status in context
                    context.updateStatus(tmp.getWarnings(), tmp.getStatusFlags());
                    if ((tmp.getStatusFlags() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) != 0) {
                        more = new QueryResultHandler(contextReference, scheduler, forwarder, stateCallback,
                            packetForwarded, packetDroppedByLsn, binaryProtocol, this);
                    }
                    rows.put(EOF_ROW); // end mark
                    updateState(ResultState.OK);
                } else {
                    final EofPacket tmp = new EofPacket();
                    tmp.decode(decoder, context.getCapabilities());
                    warnings = tmp.getWarnings();
                    statusFlags = tmp.getStatusFlags();
                    eof = tmp;
                    // auto update status in context
                    context.updateStatus(tmp.getWarnings(), tmp.getStatusFlags());
                    if ((tmp.getStatusFlags() & StatusFlags.SERVER_MORE_RESULTS_EXISTS) != 0) {
                        more = new QueryResultHandler(contextReference, scheduler, forwarder, stateCallback,
                            packetForwarded, packetDroppedByLsn, binaryProtocol, this);
                    }
                    rows.put(EOF_ROW); // end mark
                    updateState(ResultState.EOF);
                }

                // rebuild end packet if CLIENT_DEPRECATE_EOF mismatch
                if (forwarder != null) {
                    final MysqlContext forwardContext = forwarder.getContext();
                    if (compatibleSeqPatch != 0) {
                        assert context.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF) != forwardContext.hasCapability(
                            Capabilities.CLIENT_DEPRECATE_EOF);
                        try (final Encoder.BytesOutput output = new Encoder.BytesOutput()) {
                            try (final Encoder encoder = Encoder.create(null, output)) {
                                encoder.setSeq(nowSeq);
                                if (forwardContext.hasCapability(Capabilities.CLIENT_DEPRECATE_EOF)) {
                                    // build ok packet
                                    final OkPacket tmp = new OkPacket();
                                    tmp.setEOF(true);
                                    tmp.setWarnings(warnings);
                                    tmp.setStatusFlags(statusFlags);
                                    tmp.encode(encoder, forwardContext.getCapabilities());
                                } else {
                                    // build eof packet
                                    final EofPacket tmp = new EofPacket();
                                    tmp.setWarnings(warnings);
                                    tmp.setStatusFlags(statusFlags);
                                    tmp.encode(encoder, forwardContext.getCapabilities());
                                }
                                encoder.flush();
                            }
                            pendingPackets.add(output.getBytes());
                        } catch (IOException e) {
                            throw new RuntimeException("Error when construct fields EOF packet", e);
                        }
                        // push this rewrite packet
                        if (!pendingPackets.isEmpty()) {
                            pushPackets(pendingPackets);
                            pendingPackets.clear();
                        }
                        // and drop original packet
                        dealing = PacketDealing.DROP;
                    } else {
                        dealing = PacketDealing.FORWARD;
                    }
                } else {
                    dealing = PacketDealing.DROP;
                }
            } else {
                // new a row if no forwarder
                if (null == forwarder) {
                    if (binaryProtocol) {
                        // consume header
                        if (decoder.u8_s() != 0x00) {
                            throw new RuntimeException("Error when decode binary row packet, header is not 0x00");
                        }
                        final int nullBitmapLen = (columnCount + 7 + 2) / 8;
                        if (decoder.remaining() < nullBitmapLen) {
                            throw new RuntimeException(
                                "Error when decode binary row packet, null bitmap is not enough");
                        }
                        final byte[] nullBitmap = decoder.str(nullBitmapLen);
                        final BinaryValue[] row = new BinaryValue[columnCount];
                        for (int i = 0; i < columnCount; i++) {
                            if ((nullBitmap[i / 8] & (1 << (i % 8))) != 0) {
                                // null
                                continue;
                            }
                            final BinaryValue val = new BinaryValue(fields.get(i).getType() & 0xFF, null);
                            val.decodeValue(decoder);
                            row[i] = val;
                        }
                        rows.put(row);
                    } else {
                        final byte[][] row = new byte[columnCount][];
                        for (int i = 0; i < columnCount; i++) {
                            if ((decoder.peek_s() & 0xFF) == 0xFB) {
                                // null(lei start with 0xFB will never occur, because 0xFB will be encoded to 2 bytes lei)
                                decoder.skip();
                                continue;
                            }
                            row[i] = decoder.le_str_s();
                        }
                        rows.put(row);
                    }
                }
                // row seq already patched
                dealing = PacketDealing.FORWARD;
            }
        }
        break;

        default:
            throw new RuntimeException(
                "Bad state: " + state + " peek bytes: 0x" + Integer.toHexString(peek));
        }

        // dealing pending and packet
        switch (dealing) {
        case PENDING:
            pendingPackets.add(packet.dump()); // make a heap copy
            // fall through
        case DROP:
        default:
            return false; // not taken

        case FORWARD:
            if (pendingPackets != null && !pendingPackets.isEmpty()) {
                throw new RuntimeException("Pending packets should be empty");
            }
            return forwardPacket(packet, decoder);

        case FORWARD_AND_PUSH:
            final boolean taken = forwardPacket(packet, decoder);
            if (!pendingPackets.isEmpty()) {
                pushPackets(pendingPackets);
                pendingPackets.clear();
            }
            return taken;
        }
    }

    @Override
    public void handleFinish() {
        super.handleFinish();
        // done flush on forwarder

        // flow control
        if (forwarder != null) {
            final FrontendConnection frontend = forwarder.getConnection();
            if (frontend.isWriteBlocking()) {
                // back pressure on backend needed
                final BackendConnectionWrapper backend = scheduler.getBackend();
                if (backend != null) {
                    backend.disableRead();
                    frontend.registerWriteResumeListener(writeResumeCallback);
                    // recheck resumed
                    if (!frontend.isWriteBlocking()) {
                        backend.enableRead();
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup() {
        super.cleanup();

        // remove flow control listener when done
        if (forwarder != null && getState().isDone()) {
            forwarder.getConnection().removeWriteResumeListener(writeResumeCallback);
        }

        // early abort for row consumer
        if (null == forwarder && getState().isAbort()) {
            rows.put(EOF_ROW);
        }
    }

    public byte[][] next(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, SQLException {
        final Object[] row = rows.poll(timeout, unit);
        if (null == row) {
            throw new TimeoutException("Fetch result timeout. " + unit.toMillis(timeout) + " ms");
        }
        if (EOF_ROW == row) {
            // optimistic read here without synchronized
            if (state.isError()) {
                if (err != null) {
                    final BackendContext context = contextReference.getAcquire();
                    throw new SQLException(context.decodeStringResults(err.getErrorMessage()),
                        context.decodeStringResults(err.getSqlState()), err.getErrorCode());
                } else {
                    throw new SQLException();
                }
            } else if (state.isAbort()) {
                throw new RuntimeException("Query abort.");
            }
            return null;
        }
        if (row instanceof byte[][]) {
            // text protocol
            return (byte[][]) row;
        }
        // binary protocol
        final byte[][] bytesRow = new byte[row.length][];
        for (int i = 0; i < row.length; i++) {
            bytesRow[i] = row[i] instanceof BinaryValue ? ((BinaryValue) row[i]).toBytes() : null;
        }
        return bytesRow;
    }

    public void consume(Consumer<byte[][]> consumer, long limitTimeNs)
        throws SQLException, InterruptedException, TimeoutException {
        while (true) {
            final long rest_ns = limitTimeNs - System.nanoTime();
            if (rest_ns <= 0) {
                throw new TimeoutException("Fetch result timeout.");
            }
            final byte[][] row = next(rest_ns, TimeUnit.NANOSECONDS);
            if (row != null) {
                consumer.accept(row);
            } else {
                break;
            }
        }
    }

    public long update(long limitTimeNs) throws SQLException, InterruptedException, TimeoutException {
        while (true) {
            final long rest_ns = limitTimeNs - System.nanoTime();
            if (rest_ns <= 0) {
                throw new TimeoutException("Fetch result timeout.");
            }
            final byte[][] row = next(rest_ns, TimeUnit.NANOSECONDS);
            if (null == row) {
                break;
            }
        }
        // Note: Status changed after EOF_ROW pushed, so don't check state, or we may get stale result.
        return ok != null ? ok.getAffectedRows() : 0;
    }
}
