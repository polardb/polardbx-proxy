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

package com.alibaba.polardbx.proxy.context;

import com.alibaba.polardbx.proxy.connection.configs.ReadOnlyConfigs;
import com.alibaba.polardbx.proxy.parser.recognizer.SQLParser;
import com.alibaba.polardbx.proxy.privilege.PrivilegeRefresher;
import com.alibaba.polardbx.proxy.protocol.command.ComQuery;
import com.alibaba.polardbx.proxy.protocol.command.StatusFlags;
import com.alibaba.polardbx.proxy.protocol.common.MysqlPacket;
import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.protocol.prepare.ComStmtExecute;
import com.alibaba.polardbx.proxy.scheduler.Scheduler;
import com.alibaba.polardbx.proxy.serverless.HaManager;
import com.alibaba.polardbx.proxy.utils.CaseInsensitiveString;
import com.alibaba.polardbx.proxy.utils.CharsetMapping;
import com.alibaba.polardbx.proxy.utils.LeakChecker;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public abstract class MysqlContext extends LeakChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlContext.class);

    public static final int DEFAULT_CHARSET_INDEX = CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4_general_ci;
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    protected final InetSocketAddress remoteAddress;
    protected final int connectionId;
    protected int capabilities;

    // charsets
    protected int clientCharsetIndex;
    protected Charset clientJavaCharset;
    protected int connectionCharsetIndex;
    protected Charset connectionJavaCharset;
    protected int resultsCharsetIndex;
    protected Charset resultsJavaCharset;

    @Setter
    protected String username;
    @Setter
    protected String privilegeHost; // host in mysql.user table which login matched
    @Setter
    protected String database;
    @Setter
    protected int maxPacketSize = 16 * 1024 * 1024;

    // execution status
    @Setter
    protected int warnings = 0;
    protected boolean inTransaction = false;
    protected boolean isAutoCommit = true;
    protected boolean cursorExists = false;

    // other infos
    @Setter
    protected String sqlMode;

    @Setter
    protected String lastError;

    @Setter
    protected String mock;

    // configs and variables for backend
    protected ReadOnlyConfigs readOnlyConfigs;
    protected Map<CaseInsensitiveString, String> globalVariables;

    // variables
    protected final Map<CaseInsensitiveString, String> userVariables = new ConcurrentHashMap<>(); // <'a','1'>
    protected final Map<CaseInsensitiveString, String> systemVariables = new ConcurrentHashMap<>(); // <'aa', '1'>

    public MysqlContext(InetSocketAddress remoteAddress, int connectionId, int capabilities) {
        this.remoteAddress = remoteAddress;
        this.connectionId = connectionId;
        this.capabilities = capabilities;
        setTag("MysqlContext of " + remoteAddress.getHostString() + ':' + remoteAddress.getPort() + '-' + connectionId);
    }

    public String getRemoteIp() {
        return remoteAddress.getAddress().getHostAddress();
    }

    public void addCapability(int capability) {
        this.capabilities |= capability;
    }

    public void removeCapability(int capability) {
        this.capabilities &= ~capability;
    }

    public boolean hasCapability(int capability) {
        return (this.capabilities & capability) != 0;
    }

    public void mergeCapability(int capability) {
        this.capabilities &= capability;
    }

    public boolean setCharset(byte charsetIndex) {
        return setCharset(charsetIndex & 0xFF);
    }

    public boolean setCharset(int charsetIndex) {
        this.clientCharsetIndex = this.connectionCharsetIndex = this.resultsCharsetIndex = charsetIndex;
        final String charsetString = CharsetMapping.getStaticJavaEncodingForCollationIndex(charsetIndex);
        if (null == charsetString) {
            return false;
        }
        // get charset
        final Charset charset;
        try {
            if (Charset.isSupported(charsetString)) {
                charset = Charset.forName(charsetString);
            } else {
                return false;
            }
        } catch (Throwable ignore) {
            return false;
        }
        this.clientJavaCharset = this.connectionJavaCharset = this.resultsJavaCharset = charset;
        return true;
    }

    public boolean isCharsetReady() {
        return clientJavaCharset != null && connectionJavaCharset != null && resultsJavaCharset != null;
    }

    public String decodeStringClient(byte[] str) {
        if (null == str || str.length == 0) {
            return null;
        }
        return new String(str, clientJavaCharset);
    }

    public byte[] encodeStringClient(String str) {
        if (null == str || str.isEmpty()) {
            return null;
        }
        return str.getBytes(clientJavaCharset);
    }

    public String decodeStringResults(byte[] str) {
        if (null == str || str.length == 0) {
            return null;
        }
        return new String(str, resultsJavaCharset);
    }

    public byte[] encodeStringResults(String str) {
        if (null == str || str.isEmpty()) {
            return null;
        }
        return str.getBytes(resultsJavaCharset);
    }

    public void updateStatus(int warnings, int status) {
        this.warnings = warnings;
        this.inTransaction = (status & StatusFlags.SERVER_STATUS_IN_TRANS) != 0;
        this.isAutoCommit = (status & StatusFlags.SERVER_STATUS_AUTOCOMMIT) != 0;
        this.cursorExists = (status & StatusFlags.SERVER_STATUS_CURSOR_EXISTS) != 0;
    }

    public void updateStatus(MysqlContext another) {
        this.warnings = another.warnings;
        this.inTransaction = another.inTransaction;
        this.isAutoCommit = another.isAutoCommit;
        this.cursorExists = another.cursorExists;
    }

    public void recordSqlAffects(Scheduler scheduler, List<QueryResultHandler> handlers, MysqlContext backendContext) {
        try {
            // success flags
            boolean hasSuccess = false;
            final boolean[] success = new boolean[handlers.size()];
            for (int i = 0; i < handlers.size(); i++) {
                final ResultState state = handlers.get(i).getState();
                final boolean result = ResultState.OK == state || ResultState.EOF == state;
                if (result) {
                    hasSuccess = true;
                }
                success[i] = result;
            }
            if (!hasSuccess) {
                return; // ignore if all failed
            }

            final SQLParser parser;
            final MysqlPacket request = scheduler.getRequest();
            if (request instanceof ComQuery) {
                final byte[] query = ((ComQuery) request).getQuery();
                parser = new SQLParser(query, 0, query.length, clientJavaCharset, sqlMode,
                    HaManager.getInstance().getVersion());
            } else if (request instanceof ComStmtExecute && !handlers.isEmpty()) {
                final Charset defaultCharset = Charset.defaultCharset();
                final byte[] bytes = scheduler.getPreparedStatement().getPrepareSql().getBytes(defaultCharset);
                parser = new SQLParser(bytes, 0, bytes.length, defaultCharset, sqlMode,
                    HaManager.getInstance().getVersion());
            } else {
                return;
            }

            // refresh privilege & database info when something changed
            if (parser.isPrivilegeDatabaseChanged()) {
                if (LOGGER.isInfoEnabled()) {
                    final String sql =
                        request instanceof ComQuery ? decodeStringClient(((ComQuery) request).getQuery()) :
                            scheduler.getPreparedStatement().getPrepareSql();
                    LOGGER.info("Privilege info changed caused by sql: {}", sql);
                }
                PrivilegeRefresher.getInstance().refresh();
            }

            // todo use SERVER_STATUS_DB_DROPPED?
            // use db, drop db
            final String newDB = parser.applyDatabase(database,
                null == backendContext.getReadOnlyConfigs() || backendContext.getReadOnlyConfigs()
                    .isLowerCaseTableNames(), success);
            if (!Objects.equals(newDB, database)) {
                database = newDB;
                backendContext.database = newDB;
            }

            // user var, session var and encoding are gathered by post gather task
        } catch (Throwable t) {
            LOGGER.error("error when record sql affects", t);
        }
    }

    public void setConfigsAndGlobalVariables(ReadOnlyConfigs readOnlyConfigs,
                                             Map<CaseInsensitiveString, String> globalVariables) {
        this.readOnlyConfigs = readOnlyConfigs;
        this.globalVariables = globalVariables;
        // set sql mode to initial value
        sqlMode = null == globalVariables ? null : globalVariables.get(new CaseInsensitiveString("sql_mode"));
    }
}
