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

import com.alibaba.polardbx.proxy.config.ConfigLoader;
import com.alibaba.polardbx.proxy.config.ConfigProps;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.privilege.PrivilegeInfo;
import com.alibaba.polardbx.proxy.privilege.Privileges;
import com.alibaba.polardbx.proxy.privilege.ProxyPrivileges;
import com.alibaba.polardbx.proxy.privilege.SecurityUtil;
import com.alibaba.polardbx.proxy.protocol.common.MysqlError;
import com.alibaba.polardbx.proxy.protocol.common.MysqlProtocolHandler;
import com.alibaba.polardbx.proxy.protocol.common.MysqlServerState;
import com.alibaba.polardbx.proxy.protocol.connection.AuthSwitchRequest;
import com.alibaba.polardbx.proxy.protocol.connection.AuthSwitchResponse;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.connection.HandshakeResponse41;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.Slice;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FrontendAuthenticator extends MysqlProtocolHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendAuthenticator.class);

    public static final String AUTH_METHOD = "mysql_native_password";
    public static final byte[] AUTH_METHOD_BYTES = AUTH_METHOD.getBytes(StandardCharsets.UTF_8);

    private final FrontendContext context;
    @Getter
    private final byte[] seed; // this seed with 0 terminator

    /// data from login response
    private String username;
    private byte[] password;
    private String database;
    private int maxPacketSize;

    public FrontendAuthenticator(FrontendContext context, byte[] seed) {
        setTag("FrontendAuthenticator");
        this.context = context;
        this.seed = seed;
    }

    private boolean finalValidate(Encoder encoder) throws IOException {
        // validate schema
        final Privileges privileges = ProxyPrivileges.getInstance();
        if (database != null && !privileges.schemaExists(database)) {
            final String ip = context.getRemoteIp();
            context.sendErr(encoder, MysqlError.ER_ACCESS_DENIED_ERROR, MysqlError.GENERAL_STATE,
                "Access denied for user '" + username + "'@'" + ip + "' because schema '" + database
                    + "' doesn't exist");
            return false;
        }

        // refresh user & db & max packet size
        context.setUsername(username);
        context.setDatabase(database);
        context.setMaxPacketSize(maxPacketSize);
        context.sendOk(encoder, false);
        return true;
    }

    private boolean authCheck(Encoder encoder) throws IOException {
        if (null == username) {
            context.sendErr(encoder, MysqlError.ER_ACCESS_DENIED_ERROR, MysqlError.GENERAL_STATE,
                "Access denied because user is empty");
            return false;
        }

        final Privileges privileges = ProxyPrivileges.getInstance();
        final String ip = context.getRemoteIp();

        if (privileges.isTrustedIp(ip, username)) {
            // rename to admin user
            username = ConfigLoader.PROPERTIES.getProperty(ConfigProps.BACKEND_USERNAME);
            if (!finalValidate(encoder)) {
                return false;
            }
            // record privilege host
            context.setPrivilegeHost("god");
            return true;
        }

        final PrivilegeInfo privilegeInfo = privileges.getPrivilegeInfo(username, ip);
        if (null == privilegeInfo) {
            context.sendErr(encoder, MysqlError.ER_ACCESS_DENIED_ERROR, MysqlError.GENERAL_STATE,
                "Access denied for user '" + username + "'@'" + ip + "' because host '" + ip
                    + "' is not in the white list");
            return false;
        }

        final byte[] mysqlPassword = privilegeInfo.getAuthentication();
        if (null == password && (null == mysqlPassword || 0 == mysqlPassword.length)) {
            context.sendOk(encoder, false);
            return true;
        }

        if (null == password || null == mysqlPassword || 0 == mysqlPassword.length ||
            !SecurityUtil.verify(password, mysqlPassword, seed, 0,
                seed.length > 0 && 0 == seed[seed.length - 1] ? seed.length - 1 : seed.length)) {
            context.sendErr(encoder, MysqlError.ER_ACCESS_DENIED_ERROR, MysqlError.GENERAL_STATE,
                "Access denied for user '" + username + "'@'" + ip + "' because password is not correct");
            return false;
        }

        if (!finalValidate(encoder)) {
            return false;
        }
        // record privilege host
        context.setPrivilegeHost(privilegeInfo.getHost());
        return true;
    }

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        throw new UnsupportedOperationException("Encoder must be provided when handling authenticate.");
    }

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder, Encoder encoder) throws IOException {
        if (MysqlServerState.Greeting == context.getState()) {
            final HandshakeResponse41 response = new HandshakeResponse41();
            response.decode(decoder, 0);

            // record useful data first
            context.mergeCapability(response.getClientFlag());
            final boolean switchCharset = context.setCharset(response.getCharacterSet());
            if (!switchCharset) {
                context.sendErr(encoder, MysqlError.ER_ACCESS_DENIED_ERROR, MysqlError.GENERAL_STATE,
                    "Access denied because character set is not supported");
                context.setState(MysqlServerState.Closed);
                return false; // not taken
            }
            // no encoding for username, so use utf8
            username = new String(response.getUsername(), StandardCharsets.UTF_8);
            password = response.getAuthResponse();
            if (password != null && 0 == password.length) {
                password = null;
            }
            database = context.decodeStringClient(response.getDatabase());
            maxPacketSize = response.getMaxPacketSize();

            // check capability after merging capability
            if (!context.hasCapability(Capabilities.CLIENT_PROTOCOL_41)) {
                context.sendErr(encoder, MysqlError.ER_ACCESS_DENIED_ERROR, MysqlError.GENERAL_STATE,
                    "Access denied because CLIENT_PROTOCOL_41 is not supported");
                context.setState(MysqlServerState.Closed);
                return false; // not taken
            }
            // todo check CLIENT_FOUND_ROWS

            // check auth method
            if (response.getAuthPluginName() != null
                && !AUTH_METHOD.equalsIgnoreCase(new String(response.getAuthPluginName()))
                && (response.getClientFlag() & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
                // switch auth method if needed
                final AuthSwitchRequest request = new AuthSwitchRequest();
                request.setPluginName(AUTH_METHOD_BYTES);
                request.setPluginData(seed);
                request.encode(encoder, context.getCapabilities());
                context.setState(MysqlServerState.AuthSwitched);
                return false; // not taken
            }
        } else if (MysqlServerState.AuthSwitched == context.getState()) {
            // auth switched, get challenge response
            final AuthSwitchResponse response = new AuthSwitchResponse();
            response.decode(decoder, 0);
            password = response.getData();
        } else {
            LOGGER.warn("Unexpected state: {} when handling authenticate.", context.getState());
            context.setState(MysqlServerState.Closed);
            return false; // not taken
        }

        // do auth check
        context.setState(authCheck(encoder) ? MysqlServerState.Authenticated : MysqlServerState.Closed);
        return false; // not taken
    }
}
