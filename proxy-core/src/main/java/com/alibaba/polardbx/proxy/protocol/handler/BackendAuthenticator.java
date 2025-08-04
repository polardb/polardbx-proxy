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

import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.context.MysqlContext;
import com.alibaba.polardbx.proxy.privilege.SecurityUtil;
import com.alibaba.polardbx.proxy.protocol.command.ErrPacket;
import com.alibaba.polardbx.proxy.protocol.common.MysqlClientState;
import com.alibaba.polardbx.proxy.protocol.common.MysqlProtocolHandler;
import com.alibaba.polardbx.proxy.protocol.connection.AuthMoreData;
import com.alibaba.polardbx.proxy.protocol.connection.AuthSwitchRequest;
import com.alibaba.polardbx.proxy.protocol.connection.AuthSwitchResponse;
import com.alibaba.polardbx.proxy.protocol.connection.Capabilities;
import com.alibaba.polardbx.proxy.protocol.connection.HandshakeResponse41;
import com.alibaba.polardbx.proxy.protocol.connection.HandshakeV10;
import com.alibaba.polardbx.proxy.protocol.decoder.Decoder;
import com.alibaba.polardbx.proxy.protocol.encoder.Encoder;
import com.alibaba.polardbx.proxy.utils.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.util.concurrent.atomic.AtomicReference;

public class BackendAuthenticator extends MysqlProtocolHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackendAuthenticator.class);

    public static final String AUTH_METHOD = "mysql_native_password";
    public static final byte[] AUTH_METHOD_BYTES = AUTH_METHOD.getBytes(StandardCharsets.UTF_8);

    private final InetSocketAddress remoteAddress;
    private final AtomicReference<BackendContext> contextReference;

    // login info
    private final String username;
    private final String encryptedPassword;
    private final String database;

    public BackendAuthenticator(InetSocketAddress remoteAddress, AtomicReference<BackendContext> contextReference,
                                String username, String encryptedPassword, String database) {
        setTag("BackendAuthenticator");
        this.remoteAddress = remoteAddress;
        this.contextReference = contextReference;
        this.username = username;
        this.encryptedPassword = encryptedPassword;
        this.database = database;
    }

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder) {
        throw new UnsupportedOperationException("Encoder must be provided when handling authenticate.");
    }

    @Override
    public boolean handleAndTakePacket(Slice packet, Decoder decoder, Encoder encoder) throws IOException {
        final BackendContext originalContext = contextReference.getPlain();
        if (null == originalContext) {
            // first packet, should be a handshake packet or error packet
            if ((decoder.peek_s() & 0xFF) == 0xFF) {
                // login fail and fake a context
                final BackendContext context = new BackendContext(
                    remoteAddress, -1, Capabilities.getBaseCapabilities());
                context.setCharset(MysqlContext.DEFAULT_CHARSET_INDEX);
                contextReference.setRelease(context);

                final ErrPacket err = new ErrPacket();
                err.decode(decoder, context.getCapabilities());
                final String errStr = context.decodeStringResults(err.getErrorMessage());
                context.setLastError(
                    "Auth fail. Error code: " + err.getErrorCode()
                        + ", Error message: " + (null == errStr ? "<null>" : errStr));
                context.setState(MysqlClientState.Closed);
            } else {
                final HandshakeV10 handshake = new HandshakeV10();
                handshake.decode(decoder, 0);
                final BackendContext context = new BackendContext(
                    remoteAddress, handshake.getConnectionId(), handshake.getCapabilityFlags());
                contextReference.setRelease(context);
                // merge my capabilities
                context.mergeCapability(Capabilities.getBaseCapabilities());

                // check capabilities
                if (!context.hasCapability(Capabilities.CLIENT_PROTOCOL_41)) {
                    context.setLastError("Unsupported protocol without CLIENT_PROTOCOL_41.");
                    context.setState(MysqlClientState.Closed);
                    return false; // not taken
                }

                context.setVersion(handshake.getVersion());
                // then do challenge
                context.setUsername(username);
                context.setDatabase(database);
                final byte[] scramble = handshake.getAuthPluginData();
                final byte[] token =
                    SecurityUtil.challenge(SecurityUtil.decrypt(encryptedPassword).getBytes(StandardCharsets.UTF_8),
                        scramble, 0,
                        scramble.length > 0 && 0 == scramble[scramble.length - 1] ? scramble.length - 1 :
                            scramble.length);

                // build response
                final HandshakeResponse41 response = new HandshakeResponse41();
                response.setClientFlag(context.getCapabilities());
                response.setMaxPacketSize(context.getMaxPacketSize());
                response.setCharacterSet((byte) MysqlContext.DEFAULT_CHARSET_INDEX);
                final boolean ignore = context.setCharset(MysqlContext.DEFAULT_CHARSET_INDEX);
                // no encoding for user, so use utf8
                response.setUsername(username.getBytes(StandardCharsets.UTF_8));
                response.setAuthResponse(token);
                response.setDatabase(context.encodeStringClient(database));
                response.setAuthPluginName(AUTH_METHOD_BYTES);

                // send it
                response.encode(encoder, context.getCapabilities());
                context.setState(MysqlClientState.WaitAuth);
            }
        } else if (MysqlClientState.WaitAuth == originalContext.getState()) {
            // auth result
            switch (decoder.peek_s() & 0xFF) {
            case 0x00:
                // auth success
                originalContext.setState(MysqlClientState.Authenticated);
                break;

            case 0x01: {
                final AuthMoreData moreData = new AuthMoreData();
                moreData.decode(decoder, originalContext.getCapabilities());

                // print result with hex
                final StringBuilder builder = new StringBuilder();
                for (final byte b : moreData.getData()) {
                    builder.append(String.format("%02x", b));
                }
                originalContext.setLastError("Unexpected server state when auth more data: " + builder);
                originalContext.setState(MysqlClientState.Closed);
            }
            break;

            case 0xFE: {
                // auth switch
                final AuthSwitchRequest request = new AuthSwitchRequest();
                request.decode(decoder, originalContext.getCapabilities());
                // challenge if supported(no encoding)
                final String authMethod = new String(request.getPluginName(), StandardCharsets.UTF_8);
                final byte[] scramble = request.getPluginData();
                final byte[] token;
                if (authMethod.equalsIgnoreCase("caching_sha2_password")) {
                    try {
                        token = SecurityUtil.challengeCachingSha2(
                            SecurityUtil.decrypt(encryptedPassword).getBytes(StandardCharsets.UTF_8), scramble, 0,
                            scramble.length > 0 && 0 == scramble[scramble.length - 1] ? scramble.length - 1 :
                                scramble.length);
                    } catch (DigestException e) {
                        LOGGER.error("Error when challenge caching_sha2_password.", e);
                        throw new RuntimeException(e);
                    }
                } else {
                    token = null;
                }
                if (token != null) {
                    final AuthSwitchResponse response = new AuthSwitchResponse();
                    response.setData(token);
                    // send it
                    response.encode(encoder, originalContext.getCapabilities());
                } else {
                    originalContext.setLastError("Auth switch needed. Server method: " + authMethod);
                    originalContext.setState(MysqlClientState.Closed);
                }
            }
            break;

            case 0xFF: {
                // auth fail
                final ErrPacket err = new ErrPacket();
                err.decode(decoder, originalContext.getCapabilities());
                final String errStr = originalContext.decodeStringResults(err.getErrorMessage());
                originalContext.setLastError(
                    "Auth fail. Error code: " + err.getErrorCode()
                        + ", Error message: " + (null == errStr ? "<null>" : errStr));
                originalContext.setState(MysqlClientState.Closed);
            }
            break;

            default:
                originalContext.setLastError("Unknown auth result pkt type: " + decoder.peek_s());
                originalContext.setState(MysqlClientState.Closed);
                break;
            }
        }
        return false; // not taken
    }
}
