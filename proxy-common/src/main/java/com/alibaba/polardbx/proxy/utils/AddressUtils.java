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

package com.alibaba.polardbx.proxy.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.regex.Pattern;

public class AddressUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddressUtils.class);
    private static final String LOCALHOST_IP = "127.0.0.1";
    private static final String EMPTY_IP = "0.0.0.0";
    private static final Pattern IP_PATTERN = Pattern.compile("[0-9]{1,3}(\\.[0-9]{1,3}){3,}");

    private static boolean isValidHostAddress(final InetAddress address) {
        if (null == address || address.isLoopbackAddress()) {
            return false;
        }
        final String name = address.getHostAddress();
        return (name != null && !EMPTY_IP.equals(name) && !LOCALHOST_IP.equals(name) && IP_PATTERN.matcher(name)
            .matches());
    }

    public static String getHostIp() {
        final InetAddress address = getHostAddress();
        return address == null ? null : address.getHostAddress();
    }

    public static String getHostName() {
        final InetAddress address = getHostAddress();
        return address == null ? null : address.getHostName();
    }

    public static InetAddress localAddress = null;

    public static InetAddress getHostAddress() {
        if (localAddress != null) {
            return localAddress;
        }

        try {
            localAddress = InetAddress.getLocalHost();
            if (isValidHostAddress(localAddress)) {
                return localAddress;
            }
        } catch (Throwable e) {
            LOGGER.warn("Failed to retrieving local host ip address, try scan network card ip address.", e);
        }

        try {
            final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                try {
                    final Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        try {
                            final InetAddress address = addresses.nextElement();
                            if (isValidHostAddress(address)) {
                                localAddress = address;
                                return address;
                            }
                        } catch (Throwable e) {
                            LOGGER.warn("Failed to retrieving network card ip address.", e);
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.warn("Failed to retrieving network card ip address.", e);
                }
            }
        } catch (Throwable e) {
            LOGGER.warn("Failed to retrieving network card ip address.", e);
        }
        LOGGER.error("Could not get local host ip address, will use 127.0.0.1 instead.");
        return localAddress;
    }
}
