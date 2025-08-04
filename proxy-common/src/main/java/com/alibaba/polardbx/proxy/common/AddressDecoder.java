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

package com.alibaba.polardbx.proxy.common;

import lombok.Getter;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Getter
public class AddressDecoder {
    private final String firstTag;
    // <tag, sock_addr>
    private final Map<String, InetSocketAddress> addressMap;

    public static InetSocketAddress decode(String address) {
        final String[] split = address.split(":");
        if (split.length != 2) {
            throw new IllegalArgumentException("Invalid address: " + address);
        }
        return new InetSocketAddress(split[0], Integer.parseInt(split[1]));
    }

    public AddressDecoder(String addresses) {
        if (null == addresses) {
            this.firstTag = null;
            this.addressMap = Collections.emptyMap();
            return;
        }

        String firstTag = null;
        final HashMap<String, InetSocketAddress> tmp = new HashMap<>(3);
        final String[] split = addresses.split(";");
        for (final String address : split) {
            if (null == firstTag) {
                firstTag = address;
            }
            tmp.put(address, decode(address));
        }
        this.firstTag = firstTag;
        this.addressMap = Collections.unmodifiableMap(tmp);
    }
}
