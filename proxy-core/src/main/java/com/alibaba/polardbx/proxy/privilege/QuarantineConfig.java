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

package com.alibaba.polardbx.proxy.privilege;

public class QuarantineConfig {
    static int addressItem(String orig, String item) throws IllegalArgumentException {
        int result = Integer.parseInt(item);
        if (result < 0 || result > 255) {
            throw new IllegalArgumentException("Address: " + orig + " is not valid");
        }
        return result;
    }

    static public long ip2long(String ip) {
        String[] ips = ip.split("\\.");
        long result = 0;
        for (int i = 0; i < ips.length; i++) {
            result += (long) addressItem(ip, ips[i]) << ((3 - i) * 8);
        }
        return result;
    }

    static class Netmask {
        private long startAddr;
        private int submask;

        // dealing % or 10.20.0.0/24 or 10.20.0.0/255.255.255.0 or 10.20.%.% or 10.20.0.0
        public Netmask(String inputmask) throws IllegalArgumentException {
            if (null == inputmask || inputmask.isEmpty()) {
                throw new IllegalArgumentException("Input maks is empty!");
            }

            inputmask = inputmask.trim();
            if (inputmask.equals("%")) {
                startAddr = 0;
                submask = 0;
            } else if (inputmask.contains("/")) {
                // format is 10.20.0.0/24 or 10.20.0.0/255.255.255.0
                String cidrIp = inputmask.replaceAll("/.*", "");
                startAddr = ip2long(cidrIp);
                final String mask = inputmask.replaceAll(".*/", "");
                if (mask.contains(".")) {
                    submask = (int) ip2long(mask);
                } else {
                    final int type = Integer.parseInt(mask);
                    submask = 0xFFFFFFFF << (32 - type);
                }
            } else if (inputmask.contains("%")) {
                // format is 10.20.%.%
                submask = 0xFFFFFFFF;
                String[] fourPlay = inputmask.split("\\.");
                for (int i = 0; i < fourPlay.length; i++) {
                    if (fourPlay[i].equals("%")) {
                        submask &= ~(0xFF << (3 - i) * 8);
                    } else {
                        startAddr += (long) addressItem(inputmask, fourPlay[i]) << ((3 - i) * 8);
                    }
                }
            } else {
                startAddr = ip2long(inputmask);
                submask = 0xFFFFFFFF;
            }
        }

        public boolean isInScope(long ip) {
            return (ip & submask) == (startAddr & submask);
        }
    }
}
