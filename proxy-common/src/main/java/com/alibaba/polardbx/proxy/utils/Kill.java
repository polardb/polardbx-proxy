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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class Kill {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kill.class);

    public static void kill9() {
        try {
            // get my pid
            final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
            final String jvmName = runtimeBean.getName();
            final long pid = Long.parseLong(jvmName.split("@")[0]);

            // invoke kill -9
            final String[] cmd = {"/bin/sh", "-c", "kill -9 " + pid};
            final Process process = Runtime.getRuntime().exec(cmd);

            // wait done
            process.waitFor();
            System.exit(-1);
            throw new RuntimeException("Should killed.");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            System.exit(-1);
            throw new RuntimeException("Should killed.");
        }
    }

    private static String stackTrace() {
        final StringBuilder builder = new StringBuilder();
        final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (int i = 2; i < stackTrace.length; i++) {
            builder.append(stackTrace[i].toString());
            if (i != stackTrace.length - 1) {
                builder.append('\n');
            }
        }
        return builder.toString();
    }

    public static void fatalError(String format, Object... args) {
        LOGGER.error("FATAL ERROR: {}\n{}", String.format(format, args), stackTrace());
        Kill.kill9();
    }
}
