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

package com.alibaba.polardbx.proxy.net;

import com.alibaba.polardbx.proxy.common.ThreadNames;
import com.alibaba.polardbx.proxy.utils.Kill;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class NIOWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOWorker.class);

    @Getter
    private final NIOProcessor[] processors;
    private final AtomicInteger index = new AtomicInteger(0);

    private static int MAX_THREADS = 32;
    private static long MAX_BUF_SIZE = 256 * 1024 * 1024; // 256MB

    static {
        if (System.getenv("cpu_cores") != null) {
            try {
                MAX_THREADS = Integer.parseInt(System.getenv("cpu_cores")) * 2; // Fix this when have env.
            } catch (Throwable e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        try {
            final long max = Runtime.getRuntime().maxMemory();
            long sz = 1024 * 1024;
            while (sz * 20 < max) {
                sz <<= 1;
            }
            MAX_BUF_SIZE = sz; // Use at most 10% heap size of direct buffer.
        } catch (Throwable e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    public NIOWorker(int threadNumber) {
        if (threadNumber > MAX_THREADS) {
            threadNumber = MAX_THREADS;
        }
        long bufNumPerThread = MAX_BUF_SIZE / threadNumber / NIOProcessor.DEFAULT_BLOCK_SIZE;
        if (bufNumPerThread > NIOProcessor.DEFAULT_BLOCK_NUMBER) {
            bufNumPerThread = NIOProcessor.DEFAULT_BLOCK_NUMBER;
        }
        LOGGER.info("NIOWorker start with {} processors and {} MB buf per processor.",
            threadNumber, bufNumPerThread * NIOProcessor.DEFAULT_BLOCK_SIZE / 1024 / 1024.f);
        processors = new NIOProcessor[threadNumber];
        try {
            for (int i = 0; i < threadNumber; ++i) {
                processors[i] = new NIOProcessor(
                    ThreadNames.NIO_PROCESSOR + "-" + i, NIOProcessor.DEFAULT_BLOCK_SIZE, (int) bufNumPerThread);
                processors[i].start();
            }
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            Kill.kill9();
        }
    }

    public NIOProcessor getProcessor() {
        int idx = index.getAndIncrement() % processors.length;
        if (idx < 0) {
            idx += processors.length;
        }
        return processors[idx];
    }

    @Override
    public String toString() {
        return "NIOWorker{" +
            "processorCount=" + processors.length +
            '}';
    }
}
