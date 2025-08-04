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

import com.alibaba.polardbx.proxy.perf.ReactorPerfCollection;
import com.alibaba.polardbx.proxy.perf.ReactorPerfItem;
import com.alibaba.polardbx.proxy.utils.FastBufferPool;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @version 1.0
 */
public class NIOProcessor extends Thread {
    public static final int DEFAULT_BLOCK_SIZE = 1024 * 8; // 8k
    public static final int DEFAULT_BLOCK_NUMBER = 2048;

    private static final Logger LOGGER = LoggerFactory.getLogger(NIOProcessor.class);

    private final String name;
    @Getter
    private final FastBufferPool bufferPool;
    private final Selector selector;
    private final ConcurrentLinkedQueue<NIOConnection> registerQueue;

    @Getter
    private final ReactorPerfCollection perfCollection = new ReactorPerfCollection();

    public NIOProcessor(String name) throws IOException {
        this(name, DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_NUMBER);
    }

    public NIOProcessor(String name, int blockSize, int blockNumber) throws IOException {
        this.name = name;
        this.bufferPool = new FastBufferPool(blockSize, blockNumber);
        this.selector = Selector.open();
        this.registerQueue = new ConcurrentLinkedQueue<>();

        // set reactor with daemon thread
        super.setName(name);
        super.setDaemon(true);
    }

    public void postRegister(NIOConnection c) {
        registerQueue.offer(c);
        selector.wakeup();
    }

    private void register(Selector selector) {
        NIOConnection c;
        while ((c = registerQueue.poll()) != null) {
            try {
                perfCollection.getRegisterCount().getAndIncrement();
                c.register(selector);
            } catch (Throwable t) {
                LOGGER.error("{}: {}", name, t.getMessage(), t);
            }
        }
    }

    @Override
    public void run() {
        final Selector selector = this.selector;
        while (true) {
            try {
                selector.select(1000L);
                register(selector);
                final Set<SelectionKey> keys = selector.selectedKeys();
                try {
                    perfCollection.getEventLoopCount().getAndIncrement();
                    for (SelectionKey key : keys) {
                        final Object att = key.attachment();
                        if (key.isValid() && att != null) {
                            try {
                                ((NIOConnection) att).event(key.readyOps());
                            } catch (Throwable t) {
                                LOGGER.error("{}: {}", name, t.getMessage(), t);
                                key.cancel();
                            }
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Throwable t) {
                LOGGER.error("{}: {}", name, t.getMessage(), t);
            }
        }
    }

    public ReactorPerfItem getPerfItem() {
        final ReactorPerfItem item = new ReactorPerfItem();

        item.setName(name);
        item.setSocketCount(perfCollection.getSocketCount().getAcquire());
        item.setEventLoopCount(perfCollection.getEventLoopCount().getAcquire());
        item.setRegisterCount(perfCollection.getRegisterCount().getAcquire());
        item.setReadCount(perfCollection.getReadCount().getAcquire());
        item.setWriteCount(perfCollection.getWriteCount().getAcquire());
        item.setConnectCount(perfCollection.getConnectCount().getAcquire());

        item.setBufferSize(bufferPool.capacity());
        item.setBufferBlockSize(bufferPool.getBlockSize());
        item.setIdleBufferCount(bufferPool.estimatedFreeBlocks());

        return item;
    }

    @Override
    public String toString() {
        return "NIOProcessor{" +
            "name='" + name + '\'' +
            ", bufferPool=" + bufferPool +
            '}';
    }
}
