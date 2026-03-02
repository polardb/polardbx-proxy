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

import com.alibaba.polardbx.proxy.perf.ReactorPerfItem;
import com.alibaba.polardbx.proxy.utils.FastBufferPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class NIOProcessorTest {

    private NIOProcessor processor;

    @Before
    public void setUp() throws IOException {
        processor = new NIOProcessor("test-processor");
        processor.start();
    }

    @After
    public void tearDown() {
        // Processor is daemon thread, will be stopped when JVM exits
    }

    @Test
    public void testConstructorDefault() throws IOException {
        NIOProcessor p = new NIOProcessor("default-processor");
        assertNotNull(p);
        assertEquals("default-processor", p.getName());
        assertTrue(p.isDaemon());
        
        FastBufferPool pool = p.getBufferPool();
        assertNotNull(pool);
        assertEquals(NIOProcessor.DEFAULT_BLOCK_SIZE, pool.getBlockSize());
    }

    @Test
    public void testConstructorWithCustomParameters() throws IOException {
        int blockSize = 4096;
        int blockNumber = 100;
        NIOProcessor p = new NIOProcessor("custom-processor", blockSize, blockNumber);
        assertNotNull(p);
        
        FastBufferPool pool = p.getBufferPool();
        assertNotNull(pool);
        assertEquals(blockSize, pool.getBlockSize());
        assertEquals(blockSize * blockNumber, pool.capacity());
    }

    @Test
    public void testBufferPoolNotNull() {
        assertNotNull(processor.getBufferPool());
    }

    @Test
    public void testPerfCollectionNotNull() {
        assertNotNull(processor.getPerfCollection());
    }

    @Test
    public void testGetPerfItem() {
        ReactorPerfItem item = processor.getPerfItem();
        
        assertNotNull(item);
        assertEquals("test-processor", item.getName());
        assertTrue(item.getBufferSize() > 0);
        assertEquals(NIOProcessor.DEFAULT_BLOCK_SIZE, item.getBufferBlockSize());
    }

    @Test
    public void testIsDaemonThread() {
        assertTrue(processor.isDaemon());
    }

    @Test
    public void testIsAliveAfterStart() {
        assertTrue(processor.isAlive());
    }

    @Test
    public void testToString() {
        String str = processor.toString();
        
        assertNotNull(str);
        assertTrue(str.contains("NIOProcessor"));
        assertTrue(str.contains("test-processor"));
        assertTrue(str.contains("bufferPool"));
    }

    @Test
    public void testDefaultBlockSize() {
        assertEquals(8 * 1024, NIOProcessor.DEFAULT_BLOCK_SIZE);
    }

    @Test
    public void testDefaultBlockNumber() {
        assertEquals(2048, NIOProcessor.DEFAULT_BLOCK_NUMBER);
    }

    @Test
    public void testPerfCollectionInitialValues() {
        // Create a fresh processor for clean perf collection
        try {
            NIOProcessor freshProcessor = new NIOProcessor("fresh-processor");
            
            assertEquals(0, freshProcessor.getPerfCollection().getSocketCount().get());
            assertEquals(0, freshProcessor.getPerfCollection().getRegisterCount().get());
            assertEquals(0, freshProcessor.getPerfCollection().getReadCount().get());
            assertEquals(0, freshProcessor.getPerfCollection().getWriteCount().get());
            assertEquals(0, freshProcessor.getPerfCollection().getConnectCount().get());
        } catch (IOException e) {
            fail("Should not throw IOException: " + e.getMessage());
        }
    }

    @Test
    public void testBufferPoolAllocation() {
        FastBufferPool pool = processor.getBufferPool();
        
        // Test allocation
        FastBufferPool.BufferHolder holder = pool.allocateAndAddReference();
        assertNotNull(holder);
        assertEquals(NIOProcessor.DEFAULT_BLOCK_SIZE, holder.size());
        
        // Clean up
        holder.subReference();
    }

    @Test
    public void testMultipleProcessorsIndependent() throws IOException {
        NIOProcessor p1 = new NIOProcessor("p1");
        NIOProcessor p2 = new NIOProcessor("p2");
        
        assertNotSame(p1.getBufferPool(), p2.getBufferPool());
        assertNotSame(p1.getPerfCollection(), p2.getPerfCollection());
    }
}
