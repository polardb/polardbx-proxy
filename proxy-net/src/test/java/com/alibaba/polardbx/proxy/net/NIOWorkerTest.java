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

import org.junit.Test;

import static org.junit.Assert.*;

public class NIOWorkerTest {

    @Test
    public void testConstructorWithSmallThreadNumber() {
        NIOWorker worker = new NIOWorker(2);
        assertNotNull(worker);
        assertEquals(2, worker.getProcessors().length);
    }

    @Test
    public void testConstructorWithLargeThreadNumber() {
        // Test that thread number is capped at MAX_THREADS
        NIOWorker worker = new NIOWorker(100);
        assertNotNull(worker);
        // Should be capped, but since MAX_THREADS depends on env, just verify it's created
        assertTrue(worker.getProcessors().length > 0);
        assertTrue(worker.getProcessors().length <= 100);
    }

    @Test
    public void testGetProcessorRoundRobin() {
        NIOWorker worker = new NIOWorker(3);
        
        // Get processors multiple times and verify round-robin behavior
        NIOProcessor p0 = worker.getProcessor();
        NIOProcessor p1 = worker.getProcessor();
        NIOProcessor p2 = worker.getProcessor();
        NIOProcessor p3 = worker.getProcessor(); // Should wrap around to first
        
        assertNotNull(p0);
        assertNotNull(p1);
        assertNotNull(p2);
        assertNotNull(p3);
        
        // Verify round-robin - p3 should be same as p0
        assertEquals(p0, p3);
    }

    @Test
    public void testGetProcessorNeverReturnsNull() {
        NIOWorker worker = new NIOWorker(4);
        
        // Call getProcessor many times to test index overflow handling
        for (int i = 0; i < 1000; i++) {
            NIOProcessor processor = worker.getProcessor();
            assertNotNull("Processor should never be null at iteration " + i, processor);
        }
    }

    @Test
    public void testProcessorsAreStarted() {
        NIOWorker worker = new NIOWorker(2);
        
        for (NIOProcessor processor : worker.getProcessors()) {
            assertTrue("Processor should be alive", processor.isAlive());
            assertTrue("Processor should be daemon thread", processor.isDaemon());
        }
    }

    @Test
    public void testProcessorBufferPoolCreation() {
        NIOWorker worker = new NIOWorker(2);
        
        for (NIOProcessor processor : worker.getProcessors()) {
            assertNotNull("Buffer pool should not be null", processor.getBufferPool());
            assertTrue("Buffer pool capacity should be positive", processor.getBufferPool().capacity() > 0);
        }
    }

    @Test
    public void testToString() {
        NIOWorker worker = new NIOWorker(3);
        String str = worker.toString();
        
        assertNotNull(str);
        assertTrue(str.contains("NIOWorker"));
        assertTrue(str.contains("processorCount=3"));
    }

    @Test
    public void testSingleProcessor() {
        NIOWorker worker = new NIOWorker(1);
        
        // With single processor, should always return same processor
        NIOProcessor p1 = worker.getProcessor();
        NIOProcessor p2 = worker.getProcessor();
        NIOProcessor p3 = worker.getProcessor();
        
        assertEquals(p1, p2);
        assertEquals(p2, p3);
    }
}
