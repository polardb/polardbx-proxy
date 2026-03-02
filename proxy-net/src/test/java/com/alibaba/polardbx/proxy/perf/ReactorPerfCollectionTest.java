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

package com.alibaba.polardbx.proxy.perf;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class ReactorPerfCollectionTest {

    private ReactorPerfCollection collection;

    @Before
    public void setUp() {
        collection = new ReactorPerfCollection();
    }

    // -------------------------------------------------------------------------
    // 初始值测试
    // -------------------------------------------------------------------------

    @Test
    public void testInitialSocketCountIsZero() {
        assertEquals(0L, collection.getSocketCount().get());
    }

    @Test
    public void testInitialEventLoopCountIsZero() {
        assertEquals(0L, collection.getEventLoopCount().get());
    }

    @Test
    public void testInitialRegisterCountIsZero() {
        assertEquals(0L, collection.getRegisterCount().get());
    }

    @Test
    public void testInitialReadCountIsZero() {
        assertEquals(0L, collection.getReadCount().get());
    }

    @Test
    public void testInitialWriteCountIsZero() {
        assertEquals(0L, collection.getWriteCount().get());
    }

    @Test
    public void testInitialConnectCountIsZero() {
        assertEquals(0L, collection.getConnectCount().get());
    }

    // -------------------------------------------------------------------------
    // getter 返回 AtomicLong 非空测试
    // -------------------------------------------------------------------------

    @Test
    public void testGettersReturnNonNullAtomicLong() {
        assertNotNull(collection.getSocketCount());
        assertNotNull(collection.getEventLoopCount());
        assertNotNull(collection.getRegisterCount());
        assertNotNull(collection.getReadCount());
        assertNotNull(collection.getWriteCount());
        assertNotNull(collection.getConnectCount());
    }

    // -------------------------------------------------------------------------
    // 单次递增测试
    // -------------------------------------------------------------------------

    @Test
    public void testIncrementSocketCount() {
        collection.getSocketCount().incrementAndGet();
        assertEquals(1L, collection.getSocketCount().get());
    }

    @Test
    public void testIncrementEventLoopCount() {
        collection.getEventLoopCount().incrementAndGet();
        assertEquals(1L, collection.getEventLoopCount().get());
    }

    @Test
    public void testIncrementRegisterCount() {
        collection.getRegisterCount().incrementAndGet();
        assertEquals(1L, collection.getRegisterCount().get());
    }

    @Test
    public void testIncrementReadCount() {
        collection.getReadCount().incrementAndGet();
        assertEquals(1L, collection.getReadCount().get());
    }

    @Test
    public void testIncrementWriteCount() {
        collection.getWriteCount().incrementAndGet();
        assertEquals(1L, collection.getWriteCount().get());
    }

    @Test
    public void testIncrementConnectCount() {
        collection.getConnectCount().incrementAndGet();
        assertEquals(1L, collection.getConnectCount().get());
    }

    // -------------------------------------------------------------------------
    // 多次递增测试
    // -------------------------------------------------------------------------

    @Test
    public void testMultipleIncrementsSocketCount() {
        for (int i = 0; i < 100; i++) {
            collection.getSocketCount().incrementAndGet();
        }
        assertEquals(100L, collection.getSocketCount().get());
    }

    @Test
    public void testMultipleIncrementsAllCounters() {
        final int times = 50;
        for (int i = 0; i < times; i++) {
            collection.getSocketCount().incrementAndGet();
            collection.getEventLoopCount().incrementAndGet();
            collection.getRegisterCount().incrementAndGet();
            collection.getReadCount().incrementAndGet();
            collection.getWriteCount().incrementAndGet();
            collection.getConnectCount().incrementAndGet();
        }
        assertEquals(times, collection.getSocketCount().get());
        assertEquals(times, collection.getEventLoopCount().get());
        assertEquals(times, collection.getRegisterCount().get());
        assertEquals(times, collection.getReadCount().get());
        assertEquals(times, collection.getWriteCount().get());
        assertEquals(times, collection.getConnectCount().get());
    }

    // -------------------------------------------------------------------------
    // 各计数器独立性测试（修改一个不影响其他）
    // -------------------------------------------------------------------------

    @Test
    public void testCountersAreIndependent() {
        collection.getSocketCount().set(10);
        collection.getReadCount().set(20);

        assertEquals(10L, collection.getSocketCount().get());
        assertEquals(0L, collection.getEventLoopCount().get());
        assertEquals(0L, collection.getRegisterCount().get());
        assertEquals(20L, collection.getReadCount().get());
        assertEquals(0L, collection.getWriteCount().get());
        assertEquals(0L, collection.getConnectCount().get());
    }

    // -------------------------------------------------------------------------
    // getAndIncrement 语义测试（与 NIOProcessor 实际调用方式一致）
    // -------------------------------------------------------------------------

    @Test
    public void testGetAndIncrementReturnsOldValue() {
        collection.getRegisterCount().set(5L);
        long prev = collection.getRegisterCount().getAndIncrement();
        assertEquals(5L, prev);
        assertEquals(6L, collection.getRegisterCount().get());
    }

    @Test
    public void testGetAcquireReadsLatestValue() {
        collection.getEventLoopCount().set(42L);
        assertEquals(42L, collection.getEventLoopCount().getAcquire());
    }

    // -------------------------------------------------------------------------
    // 并发递增正确性测试
    // -------------------------------------------------------------------------

    @Test
    public void testConcurrentIncrementSocketCount() throws InterruptedException {
        final int threadCount = 10;
        final int incrementsPerThread = 1000;
        final CountDownLatch latch = new CountDownLatch(threadCount);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            threads.add(new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    collection.getSocketCount().incrementAndGet();
                }
                latch.countDown();
            }));
        }
        threads.forEach(Thread::start);
        latch.await();

        assertEquals((long) threadCount * incrementsPerThread, collection.getSocketCount().get());
    }

    @Test
    public void testConcurrentIncrementAllCounters() throws InterruptedException {
        final int threadCount = 5;
        final int incrementsPerThread = 200;
        final CountDownLatch latch = new CountDownLatch(threadCount);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            threads.add(new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    collection.getReadCount().incrementAndGet();
                    collection.getWriteCount().incrementAndGet();
                    collection.getConnectCount().incrementAndGet();
                }
                latch.countDown();
            }));
        }
        threads.forEach(Thread::start);
        latch.await();

        long expected = (long) threadCount * incrementsPerThread;
        assertEquals(expected, collection.getReadCount().get());
        assertEquals(expected, collection.getWriteCount().get());
        assertEquals(expected, collection.getConnectCount().get());
    }

    // -------------------------------------------------------------------------
    // 大数值测试
    // -------------------------------------------------------------------------

    @Test
    public void testSetLargeValue() {
        collection.getEventLoopCount().set(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, collection.getEventLoopCount().get());
    }

    @Test
    public void testSetNegativeValue() {
        // AtomicLong 本身支持负数，计数器理论上不应为负，但接口层面不做限制
        collection.getSocketCount().set(-1L);
        assertEquals(-1L, collection.getSocketCount().get());
    }

    // -------------------------------------------------------------------------
    // 每个实例拥有独立的计数器对象
    // -------------------------------------------------------------------------

    @Test
    public void testTwoInstancesAreIndependent() {
        ReactorPerfCollection other = new ReactorPerfCollection();
        collection.getSocketCount().set(99L);

        assertEquals(99L, collection.getSocketCount().get());
        assertEquals(0L, other.getSocketCount().get());
        assertNotSame(collection.getSocketCount(), other.getSocketCount());
    }

    // -------------------------------------------------------------------------
    // AtomicLong 引用稳定性（多次 getter 返回同一对象）
    // -------------------------------------------------------------------------

    @Test
    public void testGetterReturnsSameAtomicLongInstance() {
        AtomicLong ref1 = collection.getSocketCount();
        AtomicLong ref2 = collection.getSocketCount();
        assertSame(ref1, ref2);
    }
}
