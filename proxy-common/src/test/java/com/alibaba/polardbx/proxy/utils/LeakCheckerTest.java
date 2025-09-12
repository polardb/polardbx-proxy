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

import com.alibaba.polardbx.proxy.config.FastConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class LeakCheckerTest {
    private static class Resource extends LeakChecker {
        private final AtomicInteger freeCount;
        public long val = 0;

        private static final class MyLeakCallback implements LeakCallback {
            private final AtomicInteger leakCount;

            public MyLeakCallback(AtomicInteger leakCount) {
                this.leakCount = leakCount;
            }

            @Override
            public void setTag(String tag) {
            }

            @Override
            public void run() {
                leakCount.getAndIncrement();
            }
        }

        public Resource(final AtomicInteger leakCount, final AtomicInteger freeCount) {
            super(new MyLeakCallback(leakCount));
            this.freeCount = freeCount;
        }

        @Override
        public void close() {
            freeCount.incrementAndGet();
            // finalize the leak check
            super.close();
        }
    }

    private void leak(AtomicInteger leakCount, AtomicInteger freeCount) {
        final Resource resource = new Resource(leakCount, freeCount); // leak it
        resource.val = System.nanoTime();
    }

    private void notLeak(AtomicInteger leakCount, AtomicInteger freeCount) {
        try (final Resource resource = new Resource(leakCount, freeCount)) {
            resource.val = System.nanoTime();
        }
    }

    @Before
    public void setUp() {
        FastConfig.enableLeakCheck = true;
    }

    @Test
    public void testLeak() throws Exception {
        final AtomicInteger leakCount = new AtomicInteger(0);
        final AtomicInteger freeCount = new AtomicInteger(0);
        leak(leakCount, freeCount);

        System.gc();
        Thread.sleep(100);
        Assert.assertEquals(1, leakCount.get());
        Assert.assertEquals(0, freeCount.get());
    }

    @Test
    public void testNotLeak() throws Exception {
        final AtomicInteger leakCount = new AtomicInteger(0);
        final AtomicInteger freeCount = new AtomicInteger(0);
        notLeak(leakCount, freeCount);

        System.gc();
        Thread.sleep(100);
        Assert.assertEquals(0, leakCount.get());
        Assert.assertEquals(1, freeCount.get());
    }
}
