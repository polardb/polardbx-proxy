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

package com.alibaba.polardbx.proxy.cluster;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FastIdGeneratorTest {

    @Test
    public void testConcurrentNextIdPerformance() throws Exception {
        // 创建FastIdGenerator实例
        FastIdGenerator idGenerator = new FastIdGenerator(1);

        // 设置并发线程数和每个线程生成的ID数量
        int threadCount = 10;
        int idsPerThread = 1000000;

        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // 创建任务列表
        List<Callable<long[]>> tasks = new ArrayList<>();

        // 记录开始时间
        long startTime = System.currentTimeMillis();

        // 创建并发任务
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            tasks.add(() -> {
                long[] ids = new long[idsPerThread];
                for (int j = 0; j < idsPerThread; j++) {
                    ids[j] = idGenerator.nextId();
                }
                return ids;
            });
        }

        // 执行所有任务
        List<Future<long[]>> futures = executor.invokeAll(tasks);

        // 记录结束时间
        long endTime = System.currentTimeMillis();

        // 关闭线程池
        executor.shutdown();

        // 收集所有生成的ID
        List<Long> allIds = new ArrayList<>(threadCount * idsPerThread);
        for (Future<long[]> future : futures) {
            long[] ids = future.get();
            for (long id : ids) {
                allIds.add(id);
            }
        }

        // 计算QPS
        long totalTimeMs = endTime - startTime;
        double qps = (double) (threadCount * idsPerThread) / (totalTimeMs / 1000.0);

        // 输出结果
        System.out.println("Concurrency Test Results:");
        System.out.println("Total IDs generated: " + allIds.size());
        System.out.println("Time taken: " + totalTimeMs + " ms");
        System.out.println("QPS: " + String.format("%.2f", qps));

        // 验证生成的ID数量是否正确
        assertTrue("Should generate " + (threadCount * idsPerThread) + " IDs",
            allIds.size() == threadCount * idsPerThread);

        // 验证ID的唯一性
        long uniqueIdCount = allIds.stream().distinct().count();
        assertEquals("All IDs should be unique", uniqueIdCount, allIds.size());
    }
}