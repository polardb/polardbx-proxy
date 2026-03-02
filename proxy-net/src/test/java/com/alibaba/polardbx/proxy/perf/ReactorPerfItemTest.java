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

import static org.junit.Assert.*;

public class ReactorPerfItemTest {

    private ReactorPerfItem item;

    @Before
    public void setUp() {
        item = new ReactorPerfItem();
    }

    // -------------------------------------------------------------------------
    // 默认初始值测试（所有数值字段默认为 0，String 字段默认为 null）
    // -------------------------------------------------------------------------

    @Test
    public void testDefaultNameIsNull() {
        assertNull(item.getName());
    }

    @Test
    public void testDefaultSocketCountIsZero() {
        assertEquals(0L, item.getSocketCount());
    }

    @Test
    public void testDefaultEventLoopCountIsZero() {
        assertEquals(0L, item.getEventLoopCount());
    }

    @Test
    public void testDefaultRegisterCountIsZero() {
        assertEquals(0L, item.getRegisterCount());
    }

    @Test
    public void testDefaultReadCountIsZero() {
        assertEquals(0L, item.getReadCount());
    }

    @Test
    public void testDefaultWriteCountIsZero() {
        assertEquals(0L, item.getWriteCount());
    }

    @Test
    public void testDefaultConnectCountIsZero() {
        assertEquals(0L, item.getConnectCount());
    }

    @Test
    public void testDefaultBufferSizeIsZero() {
        assertEquals(0L, item.getBufferSize());
    }

    @Test
    public void testDefaultBufferBlockSizeIsZero() {
        assertEquals(0L, item.getBufferBlockSize());
    }

    @Test
    public void testDefaultIdleBufferCountIsZero() {
        assertEquals(0L, item.getIdleBufferCount());
    }

    // -------------------------------------------------------------------------
    // setter / getter 正确性测试
    // -------------------------------------------------------------------------

    @Test
    public void testSetAndGetName() {
        item.setName("reactor-0");
        assertEquals("reactor-0", item.getName());
    }

    @Test
    public void testSetAndGetSocketCount() {
        item.setSocketCount(100L);
        assertEquals(100L, item.getSocketCount());
    }

    @Test
    public void testSetAndGetEventLoopCount() {
        item.setEventLoopCount(200L);
        assertEquals(200L, item.getEventLoopCount());
    }

    @Test
    public void testSetAndGetRegisterCount() {
        item.setRegisterCount(300L);
        assertEquals(300L, item.getRegisterCount());
    }

    @Test
    public void testSetAndGetReadCount() {
        item.setReadCount(400L);
        assertEquals(400L, item.getReadCount());
    }

    @Test
    public void testSetAndGetWriteCount() {
        item.setWriteCount(500L);
        assertEquals(500L, item.getWriteCount());
    }

    @Test
    public void testSetAndGetConnectCount() {
        item.setConnectCount(600L);
        assertEquals(600L, item.getConnectCount());
    }

    @Test
    public void testSetAndGetBufferSize() {
        item.setBufferSize(1024L * 1024L);
        assertEquals(1024L * 1024L, item.getBufferSize());
    }

    @Test
    public void testSetAndGetBufferBlockSize() {
        item.setBufferBlockSize(8192L);
        assertEquals(8192L, item.getBufferBlockSize());
    }

    @Test
    public void testSetAndGetIdleBufferCount() {
        item.setIdleBufferCount(128L);
        assertEquals(128L, item.getIdleBufferCount());
    }

    // -------------------------------------------------------------------------
    // 字段独立性测试（设置一个字段不影响其他字段）
    // -------------------------------------------------------------------------

    @Test
    public void testFieldsAreIndependent() {
        item.setSocketCount(10L);
        item.setReadCount(20L);
        item.setBufferSize(30L);

        assertEquals(10L, item.getSocketCount());
        assertEquals(0L, item.getEventLoopCount());
        assertEquals(0L, item.getRegisterCount());
        assertEquals(20L, item.getReadCount());
        assertEquals(0L, item.getWriteCount());
        assertEquals(0L, item.getConnectCount());
        assertEquals(30L, item.getBufferSize());
        assertEquals(0L, item.getBufferBlockSize());
        assertEquals(0L, item.getIdleBufferCount());
    }

    // -------------------------------------------------------------------------
    // 边界值测试
    // -------------------------------------------------------------------------

    @Test
    public void testSetMaxLongValues() {
        item.setSocketCount(Long.MAX_VALUE);
        item.setEventLoopCount(Long.MAX_VALUE);
        item.setRegisterCount(Long.MAX_VALUE);
        item.setReadCount(Long.MAX_VALUE);
        item.setWriteCount(Long.MAX_VALUE);
        item.setConnectCount(Long.MAX_VALUE);
        item.setBufferSize(Long.MAX_VALUE);
        item.setBufferBlockSize(Long.MAX_VALUE);
        item.setIdleBufferCount(Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, item.getSocketCount());
        assertEquals(Long.MAX_VALUE, item.getEventLoopCount());
        assertEquals(Long.MAX_VALUE, item.getRegisterCount());
        assertEquals(Long.MAX_VALUE, item.getReadCount());
        assertEquals(Long.MAX_VALUE, item.getWriteCount());
        assertEquals(Long.MAX_VALUE, item.getConnectCount());
        assertEquals(Long.MAX_VALUE, item.getBufferSize());
        assertEquals(Long.MAX_VALUE, item.getBufferBlockSize());
        assertEquals(Long.MAX_VALUE, item.getIdleBufferCount());
    }

    @Test
    public void testSetMinLongValues() {
        item.setSocketCount(Long.MIN_VALUE);
        item.setBufferSize(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, item.getSocketCount());
        assertEquals(Long.MIN_VALUE, item.getBufferSize());
    }

    @Test
    public void testSetNegativeCounters() {
        item.setReadCount(-1L);
        item.setWriteCount(-100L);
        assertEquals(-1L, item.getReadCount());
        assertEquals(-100L, item.getWriteCount());
    }

    // -------------------------------------------------------------------------
    // name 字段特殊值测试
    // -------------------------------------------------------------------------

    @Test
    public void testSetNameToNull() {
        item.setName("some-name");
        item.setName(null);
        assertNull(item.getName());
    }

    @Test
    public void testSetNameToEmptyString() {
        item.setName("");
        assertEquals("", item.getName());
    }

    @Test
    public void testSetNameWithSpecialCharacters() {
        String specialName = "reactor-0_#$%^&*()";
        item.setName(specialName);
        assertEquals(specialName, item.getName());
    }

    @Test
    public void testSetNameWithUnicode() {
        item.setName("反应堆-0");
        assertEquals("反应堆-0", item.getName());
    }

    // -------------------------------------------------------------------------
    // bufferBlockSize 除零风险校验（文档化已知风险）
    // -------------------------------------------------------------------------

    /**
     * 说明：ShowReactorHandler 中存在 bufferSize / bufferBlockSize 的计算。
     * 当 bufferBlockSize 为 0（默认值）时，会触发 ArithmeticException。
     * ReactorPerfItem 本身不做防护，调用方需保证 bufferBlockSize > 0。
     */
    @Test
    public void testBufferBlockSizeDefaultIsZero_divisionRisk() {
        ReactorPerfItem fresh = new ReactorPerfItem();
        assertEquals(0L, fresh.getBufferBlockSize());
        // 记录风险：若调用方直接计算 bufferSize / bufferBlockSize 将抛出 ArithmeticException
        try {
            long result = fresh.getBufferSize() / fresh.getBufferBlockSize();
            fail("Expected ArithmeticException due to division by zero, but got: " + result);
        } catch (ArithmeticException e) {
            // 预期异常，说明调用方需要防御性检查
        }
    }

    @Test
    public void testBufferBlockSizeNonZeroDoesNotThrow() {
        item.setBufferSize(1024L * 1024L);
        item.setBufferBlockSize(8192L);
        // 不应抛出异常
        long totalBlocks = item.getBufferSize() / item.getBufferBlockSize();
        assertEquals(128L, totalBlocks);
    }

    // -------------------------------------------------------------------------
    // 多次 set 覆盖测试
    // -------------------------------------------------------------------------

    @Test
    public void testSetOverwritesPreviousValue() {
        item.setSocketCount(1L);
        item.setSocketCount(999L);
        assertEquals(999L, item.getSocketCount());
    }

    @Test
    public void testSetNameOverwrite() {
        item.setName("first");
        item.setName("second");
        assertEquals("second", item.getName());
    }

    // -------------------------------------------------------------------------
    // 两个实例相互独立
    // -------------------------------------------------------------------------

    @Test
    public void testTwoInstancesAreIndependent() {
        ReactorPerfItem other = new ReactorPerfItem();
        item.setSocketCount(42L);
        item.setName("item-a");

        assertEquals(42L, item.getSocketCount());
        assertEquals(0L, other.getSocketCount());
        assertNull(other.getName());
    }

    // -------------------------------------------------------------------------
    // 模拟 NIOProcessor.getPerfItem() 的赋值场景
    // -------------------------------------------------------------------------

    @Test
    public void testSimulateGetPerfItemAssignment() {
        item.setName("worker-1");
        item.setSocketCount(5L);
        item.setEventLoopCount(1000L);
        item.setRegisterCount(5L);
        item.setReadCount(2000L);
        item.setWriteCount(1800L);
        item.setConnectCount(3L);
        item.setBufferSize(16L * 1024L * 1024L);
        item.setBufferBlockSize(8192L);
        item.setIdleBufferCount(1900L);

        assertEquals("worker-1", item.getName());
        assertEquals(5L, item.getSocketCount());
        assertEquals(1000L, item.getEventLoopCount());
        assertEquals(5L, item.getRegisterCount());
        assertEquals(2000L, item.getReadCount());
        assertEquals(1800L, item.getWriteCount());
        assertEquals(3L, item.getConnectCount());
        assertEquals(16L * 1024L * 1024L, item.getBufferSize());
        assertEquals(8192L, item.getBufferBlockSize());
        assertEquals(1900L, item.getIdleBufferCount());

        // 验证 totalBlocks 计算
        long totalBlocks = item.getBufferSize() / item.getBufferBlockSize();
        assertEquals(2048L, totalBlocks);
    }
}
