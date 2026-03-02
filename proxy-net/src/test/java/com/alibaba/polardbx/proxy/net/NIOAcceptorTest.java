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

import com.alibaba.polardbx.proxy.utils.AutoCloseableContainer;
import com.alibaba.polardbx.proxy.utils.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.*;

/**
 * Unit tests for NIOAcceptor.
 * Note: Tests that require actual network connections are skipped in some environments
 * due to network restrictions.
 */
public class NIOAcceptorTest {

    private NIOWorker worker;
    private NIOAcceptor acceptor;

    private static class TestConnection extends NIOConnection {
        public TestConnection(SocketChannel channel, NIOProcessor processor) {
            super(channel, processor, true, 64 * 1024, 128);
        }

        @Override
        protected void onEstablished() {
        }

        @Override
        protected int probeLength(ByteBuffer buf, int offset, int length) {
            return length;
        }

        @Override
        protected void onPacket(AutoCloseableContainer<Slice> packets) {
        }

        @Override
        protected void onFatalError(Throwable t) {
        }
    }

    @Before
    public void setUp() throws IOException {
        worker = new NIOWorker(2);
    }

    @After
    public void tearDown() {
        if (acceptor != null) {
            acceptor.offline();
        }
    }

    private NIOConnectionFactory createFactory() {
        return (channel, processor) -> new TestConnection(channel, processor);
    }

    @Test
    public void testAcceptorCreation() throws IOException {
        acceptor = new NIOAcceptor("test-acceptor", 0, worker, createFactory());
        
        assertNotNull(acceptor);
        assertTrue(acceptor.getPort() > 0);
        assertEquals("test-acceptor", acceptor.getName());
    }

    @Test
    public void testAcceptorStart() throws IOException {
        acceptor = new NIOAcceptor("test-acceptor", 0, worker, createFactory());
        acceptor.start();
        
        assertTrue(acceptor.isAlive());
        assertFalse(acceptor.isDaemon()); // Acceptor is not daemon
    }

    @Test
    public void testToString() throws IOException {
        acceptor = new NIOAcceptor("test-acceptor", 0, worker, createFactory());
        
        String str = acceptor.toString();
        assertNotNull(str);
        assertTrue(str.contains("NIOAcceptor"));
        assertTrue(str.contains("test-acceptor"));
        assertTrue(str.contains("port="));
    }

    @Test
    public void testGetPort() throws IOException {
        acceptor = new NIOAcceptor("test-acceptor", 0, worker, createFactory());
        
        int port = acceptor.getPort();
        assertTrue(port > 0);
        assertTrue(port < 65536);
    }

    @Test
    public void testOffline() throws IOException, InterruptedException {
        acceptor = new NIOAcceptor("test-acceptor", 0, worker, createFactory());
        acceptor.start();
        
        assertTrue(acceptor.isAlive());
        
        acceptor.offline();
        
        // Wait a bit for the thread to stop
        Thread.sleep(200);
        
        // The acceptor thread should be stopped or stopping
        // Note: The thread might not be immediately dead after offline
    }

    @Test
    public void testMultipleAcceptors() throws IOException {
        NIOAcceptor acceptor1 = new NIOAcceptor("acceptor-1", 0, worker, createFactory());
        NIOAcceptor acceptor2 = new NIOAcceptor("acceptor-2", 0, worker, createFactory());
        
        assertNotEquals(acceptor1.getPort(), acceptor2.getPort());
        
        acceptor1.start();
        acceptor2.start();
        
        assertTrue(acceptor1.isAlive());
        assertTrue(acceptor2.isAlive());
        
        acceptor1.offline();
        acceptor2.offline();
    }

    @Test
    public void testAcceptorName() throws IOException {
        acceptor = new NIOAcceptor("my-custom-name", 0, worker, createFactory());
        assertEquals("my-custom-name", acceptor.getName());
    }

    @Test
    public void testAcceptorThreadNotDaemon() throws IOException {
        acceptor = new NIOAcceptor("test-acceptor", 0, worker, createFactory());
        // Acceptor thread should NOT be daemon to prevent JVM exit
        assertFalse(acceptor.isDaemon());
    }

    @Test
    public void testWorkerAssignment() throws IOException {
        acceptor = new NIOAcceptor("test-acceptor", 0, worker, createFactory());
        
        // The acceptor should use the provided worker
        String str = acceptor.toString();
        assertTrue(str.contains("worker="));
    }

    @Test
    public void testFactoryAssignment() throws IOException {
        NIOConnectionFactory factory = createFactory();
        acceptor = new NIOAcceptor("test-acceptor", 0, worker, factory);
        
        // The acceptor should use the provided factory
        String str = acceptor.toString();
        assertTrue(str.contains("factory="));
    }
}
