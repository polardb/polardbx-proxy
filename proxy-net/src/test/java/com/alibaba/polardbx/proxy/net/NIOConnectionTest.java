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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class NIOConnectionTest {

    private NIOProcessor processor;
    private ServerSocket serverSocket;
    private int testPort;

    @Before
    public void setUp() throws IOException {
        processor = new NIOProcessor("test-processor");
        processor.start();
        
        // Create a test server socket
        serverSocket = new ServerSocket(0);
        testPort = serverSocket.getLocalPort();
    }

    @After
    public void tearDown() throws IOException {
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
    }

    private static class TestConnection extends NIOConnection {
        private final AtomicBoolean established = new AtomicBoolean(false);
        private final AtomicReference<Throwable> fatalError = new AtomicReference<>();
        private final AtomicReference<AutoCloseableContainer<Slice>> receivedPackets = new AtomicReference<>();

        public TestConnection(SocketChannel channel, NIOProcessor processor, boolean connected) {
            super(channel, processor, connected, 64 * 1024, 128);
        }

        @Override
        protected void onEstablished() {
            established.set(true);
        }

        @Override
        protected int probeLength(ByteBuffer buf, int offset, int length) {
            // Simple protocol: first 4 bytes is length (including header)
            if (length < 4) {
                return -1;
            }
            return buf.getInt(offset);
        }

        @Override
        protected void onPacket(AutoCloseableContainer<Slice> packets) {
            receivedPackets.set(packets);
        }

        @Override
        protected void onFatalError(Throwable t) {
            fatalError.set(t);
        }

        public boolean isEstablished() {
            return established.get();
        }

        public Throwable getFatalError() {
            return fatalError.get();
        }
    }

    @Test
    public void testConnectBlocking() throws IOException {
        // Start accepting in background
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        // Connect with blocking mode
        SocketAddress address = new InetSocketAddress("127.0.0.1", testPort);
        SocketChannel channel = NIOConnection.connectBlocking(address, 5000);
        
        assertNotNull(channel);
        assertTrue(channel.isConnected());
        assertFalse(channel.isBlocking()); // Should be non-blocking after connect
        
        channel.close();
    }

    @Test(expected = IOException.class)
    public void testConnectBlockingTimeout() throws IOException {
        // Try to connect to an address that doesn't exist (should timeout)
        SocketAddress address = new InetSocketAddress("192.0.2.1", 12345); // TEST-NET-1
        NIOConnection.connectBlocking(address, 100); // Very short timeout
    }

    @Test
    public void testConnectNonBlocking() throws IOException {
        // Start accepting in background
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketAddress address = new InetSocketAddress("127.0.0.1", testPort);
        SocketChannel channel = NIOConnection.connectNonBlocking(address);
        
        assertNotNull(channel);
        assertFalse(channel.isBlocking());
        
        // In non-blocking mode, connection may still be pending
        // So we just verify the channel is open
        assertTrue(channel.isOpen());
        
        channel.close();
    }

    @Test
    public void testEnsureMinimumTcpBufferClient() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        Socket socket = channel.socket();
        
        // Test client mode (small send, large recv)
        NIOConnection.ensureMinimumTcpBuffer(socket, true);
        
        assertTrue(socket.getSendBufferSize() >= 16 * 1024);
        assertTrue(socket.getReceiveBufferSize() >= 64 * 1024);
        
        channel.close();
    }

    @Test
    public void testEnsureMinimumTcpBufferServer() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        Socket socket = channel.socket();
        
        // Test server mode (large send, small recv)
        NIOConnection.ensureMinimumTcpBuffer(socket, false);
        
        assertTrue(socket.getSendBufferSize() >= 64 * 1024);
        assertTrue(socket.getReceiveBufferSize() >= 16 * 1024);
        
        channel.close();
    }

    @Test
    public void testConnectionCreation() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        assertNotNull(connection);
        assertNotNull(connection.getProcessor());
        assertEquals(processor, connection.getProcessor());
        
        connection.close();
    }

    @Test
    public void testConnectionClose() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        // Close should be idempotent
        connection.close();
        connection.close();
        connection.close();
        
        // Should not throw any exception
    }

    @Test
    public void testIdleTime() throws IOException, InterruptedException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        long idleTime1 = connection.idleTime();
        Thread.sleep(100);
        long idleTime2 = connection.idleTime();
        
        assertTrue("Idle time should increase", idleTime2 > idleTime1);
        assertTrue("Idle time difference should be around 100ms",
            idleTime2 - idleTime1 >= 90_000_000L); // 90ms in nanos
        
        connection.close();
    }

    @Test
    public void testConnectionComparison() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel1 = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        SocketChannel channel2 = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection conn1 = new TestConnection(channel1, processor, true);
        TestConnection conn2 = new TestConnection(channel2, processor, true);
        
        // Compare should be based on internal ID
        assertNotEquals(0, conn1.compareTo(conn2));
        assertEquals(-conn1.compareTo(conn2), conn2.compareTo(conn1));
        assertEquals(0, conn1.compareTo(conn1));
        
        conn1.close();
        conn2.close();
    }

    @Test
    public void testToString() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        String str = connection.toString();
        assertNotNull(str);
        assertTrue(str.contains("NIO-Connection"));
        
        connection.close();
    }

    @Test
    public void testRemoteAddress() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        InetSocketAddress remoteAddr = connection.remoteAddress();
        assertNotNull(remoteAddr);
        assertEquals(testPort, remoteAddr.getPort());
        
        connection.close();
    }

    @Test
    public void testWriteBlocking() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        // Initially should not be write blocking
        assertFalse(connection.isWriteBlocking());
        
        connection.close();
    }

    @Test
    public void testWriteResumeListener() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        Runnable listener = () -> listenerCalled.set(true);
        
        connection.registerWriteResumeListener(listener);
        connection.removeWriteResumeListener(listener);
        
        // Just verify no exceptions
        
        connection.close();
    }

    @Test
    public void testConnectionString() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        String connStr = connection.connectionString();
        assertNotNull(connStr);
        assertTrue(connStr.contains("<->"));
        
        // Call again to test caching
        String connStr2 = connection.connectionString();
        assertEquals(connStr, connStr2);
        
        connection.close();
    }

    @Test
    public void testIsValidBeforeRegister() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        // Before registration, isValid should be false
        assertFalse(connection.isValid());
        
        connection.close();
    }

    @Test
    public void testRegisterAndEstablished() throws IOException, InterruptedException {
        final CountDownLatch acceptLatch = new CountDownLatch(1);
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
                acceptLatch.countDown();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        
        // Register with processor
        processor.postRegister(connection);
        
        // Wait for registration
        Thread.sleep(100);
        
        assertTrue(acceptLatch.await(5, TimeUnit.SECONDS));
        
        // After registration, connection should be established
        assertTrue(connection.isEstablished());
        assertTrue(connection.isValid());
        
        connection.close();
    }

    @Test
    public void testWriteWithClosedConnection() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        connection.close();
        
        // Create a simple slice
        ByteBuffer buf = ByteBuffer.allocate(10);
        Slice slice = new Slice(buf, 0, 10);
        
        try {
            connection.write(slice);
            fail("Should throw exception when writing to closed connection");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testWriteContainerWithClosedConnection() throws IOException {
        Thread acceptThread = new Thread(() -> {
            try {
                serverSocket.accept();
            } catch (IOException ignored) {
            }
        });
        acceptThread.start();

        SocketChannel channel = NIOConnection.connectBlocking(
            new InetSocketAddress("127.0.0.1", testPort), 5000);
        
        TestConnection connection = new TestConnection(channel, processor, true);
        connection.close();
        
        try (AutoCloseableContainer<Slice> container = new AutoCloseableContainer<>()) {
            ByteBuffer buf = ByteBuffer.allocate(10);
            container.add(new Slice(buf, 0, 10));
            
            try {
                connection.write(container);
                fail("Should throw exception when writing to closed connection");
            } catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("closed"));
            }
        }
    }
}
