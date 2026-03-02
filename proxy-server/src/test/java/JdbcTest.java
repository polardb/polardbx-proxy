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

import org.junit.Test;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcTest {
    private static final String BASE_URL = "jdbc:mysql://127.1:3307/test?characterEncoding=UTF-8&useSSL=false";
    private static final String USR = "polardbx_root";
    private static final String PSW = "";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void setGlobalMock(String mock) throws Exception {
        try (final Connection connection = DriverManager.getConnection(BASE_URL, USR, PSW);
            final Statement statement = connection.createStatement()) {
            statement.execute("set global mock = '" + mock + "'");
        }
    }

    private void createMyTableIfNotExists() throws Exception {
        try (final Connection connection = DriverManager.getConnection(BASE_URL, USR, PSW);
             final Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS mytable (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))");
        }
    }

    private void insertDataIfTableEmpty() throws Exception {
        createMyTableIfNotExists();
        try (final Connection connection = DriverManager.getConnection(BASE_URL, USR, PSW);
             final Statement statement = connection.createStatement();
             final ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM mytable")) {

            rs.next();
            int count = rs.getInt(1);

            if (count == 0) {
                // 插入一些测试数据
                statement.execute("INSERT INTO mytable (name) VALUES ('test1')");
                statement.execute("INSERT INTO mytable (name) VALUES ('test2')");
                statement.execute("INSERT INTO mytable (name) VALUES ('test3')");
                System.out.println("Inserted test data into mytable");
            } else {
                System.out.println("mytable already contains " + count + " rows");
            }
        }
    }

    private void createT25StreamReadTableIfNotExists() throws Exception {
        try (final Connection connection = DriverManager.getConnection(BASE_URL, USR, PSW);
             final Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS t_25_stream_read (id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(255))");
        }
    }

    private void populateT25StreamReadTableIfEmpty() throws Exception {
        createT25StreamReadTableIfNotExists();

        try (final Connection connection = DriverManager.getConnection(BASE_URL, USR, PSW);
             final Statement statement = connection.createStatement();
             final ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM t_25_stream_read")) {

            rs.next();
            int count = rs.getInt(1);

            if (count == 0) {
                System.out.println("t_25_stream_read table is empty, inserting 1,000,000 rows...");

                // 使用多线程批量插入100万行数据
                final int totalRows = 1000000;
                final int threadCount = 10;
                final int rowsPerThread = totalRows / threadCount;

                Thread[] threads = new Thread[threadCount];

                for (int i = 0; i < threadCount; i++) {
                    final int threadIndex = i;
                    threads[i] = new Thread(() -> {
                        try {
                            // 每个线程使用独立的连接
                            try (final Connection conn = DriverManager.getConnection(BASE_URL, USR, PSW);
                                 final Statement stmt = conn.createStatement()) {

                                // 手动拼接batch insert语句
                                StringBuilder batchSql = new StringBuilder("INSERT INTO t_25_stream_read (data) VALUES ");
                                int startId = threadIndex * rowsPerThread + 1;
                                int endId = (threadIndex + 1) * rowsPerThread;

                                for (int j = startId; j <= endId; j++) {
                                    if (j > startId) {
                                        batchSql.append(",");
                                    }
                                    batchSql.append("('data_row_").append(j).append("')");
                                }

                                int inserted = stmt.executeUpdate(batchSql.toString());
                                System.out.println("Thread " + threadIndex + " inserted " + inserted + " rows");
                            }
                        } catch (Exception e) {
                            System.err.println("Error in thread " + threadIndex + ": " + e.getMessage());
                            e.printStackTrace();
                        }
                    });
                }

                // 启动所有线程
                for (Thread thread : threads) {
                    thread.start();
                }

                // 等待所有线程完成
                for (Thread thread : threads) {
                    thread.join();
                }

                System.out.println("Finished inserting 1,000,000 rows into t_25_stream_read table");
            } else {
                System.out.println("t_25_stream_read already contains " + count + " rows");
            }
        }
    }

    @Test
    public void testPrepareInOtherSchema() throws Exception {
        try (final Connection connection = DriverManager.getConnection(BASE_URL + "&useServerPrepStmts=true", USR,
            PSW)) {

            try (final Statement statement = connection.createStatement()) {
                statement.execute("use test");
            }

            try (final PreparedStatement ps = connection.prepareStatement("SELECT CONCAT(?, ?)")) {
                ps.setString(1, "hello' ");
                ps.setString(2, "world!");

                // use one thread take connection
                final Thread thread = new Thread(() -> {
                    try (final Connection connection2 = DriverManager.getConnection(BASE_URL, USR, PSW)) {
                        try (final Statement statement = connection2.createStatement()) {
                            statement.execute("select sleep(1) for update");
                        }
                    } catch (Exception ignore) {
                    }
                });
                thread.start();
                Thread.sleep(100);

                try (final Statement statement = connection.createStatement()) {
                    statement.execute("use mysql");
                }

                try (ResultSet result = ps.executeQuery()) {
                    while (result.next()) {
                        String concat = result.getString(1);
                        System.out.println(concat);
                    }
                }
            }
        }
    }

    @Test
    public void testBigQuery() throws Exception {
        populateT25StreamReadTableIfEmpty();
        try (final Connection conn = DriverManager.getConnection(BASE_URL + "&useServerPrepStmts=true", USR, PSW);
            final Statement stmt = conn.createStatement()) {
            stmt.setFetchSize(Integer.MIN_VALUE);

            try (final ResultSet rs = stmt.executeQuery("select * from t_25_stream_read limit 1000000")) {
                long cnt = 0;
                while (rs.next()) {
                    cnt++;
                    if (cnt % 10000 == 0) {
                        Thread.sleep(10);
                        System.out.println("fetch " + cnt);
                    }
                }
            }
        }
    }

    @Test
    public void testCursor() throws Exception {
        populateT25StreamReadTableIfEmpty();
        try (final Connection conn = DriverManager.getConnection(
            BASE_URL + "&useServerPrepStmts=true&useCursorFetch=true", USR, PSW);
            final Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(1);

            try (final ResultSet rs = stmt.executeQuery("select * from t_25_stream_read limit 10")) {
                long cnt = 0;
                while (rs.next()) {
                    cnt++;
                    if (cnt % 100 == 0) {
                        Thread.sleep(100);
                        System.out.println("fetch " + cnt);
                    }
                }
            }
        }
    }

    @Test
    public void testLongData() throws Exception {
        insertDataIfTableEmpty();
        try (final Connection conn = DriverManager.getConnection(BASE_URL + "&useServerPrepStmts=true", USR, PSW);
            final PreparedStatement stmt = conn.prepareStatement("select * from mytable where name = ?")) {
            stmt.setClob(1, new StringReader("hello world"));
            stmt.execute();
        }
    }

    @Test
    public void testReset() throws Exception {
        insertDataIfTableEmpty();
        try (final Connection conn = DriverManager.getConnection(
            BASE_URL + "&useServerPrepStmts=true&useCursorFetch=true", USR, PSW);
            final PreparedStatement stmt = conn.prepareStatement("select * from mytable where name = ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(1);
            stmt.setClob(1, new StringReader("hello world"));
            stmt.execute();

            stmt.clearParameters();
            stmt.setClob(1, new StringReader("hello world2"));
            stmt.execute();
        }
    }

    @Test
    public void largePacket() throws Exception {
        try (final Connection conn = DriverManager.getConnection(
            BASE_URL + "&useServerPrepStmts=true&useCursorFetch=true", USR, PSW);
            final Statement stmt = conn.createStatement()) {

            final StringBuilder builder = new StringBuilder();
            builder.append("select '");
            for (int i = 0; i < 2000000; i++) {
                builder.append("0123456789");
            }
            builder.append("'");
            stmt.execute(builder.toString());
        }
    }

    @Test
    public void testNoDeprecateEof() throws Exception {
        populateT25StreamReadTableIfEmpty();
        setGlobalMock("force_frontend_no_deprecate_eof");
        try (final Connection conn = DriverManager.getConnection(
            BASE_URL + "&useServerPrepStmts=true&useCursorFetch=true", USR, PSW)) {

            try (final Statement stmt = conn.createStatement()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("select '");
                for (int i = 0; i < 2000000; i++) {
                    builder.append("0123456789");
                }
                builder.append("'");
                stmt.execute(builder.toString());

                stmt.execute("select 1,2,3");
            }

            // PS
            try (final PreparedStatement ps = conn.prepareStatement("SELECT CONCAT(?, ?)")) {
                ps.setString(1, "hello' ");
                ps.setString(2, "world!");
                try (ResultSet result = ps.executeQuery()) {
                    while (result.next()) {
                        String concat = result.getString(1);
                        System.out.println(concat);
                    }
                }
            }

            // PS cursor
            try (final Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                stmt.setFetchSize(1);

                try (final ResultSet rs = stmt.executeQuery("select * from t_25_stream_read limit 10")) {
                    long cnt = 0;
                    while (rs.next()) {
                        cnt++;
                        if (cnt % 100 == 0) {
                            Thread.sleep(100);
                            System.out.println("fetch " + cnt);
                        }
                    }
                }
            }
        }
        setGlobalMock("");
    }

    @Test
    public void testPrepareStmtNoParamAndResult() throws Exception {
        try (final Connection conn = DriverManager.getConnection(BASE_URL + "&useServerPrepStmts=true", USR, PSW)) {
            try (final PreparedStatement ps = conn.prepareStatement("commit")) {
                ps.executeUpdate();
                System.out.println("commit");
            }
        }
    }

    @Test
    public void testMultiStmt() throws Exception {
        try (final Connection conn = DriverManager.getConnection(
            BASE_URL + "&allowMultiQueries=true", USR, PSW)) {

            try (final Statement stmt = conn.createStatement()) {
                boolean isResultSet = stmt.execute("select 1,2,3;select 1");
                do {
                    if (isResultSet) {
                        try (final ResultSet rs = stmt.getResultSet()) {
                            while (rs.next()) {
                            }
                            System.out.println("ResultSet");
                        }
                    } else {
                        int updateCount = stmt.getUpdateCount();
                        System.out.println("Update Count: " + updateCount);
                    }
                    // 检查是否有多个结果
                    isResultSet = stmt.getMoreResults();
                } while (isResultSet || stmt.getUpdateCount() != -1);
            }
        }
    }
}
