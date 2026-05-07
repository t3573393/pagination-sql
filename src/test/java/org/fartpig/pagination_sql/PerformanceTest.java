package org.fartpig.pagination_sql;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PerformanceTest {

    private Connection connection;
    private Paginator paginator;

    @Before
    public void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:perftest" + System.nanoTime() + ";DB_CLOSE_DELAY=-1", "sa", "");
        paginator = new Paginator(Database.H2);
        createLargeTestData();
    }

    @After
    public void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void createLargeTestData() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE performance_test (" +
                "id INT PRIMARY KEY AUTO_INCREMENT, " +
                "name VARCHAR(100), " +
                "description VARCHAR(500), " +
                "item_value DECIMAL(15,2), " +
                "category VARCHAR(50), " +
                "created_date DATE)");

        int batchSize = 1000;
        int totalRecords = 10000;

        for (int batch = 0; batch < totalRecords / batchSize; batch++) {
            StringBuilder sql = new StringBuilder("INSERT INTO performance_test (name, description, item_value, category, created_date) VALUES ");
            for (int i = 0; i < batchSize; i++) {
                int id = batch * batchSize + i + 1;
                if (i > 0) sql.append(", ");
                sql.append("('Name").append(id)
                   .append("', 'Description for record ").append(id)
                   .append(" with some additional text to make it longer', ")
                   .append(id * 10.5)
                   .append(", 'Category").append(id % 100)
                   .append("', '2024-01-01')");
            }
            stmt.execute(sql.toString());
        }
        stmt.close();
    }

    @Test
    public void testSqlTransformationPerformance() {
        String[] sqls = {
            "SELECT * FROM users",
            "SELECT id, name, email FROM users WHERE status = 'active'",
            "SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' ORDER BY o.total DESC",
            "SELECT COUNT(*), category FROM products GROUP BY category HAVING COUNT(*) > 10 ORDER BY COUNT(*) DESC"
        };

        long totalTime = 0;
        int iterations = 10000;

        for (String sql : sqls) {
            long startTime = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                paginator.paginate(sql, 10, 20);
            }
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            totalTime += duration;
            System.out.println("SQL transformation (" + iterations + " iterations): " + duration + " ms");
            System.out.println("  Average per call: " + (duration * 1000000.0 / iterations) + " ns");
        }

        System.out.println("Total SQL transformation time: " + totalTime + " ms");
        assertTrue("SQL transformation should complete in reasonable time", totalTime < 5000);
    }

    @Test
    public void testAllDatabasesTransformationPerformance() {
        String sql = "SELECT * FROM performance_test WHERE category = 'Category1' ORDER BY id";
        long totalTime = 0;
        int iterations = 5000;

        for (Database db : Database.values()) {
            Paginator p = new Paginator(db);
            long startTime = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                try {
                    p.paginate(sql, 10, 20);
                } catch (Exception e) {
                    // Ignore errors for unsupported operations
                }
            }
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            totalTime += duration;
            System.out.println("Database " + db + " (" + iterations + " iterations): " + duration + " ms");
        }

        System.out.println("Total transformation time across all databases: " + totalTime + " ms");
        assertTrue("All databases transformation should complete in reasonable time", totalTime < 10000);
    }

    @Test
    public void testLargeResultSetPaginationPerformance() throws SQLException {
        int pageSize = 100;
        int totalPages = 10;
        long totalTime = 0;

        for (int page = 0; page < totalPages; page++) {
            String baseSql = "SELECT * FROM performance_test ORDER BY id";
            String paginatedSql = paginator.paginate(baseSql, page * pageSize, pageSize);

            long startTime = System.nanoTime();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(paginatedSql);

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                rs.getInt("id");
                rs.getString("name");
                rs.getString("description");
                rs.getDouble("item_value");
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            totalTime += duration;

            assertEquals("Should return correct number of rows", pageSize, rowCount);
            rs.close();
            stmt.close();
        }

        System.out.println("Large result set pagination (" + totalPages + " pages, " + pageSize + " per page): " + totalTime + " ms");
        System.out.println("Average per page: " + (totalTime / totalPages) + " ms");
        assertTrue("Large result set pagination should complete in reasonable time", totalTime < 5000);
    }

    @Test
    public void testComplexJoinPaginationPerformance() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE orders (" +
                "id INT PRIMARY KEY AUTO_INCREMENT, " +
                "user_id INT, " +
                "total DECIMAL(10,2), " +
                "order_date DATE)");

        for (int i = 1; i <= 5000; i++) {
            stmt.execute("INSERT INTO orders (user_id, total, order_date) VALUES " +
                    "(" + (i % 1000 + 1) + ", " + (i * 15.5) + ", '2024-01-01')");
        }
        stmt.close();

        String joinSql = "SELECT p.name, p.item_value, o.total, o.order_date " +
                "FROM performance_test p " +
                "JOIN orders o ON p.id = o.user_id " +
                "WHERE p.category = 'Category1' " +
                "ORDER BY o.total DESC";

        int iterations = 100;
        long totalTime = 0;

        for (int i = 0; i < iterations; i++) {
            String paginatedSql = paginator.paginate(joinSql, 0, 50);

            long startTime = System.nanoTime();
            Statement queryStmt = connection.createStatement();
            ResultSet rs = queryStmt.executeQuery(paginatedSql);

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }

            long endTime = System.nanoTime();
            totalTime += (endTime - startTime) / 1000000;

            rs.close();
            queryStmt.close();
        }

        System.out.println("Complex join pagination (" + iterations + " iterations): " + totalTime + " ms");
        System.out.println("Average per query: " + (totalTime / iterations) + " ms");
        assertTrue("Complex join pagination should complete in reasonable time", totalTime < 3000);
    }

    @Test
    public void testAggregateQueryPaginationPerformance() throws SQLException {
        String aggregateSql = "SELECT category, COUNT(*) as cnt, AVG(item_value) as avg_value, SUM(item_value) as total " +
                "FROM performance_test GROUP BY category ORDER BY cnt DESC";

        int iterations = 100;
        long totalTime = 0;

        for (int i = 0; i < iterations; i++) {
            String paginatedSql = paginator.paginate(aggregateSql, 0, 10);

            long startTime = System.nanoTime();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(paginatedSql);

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                rs.getString("category");
                rs.getInt("cnt");
                rs.getDouble("avg_value");
                rs.getDouble("total");
            }

            long endTime = System.nanoTime();
            totalTime += (endTime - startTime) / 1000000;

            rs.close();
            stmt.close();
        }

        System.out.println("Aggregate query pagination (" + iterations + " iterations): " + totalTime + " ms");
        System.out.println("Average per query: " + (totalTime / iterations) + " ms");
        assertTrue("Aggregate query pagination should complete in reasonable time", totalTime < 2000);
    }

    @Test
    public void testSubqueryPaginationPerformance() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE orders_sub (" +
                "id INT PRIMARY KEY AUTO_INCREMENT, " +
                "user_id INT, " +
                "product VARCHAR(100))");

        for (int i = 1; i <= 1000; i++) {
            stmt.execute("INSERT INTO orders_sub (user_id, product) VALUES (" + i + ", 'Product" + i + "')");
        }
        stmt.close();

        String subquerySql = "SELECT * FROM performance_test " +
                "WHERE id IN (SELECT user_id FROM orders_sub) " +
                "ORDER BY id";

        int iterations = 50;
        long totalTime = 0;

        for (int i = 0; i < iterations; i++) {
            String paginatedSql = paginator.paginate(subquerySql, 0, 100);

            long startTime = System.nanoTime();
            Statement queryStmt = connection.createStatement();
            ResultSet rs = queryStmt.executeQuery(paginatedSql);

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }

            long endTime = System.nanoTime();
            totalTime += (endTime - startTime) / 1000000;

            rs.close();
            queryStmt.close();
        }

        System.out.println("Subquery pagination (" + iterations + " iterations): " + totalTime + " ms");
        System.out.println("Average per query: " + (totalTime / iterations) + " ms");
        assertTrue("Subquery pagination should complete in reasonable time", totalTime < 3000);
    }

    @Test
    public void testBulkPaginationPerformance() throws SQLException {
        int pageSize = 50;
        int totalRecords = 10000;
        int totalPages = totalRecords / pageSize;
        long totalTime = 0;

        String baseSql = "SELECT * FROM performance_test ORDER BY id";

        for (int page = 0; page < totalPages; page++) {
            String paginatedSql = paginator.paginate(baseSql, page * pageSize, pageSize);

            long startTime = System.nanoTime();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(paginatedSql);

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }

            long endTime = System.nanoTime();
            totalTime += (endTime - startTime) / 1000000;

            rs.close();
            stmt.close();
        }

        System.out.println("Bulk pagination (" + totalPages + " pages, " + totalRecords + " total records): " + totalTime + " ms");
        System.out.println("Average per page: " + (totalTime / totalPages) + " ms");
        System.out.println("Throughput: " + (totalRecords * 1000.0 / totalTime) + " records/second");
        assertTrue("Bulk pagination should complete in reasonable time", totalTime < 10000);
    }

    @Test
    public void testOffsetPerformance() throws SQLException {
        int[] offsets = {0, 1000, 5000, 9000};
        int limit = 100;
        long totalTime = 0;

        for (int offset : offsets) {
            String baseSql = "SELECT * FROM performance_test ORDER BY id";
            String paginatedSql = paginator.paginate(baseSql, offset, limit);

            long startTime = System.nanoTime();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(paginatedSql);

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            totalTime += duration;

            System.out.println("Offset " + offset + ": " + duration + " ms, rows: " + rowCount);
            rs.close();
            stmt.close();
        }

        System.out.println("Total offset test time: " + totalTime + " ms");
        assertTrue("Offset performance test should complete in reasonable time", totalTime < 5000);
    }

    @Test
    public void testMemoryEfficiency() throws SQLException {
        String baseSql = "SELECT * FROM performance_test ORDER BY id";
        String paginatedSql = paginator.paginate(baseSql, 0, 100);

        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        long memoryBefore = runtime.totalMemory() - runtime.freeMemory();

        for (int i = 0; i < 100; i++) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(paginatedSql);

            List<String> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rs.getString("name"));
            }

            rs.close();
            stmt.close();

            if (i % 20 == 0) {
                runtime.gc();
                long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
                System.out.println("Iteration " + i + ": Memory used: " + (memoryAfter / 1024) + " KB");
            }
        }

        runtime.gc();
        long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
        long memoryUsed = memoryAfter - memoryBefore;

        System.out.println("Memory used after 100 iterations: " + (memoryUsed / 1024) + " KB");
        assertTrue("Memory usage should be reasonable", memoryUsed < 50 * 1024 * 1024);
    }
}
