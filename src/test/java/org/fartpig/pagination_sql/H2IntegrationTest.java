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

public class H2IntegrationTest {

    private Connection connection;
    private Paginator paginator;

    @Before
    public void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:testdb" + System.nanoTime() + ";DB_CLOSE_DELAY=-1", "sa", "");
        paginator = new Paginator(Database.H2);
        createTestTable();
        insertTestData();
    }

    @After
    public void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void createTestTable() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE users (" +
                "id INT PRIMARY KEY AUTO_INCREMENT, " +
                "name VARCHAR(100), " +
                "email VARCHAR(100), " +
                "age INT)");
        stmt.close();
    }

    private void insertTestData() throws SQLException {
        Statement stmt = connection.createStatement();
        for (int i = 1; i <= 50; i++) {
            stmt.execute("INSERT INTO users (name, email, age) VALUES " +
                    "('User" + i + "', 'user" + i + "@test.com', " + (20 + i % 30) + ")");
        }
        stmt.close();
    }

    @Test
    public void testPaginationFirstPage() throws SQLException {
        String baseSql = "SELECT * FROM users ORDER BY id";
        String paginatedSql = paginator.paginate(baseSql, 0, 10);
        
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(paginatedSql);
        
        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getInt("id"));
        }
        
        assertEquals(10, ids.size());
        assertEquals(1, (int) ids.get(0));
        assertEquals(10, (int) ids.get(9));
        
        rs.close();
        stmt.close();
    }

    @Test
    public void testPaginationSecondPage() throws SQLException {
        String baseSql = "SELECT * FROM users ORDER BY id";
        String paginatedSql = paginator.paginate(baseSql, 10, 10);
        
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(paginatedSql);
        
        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getInt("id"));
        }
        
        assertEquals(10, ids.size());
        assertEquals(11, (int) ids.get(0));
        assertEquals(20, (int) ids.get(9));
        
        rs.close();
        stmt.close();
    }

    @Test
    public void testPaginationLastPage() throws SQLException {
        String baseSql = "SELECT * FROM users ORDER BY id";
        String paginatedSql = paginator.paginate(baseSql, 40, 15);
        
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(paginatedSql);
        
        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getInt("id"));
        }
        
        assertEquals(10, ids.size());
        assertEquals(41, (int) ids.get(0));
        assertEquals(50, (int) ids.get(9));
        
        rs.close();
        stmt.close();
    }

    @Test
    public void testPaginationWithWhereClause() throws SQLException {
        String baseSql = "SELECT * FROM users WHERE age > 40 ORDER BY id";
        String paginatedSql = paginator.paginate(baseSql, 0, 5);
        
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(paginatedSql);
        
        int count = 0;
        while (rs.next()) {
            assertTrue(rs.getInt("age") > 40);
            count++;
        }
        
        assertEquals(5, count);
        
        rs.close();
        stmt.close();
    }

    @Test
    public void testPaginationWithJoin() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE orders (" +
                "id INT PRIMARY KEY AUTO_INCREMENT, " +
                "user_id INT, " +
                "product VARCHAR(100), " +
                "amount DECIMAL(10,2))");
        
        for (int i = 1; i <= 30; i++) {
            stmt.execute("INSERT INTO orders (user_id, product, amount) VALUES " +
                    "(" + ((i % 10) + 1) + ", 'Product" + i + "', " + (i * 10.5) + ")");
        }
        stmt.close();
        
        String baseSql = "SELECT u.name, o.product, o.amount " +
                "FROM users u JOIN orders o ON u.id = o.user_id " +
                "ORDER BY o.id";
        String paginatedSql = paginator.paginate(baseSql, 5, 10);
        
        Statement queryStmt = connection.createStatement();
        ResultSet rs = queryStmt.executeQuery(paginatedSql);
        
        int count = 0;
        while (rs.next()) {
            assertNotNull(rs.getString("name"));
            assertNotNull(rs.getString("product"));
            count++;
        }
        
        assertEquals(10, count);
        
        rs.close();
        queryStmt.close();
    }

    @Test
    public void testPaginationWithAggregate() throws SQLException {
        String baseSql = "SELECT age, COUNT(*) as cnt FROM users GROUP BY age ORDER BY age";
        String paginatedSql = paginator.paginate(baseSql, 0, 5);
        
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(paginatedSql);
        
        int count = 0;
        while (rs.next()) {
            assertTrue(rs.getInt("age") >= 20);
            assertTrue(rs.getInt("cnt") > 0);
            count++;
        }
        
        assertTrue(count <= 5);
        
        rs.close();
        stmt.close();
    }

    @Test
    public void testPaginationWithSubquery() throws SQLException {
        String baseSql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY id";
        
        Statement createStmt = connection.createStatement();
        createStmt.execute("CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, user_id INT, product VARCHAR(100))");
        for (int i = 1; i <= 15; i++) {
            createStmt.execute("INSERT INTO orders (user_id, product) VALUES (" + i + ", 'Product" + i + "')");
        }
        createStmt.close();
        
        String paginatedSql = paginator.paginate(baseSql, 0, 5);
        
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(paginatedSql);
        
        int count = 0;
        while (rs.next()) {
            count++;
        }
        
        assertEquals(5, count);
        
        rs.close();
        stmt.close();
    }

    @Test
    public void testPaginationTotalCount() throws SQLException {
        Statement countStmt = connection.createStatement();
        ResultSet countRs = countStmt.executeQuery("SELECT COUNT(*) FROM users");
        countRs.next();
        int totalCount = countRs.getInt(1);
        countRs.close();
        countStmt.close();
        
        assertEquals(50, totalCount);
        
        int pageSize = 10;
        int totalPages = (int) Math.ceil((double) totalCount / pageSize);
        
        int actualPages = 0;
        for (int page = 0; page < totalPages; page++) {
            String baseSql = "SELECT * FROM users ORDER BY id";
            String paginatedSql = paginator.paginate(baseSql, page * pageSize, pageSize);
            
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(paginatedSql);
            
            while (rs.next()) {
                actualPages++;
            }
            
            rs.close();
            stmt.close();
        }
        
        assertEquals(totalCount, actualPages);
    }
}
