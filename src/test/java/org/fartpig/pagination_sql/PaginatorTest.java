package org.fartpig.pagination_sql;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

public class PaginatorTest {

    private Paginator paginator;

    @Before
    public void setUp() {
        paginator = new Paginator(Database.MYSQL);
    }

    @Test
    public void testPaginateMySQL() {
        String sql = "SELECT * FROM users";
        String result = paginator.paginate(sql, 0, 10);
        assertEquals("SELECT * FROM users limit 0, 10", result);
    }

    @Test
    public void testPaginateMySQLWithLimitOnly() {
        String sql = "SELECT * FROM users";
        String result = paginator.paginate(sql, 20);
        assertEquals("SELECT * FROM users limit 0, 20", result);
    }

    @Test
    public void testPaginateOracle() {
        Paginator oraclePaginator = new Paginator(Database.ORACLE);
        String sql = "SELECT * FROM users";
        String result = oraclePaginator.paginate(sql, 10, 20);
        assertTrue("Should contain rownum", result.contains("rownum"));
        assertTrue("Should contain rownum_ > 10", result.contains("rownum_ > 10"));
        assertTrue("Should contain rownum <= 20", result.contains("rownum <= 20"));
    }

    @Test
    public void testPaginateOracleWithLimitOnly() {
        Paginator oraclePaginator = new Paginator(Database.ORACLE);
        String sql = "SELECT * FROM users";
        String result = oraclePaginator.paginate(sql, 20);
        assertTrue("Should contain rownum", result.contains("rownum"));
        assertTrue("Should contain rownum <= 20", result.contains("rownum <= 20"));
    }

    @Test
    public void testPaginatePostgreSQL() {
        Paginator pgPaginator = new Paginator(Database.POSTGRESQL);
        String sql = "SELECT * FROM products";
        String result = pgPaginator.paginate(sql, 5, 15);
        assertEquals("SELECT * FROM products limit 15 offset 5", result);
    }

    @Test
    public void testPaginateH2() {
        Paginator h2Paginator = new Paginator(Database.H2);
        String sql = "SELECT * FROM items";
        String result = h2Paginator.paginate(sql, 10, 25);
        assertEquals("SELECT * FROM items limit 25 offset 10", result);
    }

    @Test
    public void testPaginateDB2() {
        Paginator db2Paginator = new Paginator(Database.DB2);
        String sql = "SELECT * FROM orders";
        String result = db2Paginator.paginate(sql, 0, 50);
        assertEquals("SELECT * FROM orders fetch first 50 rows only", result);
    }

    @Test
    public void testPaginateDB2WithOffset() {
        Paginator db2Paginator = new Paginator(Database.DB2);
        String sql = "SELECT * FROM orders";
        String result = db2Paginator.paginate(sql, 10, 50);
        assertTrue(result.contains("rownumber() over(order by order of inner2_)"));
        assertTrue(result.contains("rownumber_ > 10"));
    }

    @Test
    public void testPaginateSQLServer() {
        Paginator sqlServerPaginator = new Paginator(Database.SQLSERVER);
        String sql = "SELECT * FROM customers";
        String result = sqlServerPaginator.paginate(sql, 0, 100);
        assertEquals("SELECT top 100 * FROM customers", result);
    }

    @Test
    public void testPaginateSQLServerWithOffsetReturnsOriginalSql() {
        Paginator sqlServerPaginator = new Paginator(Database.SQLSERVER);
        String sql = "SELECT * FROM customers";
        String result = sqlServerPaginator.paginate(sql, 10, 100);
        assertEquals("Should return original SQL when offset is not supported", sql, result);
    }

    @Test
    public void testPaginateFirebird() {
        Paginator firebirdPaginator = new Paginator(Database.FIREBIRD);
        String sql = "SELECT * FROM employees";
        String result = firebirdPaginator.paginate(sql, 5, 10);
        assertEquals("SELECT * FROM employees first 10 skip 5", result);
    }

    @Test
    public void testPaginateInformix() {
        Paginator informixPaginator = new Paginator(Database.INFORMIX);
        String sql = "SELECT * FROM sales";
        String result = informixPaginator.paginate(sql, 0, 30);
        assertEquals("SELECT first 30 * FROM sales", result);
    }

    @Test
    public void testPaginateInformixWithOffsetReturnsOriginalSql() {
        Paginator informixPaginator = new Paginator(Database.INFORMIX);
        String sql = "SELECT * FROM sales";
        String result = informixPaginator.paginate(sql, 10, 30);
        assertEquals("Should return original SQL when offset is not supported", sql, result);
    }

    @Test
    public void testPaginateHSQL() {
        Paginator hsqlPaginator = new Paginator(Database.HSQL);
        String sql = "SELECT * FROM accounts";
        String result = hsqlPaginator.paginate(sql, 20, 50);
        assertEquals("SELECT * FROM accounts offset 20 limit 50", result);
    }

    @Test
    public void testPaginateEnterpriseDB() {
        Paginator edbPaginator = new Paginator(Database.ENTERPRISEDB);
        String sql = "SELECT * FROM transactions";
        String result = edbPaginator.paginate(sql, 15, 25);
        assertEquals("SELECT * FROM transactions limit 25 offset 15", result);
    }

    @Test
    public void testPaginateCUBRID() {
        Paginator cubridPaginator = new Paginator(Database.CUBRID);
        String sql = "SELECT * FROM books";
        String result = cubridPaginator.paginate(sql, 5, 10);
        assertEquals("SELECT * FROM books limit 5, 10", result);
    }

    @Test
    public void testPaginateHANA() {
        Paginator hanaPaginator = new Paginator(Database.HANA);
        String sql = "SELECT * FROM inventory";
        String result = hanaPaginator.paginate(sql, 100, 200);
        assertEquals("SELECT * FROM inventory limit 200 offset 100", result);
    }

    @Test
    public void testPaginateWithComplexSQL() {
        String complexSql = "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'active' ORDER BY u.name";
        String result = paginator.paginate(complexSql, 10, 20);
        assertTrue(result.startsWith(complexSql));
        assertTrue(result.contains("limit 10, 20"));
    }

    @Test
    public void testGetDatabase() {
        assertEquals(Database.MYSQL, paginator.getDataBase());
    }

    @Test
    public void testAllDatabasesHavePaginateMethod() {
        String sql = "SELECT * FROM test_table";
        for (Database db : Database.values()) {
            Paginator p = new Paginator(db);
            String result = p.paginate(sql, 0, 10);
            assertNotNull("Result should not be null for " + db, result);
            assertFalse("Result should not be empty for " + db, result.isEmpty());
        }
    }
}
