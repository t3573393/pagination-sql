package org.fartpig.pagination_sql;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestPaginator {

    @Test
    public void testPaginateAllDatabases() {
        String sql = "SELECT * FROM ANIMAL";
        for (Database dataBase : Database.values()) {
            System.out.println("dataBase: " + dataBase);
            Paginator paginator = new Paginator(dataBase);
            try {
                String resultSql = paginator.paginate(sql, 10);
                System.out.println("resultSql: " + resultSql);
                assertNotNull(resultSql);
                assertFalse(resultSql.isEmpty());
                
                resultSql = paginator.paginate(sql, 0, 10);
                System.out.println("resultSql: " + resultSql);
                assertNotNull(resultSql);
                assertFalse(resultSql.isEmpty());
            } catch (UnsupportedOperationException e) {
                System.out.println("Offset not supported for " + dataBase);
            }
            System.out.println("===================");
        }
    }

    @Test
    public void testPaginateOracle() {
        String sql = "SELECT * FROM ANIMAL";
        System.out.println("dataBase: " + Database.ORACLE);
        Paginator paginator = new Paginator(Database.ORACLE);
        String resultSql = paginator.paginate(sql, 10);
        System.out.println("resultSql: " + resultSql);
        assertTrue(resultSql.contains("rownum"));
        
        resultSql = paginator.paginate(sql, 0, 10);
        System.out.println("resultSql: " + resultSql);
        assertTrue(resultSql.contains("rownum"));
        System.out.println("===================");
    }

    @Test
    public void testPaginateMySQL() {
        String sql = "SELECT * FROM users";
        Paginator paginator = new Paginator(Database.MYSQL);
        String result = paginator.paginate(sql, 5, 10);
        assertEquals("SELECT * FROM users limit 5, 10", result);
    }

    @Test
    public void testPaginatePostgreSQL() {
        String sql = "SELECT * FROM products";
        Paginator paginator = new Paginator(Database.POSTGRESQL);
        String result = paginator.paginate(sql, 20, 50);
        assertEquals("SELECT * FROM products limit 50 offset 20", result);
    }
}
