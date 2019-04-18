package org.fartpig.pagination_sql;

import junit.framework.TestCase;

public class TestPaginator extends TestCase {

	public void testPagniate() {
		String sql = "SELECT * FROM ANIMAL";
		for (Database dataBase : Database.values()) {
			System.out.println("dataBase: " + dataBase);
			Paginator paginator = new Paginator(dataBase);
			String resultSql = paginator.paginate(sql, 10);
			System.out.println("resultSql: " + resultSql);
			resultSql = paginator.paginate(sql, 0, 10);
			System.out.println("resultSql: " + resultSql);
			System.out.println("===================");
		}

	}

	public void testPagniateOracle() {
		String sql = "SELECT * FROM ANIMAL";
		System.out.println("dataBase: " + Database.ORACLE);
		Paginator paginator = new Paginator(Database.ORACLE);
		String resultSql = paginator.paginate(sql, 10);
		System.out.println("resultSql: " + resultSql);
		resultSql = paginator.paginate(sql, 0, 10);
		System.out.println("resultSql: " + resultSql);
		System.out.println("===================");

	}

}
