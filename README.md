# pagination-sql  
convert the sql to pagination style  
code copy from the hibernate dialect getLimitString or LimitHandler.getProcessedSql  

## Usage  
 ```java
		//for oracle >= 9
		String sql = "SELECT * FROM ANIMAL";
		System.out.println("dataBase: " + Database.ORACLE);
		Paginator paginator = new Paginator(Database.ORACLE);
		String resultSql = paginator.paginate(sql, 10);
		System.out.println("resultSql: " + resultSql);
		resultSql = paginator.paginate(sql, 0, 10);
		System.out.println("resultSql: " + resultSql);
		System.out.println("===================");
 ```
