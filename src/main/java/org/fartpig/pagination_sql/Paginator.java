package org.fartpig.pagination_sql;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Paginator {

	private Database dataBase;

	public Database getDataBase() {
		return dataBase;
	}

	public Paginator(Database dataBase) {
		this.dataBase = dataBase;
	}

	public String paginate(String sql, int limit) {
		return paginate(sql, 0, limit);
	}

	public String paginate(String sql, int offset, int limit) {
		String methodName = "paginate_" + dataBase.name();
		try {
			Method method = Paginator.class.getMethod(methodName, String.class, int.class,
					int.class);
			return (String) method.invoke(this, sql, offset, limit);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return sql;
	}

	public String paginate_CUBRID(String sql, int offset, int limit) {
		// if set, use "LIMIT offset, row_count" syntax;
		// if not, use "LIMIT row_count"
		return String.format("%s limit %d, %d", sql, offset, limit);
	}

	public String paginate_DB2(String sql, int offset, int limit) {
		if (offset == 0) {
			return sql + " fetch first " + limit + " rows only";
		}
		// nest the main query in an outer select
		return "select * from ( select inner2_.*, rownumber() over(order by order of inner2_) as rownumber_ from ( "
				+ sql + " fetch first " + limit
				+ " rows only ) as inner2_ ) as inner1_ where rownumber_ > " + offset
				+ " order by rownumber_";
	}

	public String paginate_DB2_400(String sql, int offset, int limit) {
		if (offset > 0) {
			throw new UnsupportedOperationException("query result offset is not supported");
		}
		if (limit == 0) {
			return sql;
		}
		return sql + " fetch first " + limit + " rows only ";
	}

	public String paginate_DERBY(String query, int offset, int limit) {
		/**
		 * {@inheritDoc}
		 * <p/>
		 * From Derby 10.5 Docs:
		 * 
		 * <pre>
		 * Query
		 * [ORDER BY clause]
		 * [result offset clause]
		 * [fetch first clause]
		 * [FOR UPDATE clause]
		 * [WITH {RR|RS|CS|UR}]
		 * </pre>
		 */
		final StringBuilder sb = new StringBuilder(query.length() + 50);
		final String normalizedSelect = query.toLowerCase().trim();
		final int forUpdateIndex = normalizedSelect.lastIndexOf("for update");

		if (hasForUpdateClause(forUpdateIndex)) {
			sb.append(query.substring(0, forUpdateIndex - 1));
		} else if (hasWithClause(normalizedSelect)) {
			sb.append(query.substring(0, getWithIndex(query) - 1));
		} else {
			sb.append(query);
		}

		if (offset == 0) {
			sb.append(" fetch first ");
		} else {
			sb.append(" offset ").append(offset).append(" rows fetch next ");
		}

		sb.append(limit).append(" rows only");

		if (hasForUpdateClause(forUpdateIndex)) {
			sb.append(' ');
			sb.append(query.substring(forUpdateIndex));
		} else if (hasWithClause(normalizedSelect)) {
			sb.append(' ').append(query.substring(getWithIndex(query)));
		}
		return sb.toString();
	}

	public String paginate_ENTERPRISEDB(String sql, int offset, int limit) {
		return String.format("%s limit %d offset %d", sql, limit, offset);
	}

	public String paginate_FIREBIRD(String sql, int offset, int limit) {
		return String.format("%s first %d skip %d", sql, limit, offset);
	}

	public String paginate_H2(String sql, int offset, int limit) {
		return String.format("%s limit %d offset %d", sql, limit, offset);
	}

	public String paginate_HANA(String sql, int offset, int limit) {
		return String.format("%s limit %d offset %d", sql, limit, offset);
	}

	public String paginate_HSQL(String sql, int offset, int limit) {
		return String.format("%s offset %d limit %d", sql, offset, limit);
	}

	public String paginate_HSQL_L20(String sql, int offset, int limit) {
		return new StringBuilder(sql.length() + 10).append(sql)
				.insert(sql.toLowerCase().indexOf("select") + 6, " limit " + offset + " " + limit)
				.toString();
	}

	public String paginate_INFORMIX(String querySelect, int offset, int limit) {
		if (offset > 0) {
			throw new UnsupportedOperationException("query result offset is not supported");
		}
		return new StringBuilder(querySelect.length() + 8).append(querySelect)
				.insert(querySelect.toLowerCase().indexOf("select") + 6, " first " + limit)
				.toString();
	}

	public String paginate_INGRES(String querySelect, int offset, int limit) {
		if (offset > 0) {
			throw new UnsupportedOperationException("query result offset is not supported");
		}
		return new StringBuilder(querySelect.length() + 16).append(querySelect)
				.insert(6, " first " + limit).toString();
	}

	public String paginate_INGRES_9(String querySelect, int offset, int limit) {
		final StringBuilder soff = new StringBuilder(" offset " + offset);
		final StringBuilder slim = new StringBuilder(" fetch first " + limit + " rows only");
		final StringBuilder sb = new StringBuilder(
				querySelect.length() + soff.length() + slim.length()).append(querySelect);
		if (offset > 0) {
			sb.append(soff);
		}
		if (limit > 0) {
			sb.append(slim);
		}
		return sb.toString();
	}

	public String paginate_INTERBASE(String sql, int offset, int limit) {
		return String.format("%s rows %d to %d", sql, limit, offset);
	}

	public String paginate_MYSQL(String sql, int offset, int limit) {
		return String.format("%s limit %d, %d", sql, offset, limit);
	}

	public String paginate_ORACLE(String sql, int offset, int limit) {
		sql = sql.trim();
		String forUpdateClause = null;
		boolean isForUpdate = false;
		final int forUpdateIndex = sql.toLowerCase().lastIndexOf("for update");
		if (forUpdateIndex > -1) {
			// save 'for update ...' and then remove it
			forUpdateClause = sql.substring(forUpdateIndex);
			sql = sql.substring(0, forUpdateIndex - 1);
			isForUpdate = true;
		}

		final StringBuilder pagingSelect = new StringBuilder(sql.length() + 100);
		pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
		pagingSelect.append(sql);
		pagingSelect.append(" ) row_ where rownum <= ");
		pagingSelect.append(limit);
		pagingSelect.append(") where rownum_ > ");
		pagingSelect.append(offset);

		if (isForUpdate) {
			pagingSelect.append(" ");
			pagingSelect.append(forUpdateClause);
		}

		return pagingSelect.toString();
	}

	public String paginate_ORACLE_8(String sql, int offset, int limit) {
		sql = sql.trim();
		boolean isForUpdate = false;
		if (sql.toLowerCase().endsWith(" for update")) {
			sql = sql.substring(0, sql.length() - 11);
			isForUpdate = true;
		}

		final StringBuilder pagingSelect = new StringBuilder(sql.length() + 100);
		pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
		pagingSelect.append(sql);
		pagingSelect.append(" ) row_ ) where rownum_ <= ");
		pagingSelect.append(limit);
		pagingSelect.append(" and rownum_ > ");
		pagingSelect.append(offset);

		if (isForUpdate) {
			pagingSelect.append(" for update");
		}

		return pagingSelect.toString();
	}

	public String paginate_POSTGRESQL(String sql, int offset, int limit) {
		return String.format("%s limit %d offset %d", sql, limit, offset);
	}

	public String paginate_SQLSERVER(String querySelect, int offset, int limit) {
		if (offset > 0) {
			throw new UnsupportedOperationException("query result offset is not supported");
		}
		return new StringBuilder(querySelect.length() + 8).append(querySelect)
				.insert(getAfterSelectInsertPoint(querySelect), " top " + limit).toString();
	}

	public String paginate_SQLSERVER2005(String sql, int offset, int limit) {
		final StringBuilder sb = new StringBuilder(sql);
		if (sb.charAt(sb.length() - 1) == ';') {
			sb.setLength(sb.length() - 1);
		}

		final String selectClause = fillAliasInSelectClause(sb);

		final int orderByIndex = shallowIndexOfWord(sb, ORDER_BY, 0);
		if (orderByIndex > 0) {
			// ORDER BY requires using TOP.
			addTopExpression(sb);
		}

		encloseWithOuterQuery(sb);

		// Wrap the query within a with statement:
		sb.insert(0, "WITH query AS (").append(") SELECT ").append(selectClause)
				.append(" FROM query ");
		sb.append("WHERE __hibernate_row_nr__ >= ");
		sb.append(offset);
		sb.append(" AND __hibernate_row_nr__ < ");
		sb.append(limit);

		return sb.toString();
	}

	public String paginate_TIMESTEN(String querySelect, int offset, int limit) {
		if (offset > 0) {
			throw new UnsupportedOperationException("query result offset is not supported");
		}
		return new StringBuilder(querySelect.length() + 8).append(querySelect)
				.insert(6, " first " + limit).toString();
	}

	private static final String SELECT = "select";
	private static final String SELECT_WITH_SPACE = SELECT + ' ';
	private static final String FROM = "from";
	private static final String DISTINCT = "distinct";
	private static final String ORDER_BY = "order by";

	private static final Pattern ALIAS_PATTERN = Pattern.compile("(?i)\\sas\\s(.)+$");

	private static final int ALIAS_TRUNCATE_LENGTH = 10;
	private static final String WHITESPACE = " \n\r\f\t";

	private boolean hasForUpdateClause(int forUpdateIndex) {
		return forUpdateIndex >= 0;
	}

	private boolean hasWithClause(String normalizedSelect) {
		return normalizedSelect.startsWith("with ", normalizedSelect.length() - 7);
	}

	private int getWithIndex(String querySelect) {
		int i = querySelect.lastIndexOf("with ");
		if (i < 0) {
			i = querySelect.lastIndexOf("WITH ");
		}
		return i;
	}

	private int getAfterSelectInsertPoint(String sql) {
		final int selectIndex = sql.toLowerCase().indexOf("select");
		final int selectDistinctIndex = sql.toLowerCase().indexOf("select distinct");
		return selectIndex + (selectDistinctIndex == selectIndex ? 15 : 6);
	}

	private int shallowIndexOfWord(final StringBuilder sb, final String search, int fromIndex) {
		final int index = shallowIndexOf(sb, ' ' + search + ' ', fromIndex);
		// In case of match adding one because of space placed in front of
		// search term.
		return index != -1 ? (index + 1) : -1;
	}

	private int shallowIndexOf(StringBuilder sb, String search, int fromIndex) {
		// case-insensitive match
		final String lowercase = sb.toString().toLowerCase();
		final int len = lowercase.length();
		final int searchlen = search.length();
		int pos = -1;
		int depth = 0;
		int cur = fromIndex;
		do {
			pos = lowercase.indexOf(search, cur);
			if (pos != -1) {
				for (int iter = cur; iter < pos; iter++) {
					final char c = sb.charAt(iter);
					if (c == '(') {
						depth = depth + 1;
					} else if (c == ')') {
						depth = depth - 1;
					}
				}
				cur = pos + searchlen;
			}
		} while (cur < len && depth != 0 && pos != -1);
		return depth == 0 ? pos : -1;
	}

	private boolean selectsMultipleColumns(String expression) {
		final String lastExpr = expression.trim().replaceFirst("(?i)(.)*\\s", "");
		return "*".equals(lastExpr) || lastExpr.endsWith(".*");
	}

	private String getAlias(String expression) {
		final Matcher matcher = ALIAS_PATTERN.matcher(expression);
		if (matcher.find()) {
			// Taking advantage of Java regular expressions greedy behavior
			// while extracting the last AS keyword.
			// Note that AS keyword can appear in CAST operator, e.g.
			// 'cast(tab1.col1 as varchar(255)) as col1'.
			return matcher.group(0).replaceFirst("(?i)(.)*\\sas\\s", "").trim();
		}
		return null;
	}

	private String fillAliasInSelectClause(StringBuilder sb) {
		final List<String> aliases = new LinkedList<String>();
		final int startPos = shallowIndexOf(sb, SELECT_WITH_SPACE, 0);
		int endPos = shallowIndexOfWord(sb, FROM, startPos);
		int nextComa = startPos;
		int prevComa = startPos;
		int unique = 0;
		boolean selectsMultipleColumns = false;

		while (nextComa != -1) {
			prevComa = nextComa;
			nextComa = shallowIndexOf(sb, ",", nextComa);
			if (nextComa > endPos) {
				break;
			}
			if (nextComa != -1) {
				final String expression = sb.substring(prevComa, nextComa);
				if (selectsMultipleColumns(expression)) {
					selectsMultipleColumns = true;
				} else {
					String alias = getAlias(expression);
					if (alias == null) {
						// Inserting alias. It is unlikely that we would have to
						// add alias, but just in case.
						alias = generateAlias("page", unique);
						sb.insert(nextComa, " as " + alias);
						int aliasExprLength = (" as " + alias).length();
						++unique;
						nextComa += aliasExprLength;
						endPos += aliasExprLength;
					}
					aliases.add(alias);
				}
				++nextComa;
			}
		}
		// Processing last column.
		// Refreshing end position, because we might have inserted new alias.
		endPos = shallowIndexOfWord(sb, FROM, startPos);
		final String expression = sb.substring(prevComa, endPos);
		if (selectsMultipleColumns(expression)) {
			selectsMultipleColumns = true;
		} else {
			String alias = getAlias(expression);
			if (alias == null) {
				// Inserting alias. It is unlikely that we would have to add
				// alias, but just in case.
				alias = generateAlias("page", unique);
				sb.insert(endPos - 1, " as " + alias);
			}
			aliases.add(alias);
		}

		// In case of '*' or '{table}.*' expressions adding an alias breaks SQL
		// syntax, returning '*'.
		return selectsMultipleColumns ? "*" : join(", ", aliases.iterator());
	}

	private String join(String seperator, Iterator objects) {
		StringBuilder buf = new StringBuilder();
		if (objects.hasNext())
			buf.append(objects.next());
		while (objects.hasNext()) {
			buf.append(seperator).append(objects.next());
		}
		return buf.toString();
	}

	private String truncate(String string, int length) {
		if (string.length() <= length) {
			return string;
		} else {
			return string.substring(0, length);
		}
	}

	private String generateAliasRoot(String description) {
		String result = truncate(unqualifyEntityName(description), ALIAS_TRUNCATE_LENGTH)
				.toLowerCase().replace('/', '_') // entityNames may now include
													// slashes for the
													// representations
				.replace('$', '_'); // classname may be an inner class
		result = cleanAlias(result);
		if (Character.isDigit(result.charAt(result.length() - 1))) {
			return result + "x"; // ick!
		} else {
			return result;
		}
	}

	private String cleanAlias(String alias) {
		char[] chars = alias.toCharArray();
		// short cut check...
		if (!Character.isLetter(chars[0])) {
			for (int i = 1; i < chars.length; i++) {
				// as soon as we encounter our first letter, return the
				// substring
				// from that position
				if (Character.isLetter(chars[i])) {
					return alias.substring(i);
				}
			}
		}
		return alias;
	}

	private String unqualify(String qualifiedName) {
		int loc = qualifiedName.lastIndexOf(".");
		return (loc < 0) ? qualifiedName : qualifiedName.substring(loc + 1);
	}

	private String qualifier(String qualifiedName) {
		int loc = qualifiedName.lastIndexOf(".");
		return (loc < 0) ? "" : qualifiedName.substring(0, loc);
	}

	private String unqualifyEntityName(String entityName) {
		String result = unqualify(entityName);
		int slashPos = result.indexOf('/');
		if (slashPos > 0) {
			result = result.substring(0, slashPos - 1);
		}
		return result;
	}

	private String generateAlias(String description, int unique) {
		return generateAliasRoot(description) + Integer.toString(unique) + '_';
	}

	private void addTopExpression(StringBuilder sql) {
		final int distinctStartPos = shallowIndexOfWord(sql, DISTINCT, 0);
		if (distinctStartPos > 0) {
			// Place TOP after DISTINCT.
			sql.insert(distinctStartPos + DISTINCT.length(), " TOP(?)");
		} else {
			final int selectStartPos = shallowIndexOf(sql, SELECT_WITH_SPACE, 0);
			// Place TOP after SELECT.
			sql.insert(selectStartPos + SELECT.length(), " TOP(?)");
		}
	}

	private void encloseWithOuterQuery(StringBuilder sql) {
		sql.insert(0,
				"SELECT inner_query.*, ROW_NUMBER() OVER (ORDER BY CURRENT_TIMESTAMP) as __hibernate_row_nr__ FROM ( ");
		sql.append(" ) inner_query ");
	}
}
