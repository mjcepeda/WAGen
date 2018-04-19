package edu.rit.wagen.database.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import edu.rit.wagen.dabatase.iapi.Database;
import edu.rit.wagen.dto.Constraint;
import edu.rit.wagen.dto.PTable;
import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.utils.Utils;
import edu.rit.wagen.utils.Utils.ConstraintType;

/**
 * The Class DatabaseImpl.
 * @author Maria Cepeda
 */
public class DatabaseImpl implements Database {

	/** The Constant DRIVER. */
	// database data
	private static final String DRIVER = "com.mysql.jdbc.Driver";

	/** The Constant URL. */
	private final static String URL = "jdbc:mysql://localhost/?zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true";

	/** The Constant USER. */
	private final static String USER = "root";

	/** The Constant PASSWORD. */
	private final static String PASSWORD = "mientras";

	/** The Constant PTABLE. */
	private final static String PTABLE = "CREATE TABLE <SCHEMA_NAME>.PTABLE (ATTRIBUTE VARCHAR(100), SYMBOL VARCHAR(100), PREDICATE varchar(200))";

	/** The Constant SYMBOL_INDEX. */
	private final static String SYMBOL_INDEX = "CREATE INDEX IDX_SYMBOL ON <SCHEMA_NAME>.PTABLE (SYMBOL)";
	
	/** The Constant ATT_INDEX. */
	private final static String ATT_INDEX = "CREATE INDEX IDX_ATT ON <SCHEMA_NAME>.PTABLE (ATTRIBUTE)";

	/** The conn. */
	private Connection conn;

	static {
		try {
			Class.forName(DRIVER).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#execCommands(java.util.List)
	 */
	public void execCommands(List<String> commands) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			conn.setAutoCommit(false);
			int counter = 1;
			for (String insert : commands) {
				ps = conn.prepareStatement(insert);
				ps.executeUpdate();
				if (counter > 1000) {
					conn.commit();
					counter = 1;
				}
			}
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.rollback();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}

	/**
	 * Exec commands 2.
	 *
	 * @param stmts the stmts
	 * @throws SQLException the SQL exception
	 */
	public void execCommands2(List<String> stmts) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			conn.setAutoCommit(false);
			final int batchSize = 1000;
			int count = 0;

			for (String stmt : stmts) {
				ps = conn.prepareStatement(stmt);
				ps.addBatch();
				if (count++ == batchSize) {
					ps.executeBatch();
					ps.clearBatch();
					conn.commit();
					count = 0;
				}
			}
			if (count > 0) {
				ps.executeBatch();
			}
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.rollback();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}

	/**
	 * Exec updates.
	 *
	 * @param stmt the stmt
	 * @param data the data
	 * @throws SQLException the SQL exception
	 */
	public void execUpdates(String stmt, List<List<String>> data) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(stmt);

			final int batchSize = 5000;
			int count = 0;

			for (List<String> tuple : data) {
				int idx = 0;
				while (idx < tuple.size()) {
					ps.setString(idx + 1, tuple.get(idx));
					idx++;
				}
				ps.addBatch();
				if (count++ == batchSize) {
					ps.executeBatch();
					ps.clearBatch();
					conn.commit();
					count = 0;
				}
			}
			if (count > 0) {
				ps.executeBatch();
			}
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.rollback();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#execCommand(java.lang.String)
	 */
	public void execCommand(String command) throws SQLException {
		// System.out.println(command);
		Connection conn = null;
		Statement ps = null;
		try {
			conn = getConnection();
			conn.setAutoCommit(Boolean.TRUE);
			ps = conn.createStatement();
			ps.executeUpdate(command);
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.rit.wagen.dabatase.iapi.Database#createSchema(edu.rit.wagen.dto.
	 * Schema)
	 */
	public void createSchema(String schema) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			String createSchema = "CREATE DATABASE " + schema;
			ps = conn.prepareStatement(createSchema);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.rit.wagen.dabatase.iapi.Database#createTable(edu.rit.wagen.dto.
	 * TableDescription)
	 */
	public boolean createSymbolicTable(TableSchema tableSchema) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		boolean created = Boolean.FALSE;
		try {
			conn = getConnection();
			// create user table
			String sqlCreateTable = createTableString(tableSchema);
			ps = conn.prepareStatement(sqlCreateTable);
			ps.addBatch();

			List<Constraint> fkList = tableSchema.getConstraints().stream()
					.filter(c -> c.type.equals(ConstraintType.FK)).collect(Collectors.toList());
			for (Constraint c : fkList) {
				StringBuffer index = new StringBuffer("create index idx_");
				index.append(c.column).append(" on ").append(tableSchema.getSchemaName()).append(".")
						.append(tableSchema.getTableName()).append("(").append(c.column).append(")");
				ps.addBatch(index.toString());
			}

			int[] results = ps.executeBatch();
			created = results[0] >= 0 ? true : false;

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
			}
		}
		return created;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getOutputSchema(java.lang.String, java.lang.String)
	 */
	public TableSchema getOutputSchema(String schema, String table) throws SQLException {
		ArrayList<String> colNames = new ArrayList<String>();
		ArrayList<String> colTypes = new ArrayList<String>();
		List<Constraint> colContraints = new ArrayList<Constraint>();
		Connection conn = null;
		Statement s = null;
		PreparedStatement st = null;
		try {
			conn = getConnection();
			s = conn.createStatement();
			ResultSet rs = s.executeQuery("select * from " + schema + "." + table);
			ResultSetMetaData rsmd = rs.getMetaData();
			int numCols = rsmd.getColumnCount();
			for (int i = 1; i <= numCols; i++) {
				// For some JDBC drivers,
				// getColumnName()
				// gives the original column names inside base tables.
				if (!rsmd.getColumnName(i).toUpperCase().equals("ID_SDB")) {
					colNames.add(rsmd.getColumnName(i));
					colTypes.add(rsmd.getColumnTypeName(i));
				}
			}
			rs.close();
			// get constraints info
			st = conn.prepareStatement(
					"select distinct CCU.COLUMN_NAME, constraint_type, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_NAME "
							+ "from INFORMATION_SCHEMA.TABLE_CONSTRAINTS as TC "
							+ "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE as CCU "
							+ "on TC.CONSTRAINT_SCHEMA = CCU.CONSTRAINT_SCHEMA "
							+ "and TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME where ccu.TABLE_NAME = ? and tc.constraint_schema=?");
			st.setString(1, table);
			st.setString(2, schema);
			rs = st.executeQuery();
			while (rs.next()) {
				String column = rs.getString(1);
				String constraint = rs.getString(2);
				String referenced_column = rs.getString(3);
				String reference_table = rs.getString(4);
				// id column in the symbolic database is an internal column, do
				// not include it in the result
				if (!column.toUpperCase().equals("ID_SDB")) {
					colContraints.add(new Constraint(Utils.getConstraint(constraint), column, referenced_column,
							reference_table));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return new TableSchema(schema, table, colNames, colTypes, colContraints);
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getReferencerTable(java.lang.String, edu.rit.wagen.dto.TableSchema, java.lang.String, java.lang.String)
	 */
	public TableSchema getReferencerTable(String realSchema, TableSchema table, String referencedColumn,
			String referencerColumn) throws SQLException {
		Connection conn = null;
		PreparedStatement s = null;
		String referencedTable = null;
		TableSchema tableR = null;
		try {
			conn = getConnection();
			String query = new String(
					"select table_name from information_schema.key_column_usage where table_schema = ? "
							+ "and referenced_table_name = ? and referenced_column_name = ? and column_name = ?");
			s = conn.prepareStatement(query);
			s.setString(1, realSchema);
			s.setString(2, table.getTableName());
			s.setString(3, referencedColumn);
			s.setString(4, referencerColumn);
			ResultSet rs = s.executeQuery();
			if (rs.next()) {
				referencedTable = rs.getString(1);
				if (referencedTable != null) {
					tableR = getOutputSchema(realSchema, referencedTable);
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return tableR;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getSymbolValueCache(java.lang.String, java.lang.String)
	 */
	public String getSymbolValueCache(String schema, String symbol) throws SQLException {
		Connection conn = null;
		PreparedStatement s = null;
		String value = null;
		try {
			conn = getConnection();
			String query = new String("select value from " + schema + ".symbol_value_cache where symbol = ? ");
			s = conn.prepareStatement(query);
			s.setString(1, symbol);
			ResultSet rs = s.executeQuery();
			if (rs.next()) {
				value = rs.getString(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return value;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getData(edu.rit.wagen.dto.TableSchema)
	 */
	public List<Tuple> getData(TableSchema table) throws SQLException {
		Connection conn = null;
		Statement statement = null;
		List<Tuple> data = new ArrayList<Tuple>();
		try {
			conn = getConnection();
			statement = conn.createStatement();

			String sql = "select * from " + table.getSchemaName() + "." + table.getTableName();
			ResultSet rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map<String, String> m = new HashMap<>();
				for (String attr : table.getColNames()) {
					String value = rs.getString(attr);
					m.put(attr, value.replaceAll("'", ""));
				}
				Tuple tuple = new Tuple();
				tuple.setValues(m);
				data.add(tuple);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
		return data;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getData(edu.rit.wagen.dto.TableSchema, int, int)
	 */
	public List<Tuple> getData(TableSchema table, int page, int offset) throws SQLException {
		Connection conn = null;
		PreparedStatement statement = null;
		List<Tuple> data = new ArrayList<Tuple>();
		try {
			conn = getConnection();
			String sql = "select * from " + table.getSchemaName() + "." + table.getTableName()
					+ " order by id_sdb limit ?, ?";
			statement = conn.prepareStatement(sql);
			statement.setInt(1, page);
			statement.setInt(2, offset);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				Map<String, String> m = new HashMap<>();
				for (String attr : table.getColNames()) {
					m.put(attr, rs.getString(attr));
				}
				Tuple tuple = new Tuple();
				tuple.setValues(m);
				data.add(tuple);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
		return data;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#countData(edu.rit.wagen.dto.TableSchema)
	 */
	public int countData(TableSchema table) throws SQLException {
		Connection conn = null;
		Statement statement = null;
		int counter = 0;
		try {
			conn = getConnection();
			statement = conn.createStatement();

			String sql = "select count(*) from " + table.getSchemaName() + "." + table.getTableName();
			ResultSet rs = statement.executeQuery(sql);
			if (rs.next()) {
				counter = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
		return counter;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getPredicateValueCache(java.lang.String, java.lang.String)
	 */
	public String getPredicateValueCache(String schema, String pattern) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		String symbol = null;
		try {
			conn = getConnection();
			String sql = "select symbol from " + schema + ".PREDICATE_VALUE_CACHE where predicate =?";
			ps = conn.prepareStatement(sql);
			ps.setString(1, pattern);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				symbol = rs.getString(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
			}
		}
		return symbol;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#insertConstraints(java.lang.String, java.util.List)
	 */
	public void insertConstraints(String schemaName, List<PTable> constraints) throws SQLException {
		StringBuffer insert = new StringBuffer("insert into ").append(schemaName)
				.append(".PTable (attribute, symbol, predicate) values (?,?,?)");

		List<List<String>> data = new ArrayList<>();
		List<String> row = null;
		// adding every insert into the batch
		for (PTable t : constraints) {
			row = Arrays.asList(t.attribute, t.symbol, t.predicate);
			data.add(row);
		}
		execUpdates(insert.toString(), data);
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#insertSymbolicValueCache(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void insertSymbolicValueCache(String schemaName, String symbol, String value) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			// creating insert sql statement
			StringBuffer insert = new StringBuffer("insert into ").append(schemaName)
					.append(".symbol_value_cache (symbol, value) values ('" + symbol + "'," + value + ")");
			ps = conn.prepareStatement(insert.toString());
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (ps != null) {
				ps.close();
			}
		}
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#poseQuery(java.lang.String)
	 */
	public int poseQuery(String sql) throws SQLException {
		Connection conn = null;
		Statement statement = null;
		int cardinality = 0;
		try {
			conn = getConnection();
			statement = conn.createStatement();
			// creating insert sql statement
			ResultSet rs = statement.executeQuery(sql);
			if (rs.next()) {
				cardinality = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
		return cardinality;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getTableList(java.lang.String)
	 */
	public List<TableSchema> getTableList(String schema) throws SQLException {
		Connection conn = null;
		PreparedStatement s = null;
		List<TableSchema> tableList = new ArrayList<>();
		try {
			conn = getConnection();
			String query = new String(
					"select table_name from information_schema.tables where table_schema = ? and table_name <> 'PTable'");
			s = conn.prepareStatement(query);
			s.setString(1, schema);
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				tableList.add(getOutputSchema(schema, rs.getString(1)));
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return tableList;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#existTable(edu.rit.wagen.dto.TableSchema)
	 */
	public boolean existTable(TableSchema table) throws SQLException {
		Connection conn = null;
		PreparedStatement s = null;
		boolean exists = Boolean.FALSE;
		try {
			conn = getConnection();
			String query = new String(
					"select table_name from information_schema.tables where table_schema = ? and table_name = ?");
			s = conn.prepareStatement(query);
			s.setString(1, table.getSchemaName());
			s.setString(2, table.getTableName());
			ResultSet rs = s.executeQuery();
			exists = rs.next();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return exists;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getPredicates(java.lang.String, java.lang.String)
	 */
	public List<Predicate> getPredicates(String schema, String symbol) throws Exception {
		Connection conn = null;
		PreparedStatement s = null;
		List<Predicate> predicateList = null;
		try {
			conn = getConnection();
			String query = new String("select predicate from " + schema + ".ptable where symbol = ?");
			s = conn.prepareStatement(query);
			s.setString(1, symbol);
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				if (predicateList == null) {
					predicateList = new ArrayList<>();
				}
				predicateList.add(Utils.getPredicate(rs.getString(1)));
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return predicateList;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getAttributesPredicates(java.util.List, java.lang.String)
	 */
	public List<String> getAttributesPredicates(List<String> atts, String schema) throws Exception {
		Connection conn = null;
		PreparedStatement s = null;
		List<String> attsList = null;
		try {
			conn = getConnection();
			String query = new String("select distinct attribute from " + schema + ".ptable where attribute in (?)");
			String inClause = atts.stream().map(att -> "'" + att + "'").collect(Collectors.joining(", "));
			query = query.replace("?", inClause);
			s = conn.prepareStatement(query);
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				if (attsList == null) {
					attsList = new ArrayList<>();
				}
				attsList.add(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return attsList;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getPredicates(java.lang.String)
	 */
	public List<Predicate> getPredicates(String schema) throws Exception {
		Connection conn = null;
		PreparedStatement s = null;
		List<Predicate> predicateList = null;
		try {
			conn = getConnection();
			String query = new String("select predicate from " + schema + ".ptable");
			s = conn.prepareStatement(query);
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				if (predicateList == null) {
					predicateList = new ArrayList<>();
				}
				predicateList.add(Utils.getPredicate(rs.getString(1)));
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return predicateList;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getSymbols(java.lang.String)
	 */
	public List<String> getSymbols(String schema) throws Exception {
		Connection conn = null;
		PreparedStatement s = null;
		List<String> predicateList = null;
		try {
			conn = getConnection();
			String query = new String("select distinct (symbol) from " + schema + ".ptable");
			s = conn.prepareStatement(query);
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				if (predicateList == null) {
					predicateList = new ArrayList<>();
				}
				predicateList.add(rs.getString(1));
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (s != null) {
				s.close();
			}
		}
		return predicateList;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#createPTable(java.lang.String)
	 */
	public void createPTable(String schemaName) throws SQLException {
		execCommand(PTABLE.replaceAll("<SCHEMA_NAME>", schemaName));
		execCommand(SYMBOL_INDEX.replaceAll("<SCHEMA_NAME>", schemaName));
		execCommand(ATT_INDEX.replaceAll("<SCHEMA_NAME>", schemaName));
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#closedConnnection()
	 */
	public void closedConnnection() throws SQLException {
		conn.close();
	}

	/**
	 * Creates the table string.
	 *
	 * @param table the table
	 * @return the string
	 */
	private String createTableString(TableSchema table) {
		StringBuffer sb = new StringBuffer("CREATE TABLE ").append(table.getSchemaName()).append(".")
				.append(table.getTableName()).append("( id_sdb int primary key auto_increment, ");
		String columns = table.getColNames().stream().collect(Collectors.joining(" varchar(200), "));
		sb.append(columns);
		sb.append(" varchar(200))");
		return sb.toString();
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 * @throws SQLException the SQL exception
	 */
	private Connection getConnection() throws SQLException {
		if (conn == null) {
			conn = DriverManager.getConnection(URL, USER, PASSWORD);
		}
		return conn;
	}
}
