package edu.rit.wagen.database.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
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
 * 
 * @author Maria Cepeda
 */
public class DatabaseImpl implements Database {

	/** The Constant DRIVER. */
	// database data
	private static final String DRIVER = "com.mysql.jdbc.Driver";

	/** The Constant URL. */
	private final static String URL = "jdbc:mysql://localhost/?zeroDateTimeBehavior=convertToNull";

	/** The Constant USER. */
	private final static String USER = "root";

	/** The Constant PASSWORD. */
	private final static String PASSWORD = "mientras";

	static {
		try {
			Class.forName(DRIVER).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void execCommands(List<String> commands) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			Connection connection = getConnection();
			Statement statement = connection.createStatement();

			for (String command : commands) {
				statement.addBatch(command);
			}
			statement.executeBatch();

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.rollback();
				conn.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}

	public void execCommand(String command) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(command);
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
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
			if (conn != null) {
				conn.close();
			}
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
	public boolean createTable(TableSchema tableSchema) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		boolean created = Boolean.FALSE;
		try {
			conn = getConnection();
			// create user table
			String sqlCreateTable = createTableString(tableSchema);
			ps = conn.prepareStatement(sqlCreateTable);
			created = ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
		return !created;
	}

	/**
	 * Execute a batch of inserts.
	 *
	 * @param inserts
	 *            the inserts
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void insertSymbolicData(List<String> inserts) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;

		try {
			Connection connection = getConnection();
			Statement statement = connection.createStatement();

			for (String insert : inserts) {
				statement.addBatch(insert);
			}
			statement.executeBatch();

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (ps != null) {
				ps.close();
			}
		}

	}

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
				colNames.add(rsmd.getColumnName(i));
				colTypes.add(rsmd.getColumnTypeName(i));
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
				colContraints.add(
						new Constraint(Utils.getConstraint(constraint), column, referenced_column, reference_table));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (s != null) {
				s.close();
			}
		}
		return new TableSchema(schema, table, colNames, colTypes, colContraints);
	}

	// TODO MJCG Maybe this method is not neccessary, check if this info in the
	// TableSchema
	public TableSchema getReferencedTable(String realSchema, TableSchema table, String referencedColumn)
			throws SQLException {
		Connection conn = null;
		PreparedStatement s = null;
		String referencedTable = null;
		TableSchema tableR = null;
		try {
			conn = getConnection();
			String query = new String(
					"select referenced_table_name from information_schema.key_column_usage where table_schema = ? "
							+ "and table_name = ? and referenced_column_name = ?");
			s = conn.prepareStatement(query);
			s.setString(1, realSchema);
			s.setString(2, table.getTableName());
			s.setString(3, referencedColumn);
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
			if (conn != null) {
				conn.close();
			}
			if (s != null) {
				s.close();
			}
		}
		return tableR;
	}

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
			if (conn != null) {
				conn.close();
			}
			if (s != null) {
				s.close();
			}
		}
		return value;
	}

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
					m.put(attr, value);
				}
				Tuple tuple = new Tuple();
				tuple.setValues(m);
				data.add(tuple);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (statement != null) {
				statement.close();
			}
		}
		return data;
	}

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
			if (conn != null) {
				conn.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
		return symbol;
	}

	@Override
	public void insertConstraints(String schemaName, List<PTable> constraints) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;

		try {
			// TODO MJCG Why the predicate in the paper has []?
			conn = getConnection();
			// creating insert sql statement
			StringBuffer insert = new StringBuffer("insert into ").append(schemaName)
					.append(".PTable (symbol, predicate) values ('%1','%2')");
			ps = conn.prepareStatement(insert.toString());
			// adding every insert into the batch
			for (PTable t : constraints) {
				String s = insert.toString().replaceAll("%1", t.symbol).replaceAll("%2", t.predicate);
				ps.addBatch(s);
			}
			// executing the batch
			ps.executeBatch();

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}

	// public void insertData (String schemaName, String tableName, List<Tuple>
	// data) throws SQLException {
	// Connection conn = null;
	// PreparedStatement ps = null;
	//
	// try {
	// // TODO This method will the method to insert data in the final concrete
	// database
	// conn = getConnection();
	// // creating insert sql statement
	// StringBuffer insert = new StringBuffer("insert into ").append(schemaName)
	// .append(".").append("PTable (symbol, predicate) values ('%1','%2')");
	// ps = conn.prepareStatement(insert.toString());
	// // adding every insert into the batch
	// for (PTable t : constraints) {
	// String s = insert.toString().replaceAll("%1", t.symbol).replaceAll("%2",
	// t.predicate);
	// ps.addBatch(s);
	// }
	// // executing the batch
	// ps.executeBatch();
	//
	// } catch (SQLException e) {
	// e.printStackTrace();
	// throw e;
	// } finally {
	// if (conn != null) {
	// conn.close();
	// }
	// if (ps != null) {
	// ps.close();
	// }
	// }
	// }
	//
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
			if (conn != null) {
				conn.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
	}

	public List<TableSchema> getTableList(String schema) throws SQLException {
		Connection conn = null;
		PreparedStatement s = null;
		List<TableSchema> tableList = new ArrayList<>();
		try {
			conn = getConnection();
			String query = new String("select table_name from information_schema.tables where table_schema = ?");
			s = conn.prepareStatement(query);
			s.setString(1, schema);
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				tableList.add(getOutputSchema(schema, rs.getString(1)));
			}
			//TODO MJCG Find a way to order the table statements so I do not have problems with constraints
			//sort the list so there is no problems with the foreign keys
			tableList.sort(Comparator.comparing(t -> t.getConstraints(ConstraintType.FK), Comparator.comparing(List::size)));
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (s != null) {
				s.close();
			}
		}
		return tableList;
	}

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
			if (conn != null) {
				conn.close();
			}
			if (s != null) {
				s.close();
			}
		}
		return predicateList;
	}

	/**
	 * Creates the table string.
	 *
	 * @param table
	 *            the table
	 * @return the string
	 */
	private String createTableString(TableSchema table) {
		StringBuffer sb = new StringBuffer("CREATE TABLE ").append(table.getSchemaName()).append(".")
				.append(table.getTableName()).append("( ");
		String columns = table.getColNames().stream().collect(Collectors.joining(" varchar(200), "));
		sb.append(columns);
		sb.append(" varchar(200))");
		return sb.toString();
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 * @throws SQLException
	 *             the SQL exception
	 */
	private Connection getConnection() throws SQLException {
		return DriverManager.getConnection(URL, USER, PASSWORD);
	}
}
