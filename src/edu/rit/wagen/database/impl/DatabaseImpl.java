package edu.rit.wagen.database.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import edu.rit.wagen.dabatase.iapi.Database;
import edu.rit.wagen.dto.PTable;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;

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
		Connection conn = null;
		Statement s = null;
		try {
			conn = getConnection();
			s = conn.createStatement();
			ResultSet rs = s.executeQuery("select * from " + schema + "." + table);
			ResultSetMetaData rsmd = rs.getMetaData();
			int numCols = rsmd.getColumnCount();
			for (int i = 1; i <= numCols; i++) {
				// Important: Use getColumnLabel() to get new column names
				// specified
				// in AS or in CREATE VIEW. For some JDBC drivers,
				// getColumnName()
				// gives the original column names inside base tables.
				colNames.add(rsmd.getColumnLabel(i));
				colTypes.add(rsmd.getColumnTypeName(i));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (s != null) {
				s.close();
			}
		}
		return new TableSchema(schema, table, colNames, colTypes);
	}

	public List<Tuple> getData(TableSchema table) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		List<Tuple> data = new ArrayList<Tuple>();
		try {
			Connection connection = getConnection();
			Statement statement = connection.createStatement();

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
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
		return data;
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
		} finally {
			if (conn != null) {
				conn.close();
			}
			if (ps != null) {
				ps.close();
			}
		}
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
		String columns = table.getColNames().stream().collect(Collectors.joining(" varchar(100), "));
		sb.append(columns);
		sb.append(" varchar(100))");
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
