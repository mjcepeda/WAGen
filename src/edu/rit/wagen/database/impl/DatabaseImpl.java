package edu.rit.wagen.database.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import edu.rit.wagen.dabatase.iapi.Database;
import edu.rit.wagen.dto.Schema;
import edu.rit.wagen.dto.TableDescription;
import edu.rit.wagen.dto.Tuple;

/**
 * The Class DatabaseImpl.
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
	private final static String PASSWORD = "******";
	
	/** The Constant PTABLE. */
	private final static String PTABLE = "CREATE TABLE NAME.PTABLE (SYMBOL VARCHAR(100), PREDICATE varchar(200))";

	static {
		try {
			Class.forName(DRIVER).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#createSchema(edu.rit.wagen.dto.Schema)
	 */
	public void createSchema(Schema schema) throws SQLException {
		// TODO MJCG - Make this mehtod atomic
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection();
			String createSchema = "CREATE DATABASE " + schema.getName();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(createSchema);
			int result = ps.executeUpdate();
			// database created succesfully
			if (result == 1) {
				// create Ptable
				String sql = PTABLE.replaceAll("NAME", schema.getName());
				ps.execute(sql);

				conn.commit();
			}
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

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#createTable(edu.rit.wagen.dto.TableDescription)
	 */
	public boolean createTable(TableDescription table) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		boolean created = Boolean.FALSE;
		try {
			conn = getConnection();
			// create user table
			String sqlCreateTable = createTableString(table);
			ps = conn.prepareStatement(sqlCreateTable);
			created = ps.execute();
		} catch (

		SQLException e) {
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
	 * @param inserts the inserts
	 * @throws SQLException the SQL exception
	 */
	public void insertRows(List<String> inserts) throws SQLException {
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

	/* (non-Javadoc)
	 * @see edu.rit.wagen.dabatase.iapi.Database#getData(edu.rit.wagen.dto.TableDescription)
	 */
	public List<Tuple> getData(TableDescription table) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		List<Tuple> data = new ArrayList<Tuple>();
		try {
			Connection connection = getConnection();
			Statement statement = connection.createStatement();

			String sql = "select * from " + table.name;
			ResultSet rs = statement.executeQuery(sql);
			int index =0;
			while (rs.next()) {
				Map<String, String> m = new HashMap<>();
				for (String attr : table.columns) {
					String value = rs.getString(attr);
					m.put(attr, value);
				}
				Tuple tuple = new Tuple();
				tuple.setValues(m);
				data.set(index, tuple);
				index++;
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

	/**
	 * Creates the table string.
	 *
	 * @param table the table
	 * @return the string
	 */
	private String createTableString(TableDescription table) {
		StringBuffer sb = new StringBuffer("CREATE TABLE ").append(table.schemaName).append(".").append(table.name)
				.append("( ");
		String columns = table.columns.stream().collect(Collectors.joining(" varchar(100), "));
		sb.append(columns);
		sb.append(" varchar(100))");
		return sb.toString();
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 * @throws SQLException the SQL exception
	 */
	private Connection getConnection() throws SQLException {
		return DriverManager.getConnection(URL, USER, PASSWORD);
	}
}
