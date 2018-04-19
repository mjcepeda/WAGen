package edu.rit.wagen.dabatase.iapi;

import java.sql.SQLException;
import java.util.List;

import edu.rit.wagen.dto.PTable;
import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;

/**
 * The Interface Database.
 * 
 * @author Maria Cepeda
 */
public interface Database {

	/**
	 * Exec command.
	 *
	 * @param command
	 *            the command
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void execCommand(String command) throws SQLException;

	/**
	 * Exec commands.
	 *
	 * @param commands
	 *            the commands
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void execCommands(List<String> commands) throws SQLException;

	/**
	 * Create database schema and PTable.
	 *
	 * @param schema
	 *            the schema
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void createSchema(String schema) throws SQLException;

	/**
	 * Creates the table.
	 *
	 * @param table
	 *            the table
	 * @return true, if successful
	 * @throws SQLException
	 *             the SQL exception
	 */
	public boolean createSymbolicTable(TableSchema table) throws SQLException;

	/**
	 * Gets the output schema.
	 *
	 * @param schema
	 *            the schema
	 * @param table
	 *            the table
	 * @return the output schema
	 * @throws SQLException
	 *             the SQL exception
	 */
	public TableSchema getOutputSchema(String schema, String table) throws SQLException;

	/**
	 * Gets the data.
	 *
	 * @param table
	 *            the table
	 * @return the data
	 * @throws SQLException
	 *             the SQL exception
	 */
	public List<Tuple> getData(TableSchema table) throws SQLException;

	/**
	 * Insert constraints into PTable.
	 *
	 * @param schemaName
	 *            the schema name
	 * @param constraints
	 *            the constraints
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void insertConstraints(String schemaName, List<PTable> constraints) throws SQLException;

	/**
	 * Gets the referencer table.
	 *
	 * @param realSchema
	 *            the real schema
	 * @param table
	 *            the table
	 * @param referencedColumn
	 *            the referenced column
	 * @param referencerColumn
	 *            the referencer column
	 * @return the referencer table
	 * @throws SQLException
	 *             the SQL exception
	 */
	public TableSchema getReferencerTable(String realSchema, TableSchema table, String referencedColumn,
			String referencerColumn) throws SQLException;

	/**
	 * Gets the list of tables in the schema.
	 *
	 * @param schema
	 *            the schema
	 * @return the table list
	 * @throws SQLException
	 *             the SQL exception
	 */
	public List<TableSchema> getTableList(String schema) throws SQLException;

	/**
	 * Gets the symbol value cache.
	 *
	 * @param schema
	 *            the schema
	 * @param symbol
	 *            the symbol
	 * @return the symbol value cache
	 * @throws SQLException
	 *             the SQL exception
	 */
	public String getSymbolValueCache(String schema, String symbol) throws SQLException;

	/**
	 * Gets the predicates.
	 *
	 * @param schema
	 *            the schema
	 * @param symbol
	 *            the symbol
	 * @return the predicates
	 * @throws Exception
	 *             the exception
	 */
	public List<Predicate> getPredicates(String schema, String symbol) throws Exception;

	/**
	 * Insert symbolic value cache.
	 *
	 * @param schemaName
	 *            the schema name
	 * @param symbol
	 *            the symbol
	 * @param value
	 *            the value
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void insertSymbolicValueCache(String schemaName, String symbol, String value) throws SQLException;

	/**
	 * Gets the predicate value cache.
	 *
	 * @param schema
	 *            the schema
	 * @param pattern
	 *            the pattern
	 * @return the predicate value cache
	 * @throws SQLException
	 *             the SQL exception
	 */
	public String getPredicateValueCache(String schema, String pattern) throws SQLException;

	/**
	 * Exist table.
	 *
	 * @param table
	 *            the table
	 * @return true, if successful
	 * @throws Exception
	 *             the exception
	 */
	public boolean existTable(TableSchema table) throws Exception;

	/**
	 * Gets the predicates.
	 *
	 * @param schema
	 *            the schema
	 * @return the predicates
	 * @throws Exception
	 *             the exception
	 */
	public List<Predicate> getPredicates(String schema) throws Exception;

	/**
	 * Gets the symbols.
	 *
	 * @param schema
	 *            the schema
	 * @return the symbols
	 * @throws Exception
	 *             the exception
	 */
	public List<String> getSymbols(String schema) throws Exception;

	/**
	 * Gets the list of attributes from the PTable.
	 *
	 * @param atts
	 *            List of all attributes
	 * @param schema
	 *            the schema
	 * @return list of attributes in PTable
	 * @throws Exception
	 *             the exception
	 */
	public List<String> getAttributesPredicates(List<String> atts, String schema) throws Exception;

	/**
	 * Creates the Ptable.
	 *
	 * @param schema
	 *            the schema
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void createPTable(String schema) throws SQLException;

	/**
	 * Count data.
	 *
	 * @param table
	 *            the table
	 * @return the int
	 * @throws SQLException
	 *             the SQL exception
	 */
	public int countData(TableSchema table) throws SQLException;

	/**
	 * Closed connnection.
	 *
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void closedConnnection() throws SQLException;
	
	/**
	 * Gets the data.
	 *
	 * @param table the table
	 * @param page the page
	 * @param limit the limit
	 * @return the data
	 * @throws SQLException the SQL exception
	 */
	public List<Tuple> getData(TableSchema table, int page, int limit) throws SQLException;
	
	/**
	 * Pose query.
	 *
	 * @param sql the sql
	 * @return the int
	 * @throws SQLException the SQL exception
	 */
	public int poseQuery(String sql) throws SQLException;
}
