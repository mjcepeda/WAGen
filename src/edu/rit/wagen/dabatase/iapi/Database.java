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

	public void execCommand(String command) throws SQLException;

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

	public TableSchema getOutputSchema(String schema, String table) throws SQLException;

	/**
	 * Insert a list of symbolic data in a table.
	 *
	 * @param inserts
	 *            List of insert statements
	 * @throws SQLException
	 *             the SQL exception
	 */
	public void insertSymbolicData(List<String> inserts) throws SQLException;

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

	public void insertConstraints(String schemaName, List<PTable> constraints) throws SQLException;

	public TableSchema getReferencerTable(String realSchema, TableSchema table, String referencedColumn)
			throws SQLException;

	public List<TableSchema> getTableList(String schema) throws SQLException;

	public String getSymbolValueCache(String schema, String symbol) throws SQLException;

	public List<Predicate> getPredicates(String schema, String symbol) throws Exception;

	public void insertSymbolicValueCache(String schemaName, String symbol, String value) throws SQLException;

	public String getPredicateValueCache(String schema, String pattern) throws SQLException;

	public List<Tuple> getTuplesForUpdate(TableSchema table, String column) throws Exception;

	public boolean existTable(TableSchema table) throws Exception;

	public List<Predicate> getPredicates(String schema) throws Exception;
	
	public List<String> getSymbols(String schema) throws Exception;
	
	public List<String> getAttributesPredicates(List<String> atts, String schema) throws Exception;
	
	public void createPTable(String schema) throws SQLException;
}
