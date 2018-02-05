package edu.rit.wagen.dabatase.iapi;

import java.sql.SQLException;
import java.util.List;

import edu.rit.wagen.dto.Schema;
import edu.rit.wagen.dto.TableDescription;
import edu.rit.wagen.dto.Tuple;

/**
 * The Interface Database.
 * @author Maria Cepeda
 */
public interface Database {
	
	/**
	 * Create database schema and PTable.
	 *
	 * @param schema the schema
	 * @throws SQLException the SQL exception
	 */
	public void createSchema(Schema schema) throws SQLException;

	/**
	 * Creates the table.
	 *
	 * @param table the table
	 * @return true, if successful
	 * @throws SQLException the SQL exception
	 */
	public boolean createTable(TableDescription table) throws SQLException;

	/**
	 * Insert a list of rows in a table.
	 *
	 * @param inserts            List of insert statements
	 * @throws SQLException the SQL exception
	 */
	public void insertRows(List<String> inserts) throws SQLException;

	/**
	 * Gets the data.
	 *
	 * @param table the table
	 * @return the data
	 * @throws SQLException the SQL exception
	 */
	public List<Tuple> getData(TableDescription table) throws SQLException;

}
