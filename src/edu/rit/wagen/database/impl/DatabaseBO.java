package edu.rit.wagen.database.impl;

import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * The Class DatabaseBO.
 * @author Maria Cepeda
 */
public class DatabaseBO {

	/** The db. */
	private DatabaseImpl db = new DatabaseImpl();

	/**
	 * Creates the DB.
	 *
	 * @param commands the commands
	 * @param prefix the prefix
	 * @return the string
	 * @throws SQLException the SQL exception
	 */
	public String createDB(List<String> commands, String prefix) throws SQLException {
		// this list only has the create table statements
		// first thing, create the schema
		Random random = new Random();
		String schemaName = prefix + random.nextInt(1000);
		db.createSchema(schemaName);
		// second step, add the schema name to all create table statements
		List<String> updatedCommands = commands.stream()
				.map(s -> "CREATE TABLE " + schemaName + "." + s.toUpperCase().replaceAll("CREATE TABLE ", "").trim())
				.collect(Collectors.toList());
		// third, execute the create table statements
		db.execCommands(updatedCommands);
		return schemaName;
	}

	/**
	 * Creates the schema.
	 *
	 * @param schemaName the schema name
	 * @throws Exception the exception
	 */
	public void createSchema(String schemaName) throws Exception {
		db.createSchema(schemaName);
	}

	/**
	 * Creates the P table.
	 *
	 * @param schemaName the schema name
	 * @throws Exception the exception
	 */
	public void createPTable(String schemaName) throws Exception {
		db.createPTable(schemaName);
	}

	/**
	 * Execute command.
	 *
	 * @param command the command
	 * @throws Exception the exception
	 */
	public void executeCommand(String command) throws Exception {
		db.execCommand(command);
	}

	/**
	 * Drop schema.
	 *
	 * @param schema the schema
	 * @throws SQLException the SQL exception
	 */
	public void dropSchema(String schema) throws SQLException {
		db.execCommand("drop database " + schema);
	}
	
	/**
	 * Close connection.
	 *
	 * @throws SQLException the SQL exception
	 */
	public void closeConnection() throws SQLException {
		db.closedConnnection();
	}
}
