package edu.rit.wagen.database.impl;

import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class DatabaseBO {

	private DatabaseImpl db = new DatabaseImpl();

	public String createDB(List<String> commands) {
		boolean created = Boolean.TRUE;
		// this list only has the create table statements
		// first thing, create the schema
		Random random = new Random();
		String schemaName = "wagen" + random.nextInt(1000);
		try {
			db.createSchema(schemaName);
			// second step, add the schema name to all create table statements
			List<String> updatedCommands = commands.stream().map(
					s -> "CREATE TABLE " + schemaName + "." + s.toUpperCase().replaceAll("CREATE TABLE ", "").trim())
					.collect(Collectors.toList());
			// third, execute the create table statements
			db.execCommands(updatedCommands);
		} catch (SQLException e) {
			// TODO MJCG Maybe I can delete the schema in case of error
			e.printStackTrace();
		}
		return schemaName;
	}

	public void createSchema(String schemaName) throws Exception {
		db.createSchema(schemaName);
	}
	
	public void executeCommand (String command) throws Exception{
		db.execCommand(command);
	}

}