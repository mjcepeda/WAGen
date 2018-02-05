package edu.rit.wagen.dto;

import java.util.List;

/**
 * Schema definition table.
 *
 * @author Maria Cepeda
 */
public class TableDescription {

	/** The schema name. */
	public String schemaName;
	
	/** The name. */
	public String name;
	
	//TODO MJCG Do I must consider attribute type (int, varchar, numeric)
	/** The columns. */
	public List<String> columns;
	
	/** The constraints. */
	public List<Constraint> constraints;

	/**
	 * Instantiates a new table description.
	 *
	 * @param schemaName the schema name
	 * @param name the name
	 * @param columns the columns
	 */
	public TableDescription(String schemaName, String name, List<String> columns) {
		// checking required info
		if (validate(schemaName, name, columns)) {
			this.schemaName = schemaName;
			this.name = name;
			this.columns = columns;
		} else {
			throw new Error("Missing required information");
		}
	}

	/**
	 * Validate.
	 *
	 * @param schemaName the schema name
	 * @param name the name
	 * @param columns the columns
	 * @return true, if successful
	 */
	private boolean validate(String schemaName, String name, List<String> columns) {
		return (schemaName != null && !schemaName.trim().equals("") && name != null && !name.trim().equals("")
				&& columns != null && columns.size() > 0);
	}
}
