package edu.rit.wagen.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import edu.rit.wagen.utils.Utils.ConstraintType;

/**
 * The Class TableSchema.
 * @author Maria Cepeda
 */
public class TableSchema {
	
	/** The schema name. */
	protected String schemaName;
	
	/** The table name. */
	protected String tableName;
	
	/** The col names. */
	protected ArrayList<String> colNames;
	
	/** The col types. */
	protected ArrayList<String> colTypes;
	
	/** The col values. */
	protected ArrayList<String> colValues;
	
	/** The constraints. */
	protected List<Constraint> constraints;

	/**
	 * Instantiates a new table schema.
	 *
	 * @param schemaName the schema name
	 * @param tableName the table name
	 * @param colNames the col names
	 * @param colTypes the col types
	 * @param c the c
	 */
	public TableSchema(String schemaName, String tableName, ArrayList<String> colNames, ArrayList<String> colTypes,
			List<Constraint> c) {
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.colNames = colNames;
		this.colTypes = colTypes;
		this.constraints = c;
	}

	/**
	 * Gets the schema name.
	 *
	 * @return the schema name
	 */
	public String getSchemaName() {
		return schemaName;
	}

	/**
	 * Sets the schema name.
	 *
	 * @param name the new schema name
	 */
	public void setSchemaName(String name) {
		this.schemaName = name;
	}

	/**
	 * Gets the table name.
	 *
	 * @return the table name
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Gets the col names.
	 *
	 * @return the col names
	 */
	public ArrayList<String> getColNames() {
		return colNames;
	}

	/**
	 * Gets the col types.
	 *
	 * @return the col types
	 */
	public ArrayList<String> getColTypes() {
		return colTypes;
	}

	/**
	 * Gets the constraints.
	 *
	 * @return the constraints
	 */
	public List<Constraint> getConstraints() {
		return constraints;
	}

	/**
	 * Gets the constraints.
	 *
	 * @param type the type
	 * @return the constraints
	 */
	public List<Constraint> getConstraints(ConstraintType type) {
		return constraints.stream().filter(c -> c.type == type).collect(Collectors.toList());
	}

	/**
	 * To print string.
	 *
	 * @return the string
	 */
	public String toPrintString() {
		String s = tableName;
		s += "(";
		for (int i = 0; i < colNames.size(); i++) {
			if (i > 0)
				s += ", ";
			s += colNames.get(i);
			s += " ";
			s += colTypes.get(i);
		}
		s += ")";
		return s;
	}

}
