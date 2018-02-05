package edu.rit.wagen.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class Schema.
 * @author Maria Cepeda
 */
public class Schema {

	/** The name. */
	private String name;
	
	/** The tables. */
	private List<TableDescription> tables;
	
	/**
	 * Instantiates a new schema.
	 *
	 * @param name the name
	 */
	public Schema(String name) {
		this.name = name;
		this.tables = new ArrayList<>();
	}
	
	/**
	 * Instantiates a new schema.
	 *
	 * @param name the name
	 * @param tables the tables
	 */
	public Schema(String name, List<TableDescription> tables) {
		this.name = name;
		this.tables = tables;
	}
	
	/**
	 * Adds the table.
	 *
	 * @param newTable the new table
	 */
	public void addTable(TableDescription newTable) {
		this.tables.add(newTable);
	}

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name.
	 *
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the tables.
	 *
	 * @return the tables
	 */
	public List<TableDescription> getTables() {
		return tables;
	}

	/**
	 * Sets the tables.
	 *
	 * @param tables the tables to set
	 */
	public void setTables(List<TableDescription> tables) {
		this.tables = tables;
	}
	
}
