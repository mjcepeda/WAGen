package edu.rit.wagen.dto;

import java.util.List;

/**
 * The Class IntegratedSDB.
 * @author Maria Cepeda
 */
public class IntegratedSDB {

	/** The schema. */
	private String schema;
	
	/** The sdbs. */
	private List<RAQuery> sdbs;
	
	/**
	 * Instantiates a new integrated SDB.
	 *
	 * @param name the name
	 * @param sdbs the sdbs
	 */
	public IntegratedSDB(String name, List<RAQuery> sdbs) {
		this.schema = name;
		this.sdbs = sdbs;
	}

	/**
	 * Gets the schema.
	 *
	 * @return the schema
	 */
	public String getSchema() {
		return schema;
	}

	/**
	 * Sets the schema.
	 *
	 * @param schema the new schema
	 */
	public void setSchema(String schema) {
		this.schema = schema;
	}

	/**
	 * Gets the sdbs.
	 *
	 * @return the sdbs
	 */
	public List<RAQuery> getSdbs() {
		return sdbs;
	}

	/**
	 * Sets the sdbs.
	 *
	 * @param sdbs the new sdbs
	 */
	public void setSdbs(List<RAQuery> sdbs) {
		this.sdbs = sdbs;
	}
	
}
