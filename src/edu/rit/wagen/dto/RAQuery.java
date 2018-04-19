package edu.rit.wagen.dto;

import java.util.Map;

/**
 * The Class RAQuery.
 */
public class RAQuery {
	
	/** The query. */
	private String query;
	
	/** The constraints. */
	private Map<Integer, RAAnnotation> constraints;
	
	/** The sql query. */
	private String sqlQuery;
	
	/** The cardinality. */
	private int cardinality;
	

	/** The sdb name. */
	private String sdbName;
	
	/**
	 * Instantiates a new RA query.
	 *
	 * @param q the q
	 * @param sqlQuery the sql query
	 * @param cardinality the cardinality
	 * @param c the c
	 */
	public RAQuery(String q, String sqlQuery, int cardinality, Map<Integer, RAAnnotation> c) {
		this.query = q;
		this.constraints = c;
		this.sqlQuery = sqlQuery;
		this.cardinality = cardinality;
	}
	
	/**
	 * Instantiates a new RA query.
	 *
	 * @param sdbName the sdb name
	 * @param sqlQuery the sql query
	 */
	public RAQuery(String sdbName, String sqlQuery) {
		this.sdbName = sdbName;
		this.sqlQuery = sqlQuery;
		this.cardinality = cardinality;
	}
	
	/**
	 * Gets the query.
	 *
	 * @return the query
	 */
	public String getQuery(){
		return query;
	}

	/**
	 * Gets the constraints.
	 *
	 * @return the constraints
	 */
	public Map<Integer, RAAnnotation> getConstraints() {
		return constraints;
	}

	/**
	 * Gets the sdb name.
	 *
	 * @return the sdb name
	 */
	public String getSdbName() {
		return sdbName;
	}

	/**
	 * Sets the sdb name.
	 *
	 * @param sdbName the new sdb name
	 */
	public void setSdbName(String sdbName) {
		this.sdbName = sdbName;
	}
	
	/**
	 * Gets the sql query.
	 *
	 * @return the sql query
	 */
	public String getSqlQuery() {
		return sqlQuery;
	}

	/**
	 * Gets the cardinality.
	 *
	 * @return the cardinality
	 */
	public int getCardinality() {
		return cardinality;
	}

	/**
	 * Sets the cardinality.
	 *
	 * @param cardinality the new cardinality
	 */
	public void setCardinality(int cardinality) {
		this.cardinality = cardinality;
	}
}
