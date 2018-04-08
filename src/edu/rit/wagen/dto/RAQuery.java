package edu.rit.wagen.dto;

import java.util.Map;

/**
 * The Class RAQuery.
 * @author Maria Cepeda
 */
public class RAQuery {
	
	/** The query. */
	private String query;
	
	/** The constraints. */
	private Map<Integer, RAAnnotation> constraints;
	
	/**
	 * Instantiates a new RA query.
	 *
	 * @param q the q
	 * @param c the c
	 */
	public RAQuery(String q, Map<Integer, RAAnnotation> c) {
		this.query = q;
		this.constraints = c;
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
}
