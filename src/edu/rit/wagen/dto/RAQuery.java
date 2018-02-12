package edu.rit.wagen.dto;

import java.util.Map;

public class RAQuery {
	private String query;
	private Map<Integer, RAAnnotation> constraints;
	
	public RAQuery(String q, Map<Integer, RAAnnotation> c) {
		this.query = q;
		this.constraints = c;
	}
	
	public String getQuery(){
		return query;
	}

	/**
	 * @return the constraints
	 */
	public Map<Integer, RAAnnotation> getConstraints() {
		return constraints;
	}
}
