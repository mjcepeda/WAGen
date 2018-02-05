package edu.rit.wagen.dto;

import java.util.List;
import java.util.Map;

public class Tuple {
	/**
	 * K - column name
	 * V - column value
	 */
	private Map<String, String> values;
	//TODO MJCG What is this for?
	private List<Attribute> attributes;

	/**
	 * @return the values
	 */
	public Map<String, String> getValues() {
		return values;
	}

	/**
	 * @param values the values to set
	 */
	public void setValues(Map<String, String> values) {
		this.values = values;
	}
	
}
