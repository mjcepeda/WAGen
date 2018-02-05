package edu.rit.wagen.dto;

import edu.rit.wagen.utils.Constants.ConstraintType;

public class Constraint {

	private ConstraintType type;
	private String predicate;

	public Constraint(ConstraintType type, String predicate) {
		this.type = type;
		this.predicate = predicate;
	}

	/**
	 * @return the type
	 */
	public ConstraintType getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(ConstraintType type) {
		this.type = type;
	}

	/**
	 * @return the predicate
	 */
	public String getPredicate() {
		return predicate;
	}

	/**
	 * @param predicate
	 *            the predicate to set
	 */
	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}
}
