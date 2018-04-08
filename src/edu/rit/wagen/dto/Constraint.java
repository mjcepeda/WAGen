package edu.rit.wagen.dto;

import edu.rit.wagen.utils.Utils.ConstraintType;

/**
 * The Class Constraint.
 * @author Maria Cepeda
 */
public class Constraint {

	/** The type. */
	public ConstraintType type;
	
	/** The column. */
	public String column;
	
	/** The referenced column. */
	public String referencedColumn;
	
	/** The referenced table. */
	public String referencedTable;
	
	/**
	 * Instantiates a new constraint.
	 *
	 * @param type the type
	 */
	public Constraint(ConstraintType type) {
		this.type = type;
	}

	/**
	 * Instantiates a new constraint.
	 *
	 * @param type the type
	 * @param c the c
	 * @param rc the rc
	 * @param rtable the rtable
	 */
	public Constraint(ConstraintType type, String c, String rc, String rtable) {
		this.type = type;
		this.column = c;
		this.referencedColumn = rc;
		this.referencedTable = rtable;
	}
}
