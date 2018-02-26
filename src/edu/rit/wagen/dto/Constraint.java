package edu.rit.wagen.dto;

import edu.rit.wagen.utils.Utils.ConstraintType;

public class Constraint {

	public ConstraintType type;
	public String column;
	public String referencedColumn;
	public String referencedTable;
	//if case the constraint is CHECK
	public String predicate;

	public Constraint(ConstraintType type, String predicate) {
		this.type = type;
		this.predicate = predicate;
	}

	public Constraint(ConstraintType type, String c, String rc, String rtable) {
		this.type = type;
		this.column = c;
		this.referencedColumn = rc;
		this.referencedTable = rtable;
	}
}
