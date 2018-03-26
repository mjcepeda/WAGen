package edu.rit.wagen.dto;

public class Predicate {

	public String attribute;
	
	public String symbol;

	public String op;

	public String condition;

	public Predicate() {
		symbol = null;
		op = null;
		condition = null;
	}

	public Predicate(String symbol, String op, String c) {
		this.attribute = symbol.replaceAll("[0-9]", "");
		this.symbol = symbol.trim().toUpperCase();
		this.op = op.trim().toUpperCase();
		this.condition = c.trim().toUpperCase();
	}

	public String getPredicate() {
		return symbol + op + condition;
	}
	
	public String getPattern() {
		return attribute + op + condition;
	}
	
	public String toString() {
		return symbol + op + condition;
	}
}
