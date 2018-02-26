package edu.rit.wagen.dto;


public class Predicate {
	
	public String symbol;
	
	public String op;
	
	public String condition;
	
	public Predicate(String att, String op, String c) {
		this.symbol = att.trim().toUpperCase();
		this.op = op.trim().toUpperCase();
		this.condition = c.trim().toUpperCase();
	}
	
	public String toString() {
		return symbol + op + condition;
	}
}
