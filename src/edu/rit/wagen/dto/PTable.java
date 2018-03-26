package edu.rit.wagen.dto;

public class PTable {

	public String attribute;
	public String symbol;
	public String predicate;
	
	public PTable(String a, String s, String p) {
		this.attribute = a;
		this.symbol=s;
		this.predicate = p;
	}
	
	public String toString() {
		return attribute + ": " + symbol + ": " + predicate;
	}
}
