package edu.rit.wagen.dto;

public class PTable {

	public String symbol;
	public String predicate;
	
	public PTable(String s, String p) {
		this.symbol=s;
		this.predicate = p;
	}
	
	public String toString() {
		return symbol + ": " + predicate;
	}
}
