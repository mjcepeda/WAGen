package edu.rit.wagen.dto;

/**
 * The Class PTable.
 * @author Maria Cepeda
 */
public class PTable {

	/** The attribute. */
	public String attribute;
	
	/** The symbol. */
	public String symbol;
	
	/** The predicate. */
	public String predicate;
	
	/**
	 * Instantiates a new p table.
	 *
	 * @param a the a
	 * @param s the s
	 * @param p the p
	 */
	public PTable(String a, String s, String p) {
		this.attribute = a;
		this.symbol=s;
		this.predicate = p;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return attribute + ": " + symbol + ": " + predicate;
	}
}
