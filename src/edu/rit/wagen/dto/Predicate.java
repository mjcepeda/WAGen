package edu.rit.wagen.dto;

/**
 * The Class Predicate.
 * @author Maria Cepeda
 */
public class Predicate {

	/** The attribute. */
	public String attribute;
	
	/** The symbol. */
	public String symbol;

	/** The op. */
	public String op;

	/** The condition. */
	public String condition;

	/**
	 * Instantiates a new predicate.
	 */
	public Predicate() {
		symbol = null;
		op = null;
		condition = null;
	}

	/**
	 * Instantiates a new predicate.
	 *
	 * @param symbol the symbol
	 * @param op the op
	 * @param c the c
	 */
	public Predicate(String symbol, String op, String c) {
		this.attribute = symbol.replaceAll("[0-9]", "");
		this.symbol = symbol.trim().toUpperCase();
		this.op = op.trim().toUpperCase();
		this.condition = c.trim().toUpperCase();
	}

	/**
	 * Gets the predicate.
	 *
	 * @return the predicate
	 */
	public String getPredicate() {
		return symbol + op + condition;
	}
	
	/**
	 * Gets the pattern.
	 *
	 * @return the pattern
	 */
	public String getPattern() {
		return attribute + op + condition;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return symbol + op + condition;
	}
}
