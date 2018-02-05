package edu.rit.wagen.sqp.iapi.operator;

/**
 * The Class UnaryOperation.
 */
public abstract class UnaryOperation extends Operator{
	
	/** The source. */
	private Operator source;
	
	/**
	 * Instantiates a new unary operation.
	 *
	 * @param name the name
	 */
	public UnaryOperation(Operator source) {
		this.source = source;
	}
	
	/**
	 * @return the source
	 */
	public Operator getSource() {
		return source;
	}

	/**
	 * @param source the source to set
	 */
	public void setSource(Operator source) {
		this.source = source;
	}
}
