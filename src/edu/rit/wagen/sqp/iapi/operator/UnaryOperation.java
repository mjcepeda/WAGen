package edu.rit.wagen.sqp.iapi.operator;

/**
 * The Class UnaryOperation.
 */
public abstract class UnaryOperation extends RAOperator{
	
	/** The source. */
	public RAOperator source;
	
	/**
	 * Instantiates a new unary operation.
	 *
	 * @param name the name
	 */
	public UnaryOperation(RAOperator source, String sbSchema, String realSchema) {
		super(sbSchema, realSchema);
		this.source = source;
		this._counter = 0;
	}
	
	/**
	 * @return the source
	 */
	public RAOperator getSource() {
		return source;
	}

	/**
	 * @param source the source to set
	 */
	public void setSource(RAOperator source) {
		this.source = source;
	}
}
