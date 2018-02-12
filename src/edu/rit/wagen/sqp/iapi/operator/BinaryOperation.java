package edu.rit.wagen.sqp.iapi.operator;

/**
 * The Class BinaryOperation.
 */
public abstract class BinaryOperation extends RAOperator {

	/** The left source. */
	private RAOperator leftSource;

	/** The right source. */
	private RAOperator rightSource;

	
	public BinaryOperation(RAOperator leftSource, RAOperator rightSource, String sbSchema, String realSchema) {
		super(sbSchema, realSchema);
		this.leftSource = leftSource;
		this.rightSource = rightSource;
		this._counter = 0;
	}

	/**
	 * Gets the left source.
	 *
	 * @return the leftSource
	 */
	public RAOperator getLeftSource() {
		return leftSource;
	}

	/**
	 * Sets the left source.
	 *
	 * @param leftSource
	 *            the leftSource to set
	 */
	public void setLeftSource(RAOperator leftSource) {
		this.leftSource = leftSource;
	}

	/**
	 * Gets the right source.
	 *
	 * @return the rightSource
	 */
	public RAOperator getRightSource() {
		return rightSource;
	}

	/**
	 * Sets the right source.
	 *
	 * @param rightSource
	 *            the rightSource to set
	 */
	public void setRightSource(RAOperator rightSource) {
		this.rightSource = rightSource;
	}

}
