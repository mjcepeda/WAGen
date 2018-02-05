package edu.rit.wagen.sqp.iapi.operator;

/**
 * The Class BinaryOperation.
 */
public abstract class BinaryOperation extends Operator {

	/** The left source. */
	private Operator leftSource;

	/** The right source. */
	private Operator rightSource;

	
	public BinaryOperation(Operator leftSource, Operator rightSource) {
		this.leftSource = leftSource;
		this.rightSource = rightSource;
	}

	/**
	 * Gets the left source.
	 *
	 * @return the leftSource
	 */
	public Operator getLeftSource() {
		return leftSource;
	}

	/**
	 * Sets the left source.
	 *
	 * @param leftSource
	 *            the leftSource to set
	 */
	public void setLeftSource(Operator leftSource) {
		this.leftSource = leftSource;
	}

	/**
	 * Gets the right source.
	 *
	 * @return the rightSource
	 */
	public Operator getRightSource() {
		return rightSource;
	}

	/**
	 * Sets the right source.
	 *
	 * @param rightSource
	 *            the rightSource to set
	 */
	public void setRightSource(Operator rightSource) {
		this.rightSource = rightSource;
	}

}
