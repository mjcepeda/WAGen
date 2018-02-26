package edu.rit.wagen.dto;


/**
 * The Class Operator.
 */
public class Operator {

	/** The Constant EQUALS. */
	// compare operations
	public static final int EQUALS = 0;

	/** The Constant EQUALS_CHAR. */
	public static final String EQUALS_CHAR = "=";

	/** The Constant GRE. */
	public static final int GRE = 1;

	/** The Constant GRE_CHAR. */
	public static final String GRE_CHAR = ">";

	/** The Constant GREQ. */
	public static final int GREQ = 2;

	/** The Constant GREQ_CHAR. */
	public static final String GREQ_CHAR = "=>";

	/** The Constant LESS. */
	public static final int LESS = 3;

	/** The Constant LESS_CHAR. */
	public static final String LESS_CHAR = "<";

	/** The Constant LEQ. */
	public static final int LEQ = 4;

	/** The Constant LEQ_CHAR. */
	public static final String LEQ_CHAR = "<=";

	/** The Constant DISTINCT. */
	public static final int DISTINCT = 5;

	/** The Constant DISTINCT_CHAR. */
	public static final String DISTINCT_CHAR = "<>";

	/** The Constant MINUS. */
	// arithmetic operators
	public static final int MINUS = 6;

	/** The Constant MINUS_CHAR. */
	public static final String MINUS_CHAR = "-";

	/** The Constant PLUS. */
	public static final int PLUS = 7;

	/** The Constant PLUS_CHAR. */
	public static final String PLUS_CHAR = "+";

	/** The Constant DIVIDE. */
	public static final int DIVIDE = 8;

	/** The Constant DIVIDE_CHAR. */
	public static final String DIVIDE_CHAR = "/";

	/** The Constant SUM. */
	public static final int MULTIPLY = 9;

	/** The Constant MULTIPLY_CHAR. */
	public static final String MULTIPLY_CHAR = "*";

	
	/**
	 * Gets the operator.
	 *
	 * @param operator the operator
	 * @return the operator
	 */
	public static String getOperator(int operator) {
		String op = null;
		switch (operator) {
		case EQUALS:
			op = EQUALS_CHAR;
			break;
		case GRE:
			op = GRE_CHAR;
			break;
		case GREQ:
			op = GREQ_CHAR;
			break;
		case LESS:
			op = LESS_CHAR;
			break;
		case LEQ:
			op = LEQ_CHAR;
			break;
		case DISTINCT:
			op = DISTINCT_CHAR;
			break;
		case MINUS:
			op = MINUS_CHAR;
			break;
		case PLUS:
			op = PLUS_CHAR;
			break;
		case DIVIDE:
			op = DIVIDE_CHAR;
			break;
		case MULTIPLY:
			op = MULTIPLY_CHAR;
			break;
		}
		return op;

	}

	
	/**
	 * Gets the operator.
	 *
	 * @param operator the operator
	 * @return the operator
	 */
	public static int getOperator(String operator) {
		int op = EQUALS;
		switch (operator) {
		case EQUALS_CHAR:
			op = EQUALS;
			break;
		case GRE_CHAR:
			op = GRE;
			break;
		case GREQ_CHAR:
			op = GREQ;
			break;
		case LESS_CHAR:
			op = LESS;
			break;
		case LEQ_CHAR:
			op = LEQ;
			break;
		case DISTINCT_CHAR:
			op = DISTINCT;
			break;
		case MINUS_CHAR:
			op = MINUS;
			break;
		case PLUS_CHAR:
			op = PLUS;
			break;
		case DIVIDE_CHAR:
			op = DIVIDE;
			break;
		case MULTIPLY_CHAR:
			op = MULTIPLY;
			break;
		}
		return op;
	}
}
