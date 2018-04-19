package edu.rit.wagen.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import edu.rit.wagen.dto.Predicate;

/**
 * The Class Utils.
 * @author Maria Cepeda
 */
public class Utils {

	/** The Constant AND. */
	public static final String AND = "AND";

	/** The Constant EQUALS. */
	public static final String EQUALS = "=";

	/** The Constant GRE. */
	public static final String GRE = ">";

	/** The Constant GREQ. */
	public static final String GREQ = ">=";

	/** The Constant LESS. */
	public static final String LESS = "<";

	/** The Constant LEQ. */
	public static final String LEQ = "<=";

	/** The Constant DISTINCT. */
	public static final String DISTINCT = "<>";

	/** The Constant LIKE. */
	public static final String LIKE = "like";

	/** The Constant NOT_LIKE. */
	public static final String NOT_LIKE = "not like";

	/** The Constant SUM. */
	public static final String SUM = "+";
	
	/** The Constant MINUS. */
	public static final String MINUS = "-";
	
	/** The Constant TIMES. */
	public static final String TIMES = "*";
	
	/** The Constant DIVIDE. */
	public static final String DIVIDE = "/";

	/** The Constant COMPARATIVE_OPERATORS. */
	public static final String[] COMPARATIVE_OPERATORS = { DISTINCT, NOT_LIKE, LIKE, GREQ, LEQ, EQUALS, GRE, LESS};

	/** The Constant ARITHMETIC_OPERATORS. */
	public static final String[] ARITHMETIC_OPERATORS = { SUM, MINUS, TIMES, DIVIDE };
	
	/** The Constant SYMBOL_REGEX. */
	public static final String SYMBOL_REGEX = ".[a-zA-Z]+\\d";

	/**
	 * The Enum ConstraintType.
	 */
	public enum ConstraintType {
		
		/** The pk. */
		// pk, fk, unique and not null are enforced by default
		PK, 
 /** The fk. */
 FK, 
 /** The unique. */
 UNIQUE, 
 /** The check. */
 /* NOT_NULL, */ CHECK
	}

	/**
	 * Gets the constraint.
	 *
	 * @param type the type
	 * @return the constraint
	 */
	public static ConstraintType getConstraint(String type) {
		ConstraintType cType = null;
		switch (type.trim().toUpperCase()) {
		case "PRIMARY KEY":
			cType = ConstraintType.PK;
			break;
		case "FOREIGN KEY":
			cType = ConstraintType.FK;
			break;
		case "UNIQUE":
			cType = ConstraintType.UNIQUE;
			break;
		default:
			break;
		}
		return cType;
	}

	/**
	 * Gets the correlated symbols.
	 *
	 * @param p the p
	 * @return the correlated symbols
	 * @throws Exception the exception
	 */
	public static List<String> getCorrelatedSymbols(Predicate p) throws Exception {
		List<String> l = new ArrayList<>();
		boolean foundOP = false;
		int index = 0;
		// first, parse the predicate
		// List<Predicate> colPredicates = parsePredicate(predicate);
		// for (Predicate p : colPredicates) {
		// for every predicate, we will check if the condition (right part)
		// contains other symbols from the database
		// looking for conditions like age > min_age + 5
		if (Arrays.asList(ARITHMETIC_OPERATORS).contains(p.condition)) {
			while (!foundOP && index < ARITHMETIC_OPERATORS.length) {
				if (p.condition.contains(ARITHMETIC_OPERATORS[index])) {
					String left = p.condition.substring(0, p.condition.indexOf(ARITHMETIC_OPERATORS[index])).trim();
					// adding possible symbol
					if (left.matches(SYMBOL_REGEX)) {
						l.add(left);
					}
					String right = p.condition
							.substring(p.condition.indexOf(ARITHMETIC_OPERATORS[index]), p.condition.length()).trim()
							.replaceAll(ARITHMETIC_OPERATORS[index], "");
					// adding possible symbol
					l.add(right);
					if (right.matches(SYMBOL_REGEX)) {
						l.add(right);
					}
					// activate flag
					foundOP = true;
				}
				index++;
			}
		} else if (p.condition.matches(SYMBOL_REGEX)) {
			// adding possible symbol
			l.add(p.condition);
		}
		// }
		return l;
	}

	/**
	 * Parses the predicate.
	 *
	 * @param predicate the predicate
	 * @return the list
	 * @throws Exception the exception
	 */
	public static List<Predicate> parsePredicate(String predicate) throws Exception {
		List<Predicate> colPredicates = null;
		// all the predicates are in CNF (conjunctive normal form)
		String[] conditions = predicate.split(AND);
		// create a predicate for every clause
		for (String c : conditions) {
			Predicate p = getPredicate(c);
			if (p != null) {
				if (colPredicates == null) {
					colPredicates = new ArrayList<>();
				}
				colPredicates.add(p);
			}
		}
		return colPredicates;
	}

	/**
	 * Gets the predicate.
	 *
	 * @param predicate the predicate
	 * @return the predicate
	 * @throws Exception the exception
	 */
	public static Predicate getPredicate(String predicate) throws Exception {
		boolean foundOP = false;
		int index = 0;
		Predicate p = null;
		while (!foundOP && index < COMPARATIVE_OPERATORS.length) {
			if (predicate.contains(COMPARATIVE_OPERATORS[index])) {
				// Assume that the predicate always has the attribute name
				// in the left
				// getting the attribute involve in the predicate
				String left = predicate.substring(0, predicate.toLowerCase().indexOf(COMPARATIVE_OPERATORS[index]))
						.trim();
				String right = predicate
						.substring(predicate.toLowerCase().indexOf(COMPARATIVE_OPERATORS[index]), predicate.length())
						.trim().replaceAll(COMPARATIVE_OPERATORS[index], "").replaceAll("'", "\"");
				p = new Predicate(left, COMPARATIVE_OPERATORS[index].equals("<>") ? "!=" : COMPARATIVE_OPERATORS[index], right);
				// activate flag
				foundOP = true;
			}
			index++;
		}
		if (!foundOP) {
			throw new Exception("Unexpected operator symbol in: " + predicate);
		}
		return p;
	}
	
	/**
	 * Parses the condition.
	 *
	 * @param condition the condition
	 * @return the predicate
	 * @throws Exception the exception
	 */
	public static Predicate parseCondition(String condition) throws Exception {
		boolean foundOP = false;
		int index = 0;
		Predicate p = null;
		while (!foundOP && index < ARITHMETIC_OPERATORS.length) {
			if (condition.contains(ARITHMETIC_OPERATORS[index])) {
				// Assume that the predicate always has the attribute name
				// in the left
				// getting the attribute involve in the predicate
				String left = condition.substring(0, condition.toLowerCase().indexOf(ARITHMETIC_OPERATORS[index]))
						.trim();
				String right = condition
						.substring(condition.toLowerCase().indexOf(ARITHMETIC_OPERATORS[index]), condition.length())
						.trim().replaceAll(ARITHMETIC_OPERATORS[index], "");
				// predicates.add(c.replaceAll("'", "\""));
				p = new Predicate(left, ARITHMETIC_OPERATORS[index], right);
				// activate flag
				foundOP = true;
			}
			index++;
		}
		if (!foundOP) {
			throw new Exception("Unexpected operator symbol in: " + condition);
		}
		return p;
	}

	/**
	 * Negate predicate.
	 *
	 * @param predicate the predicate
	 * @return the string
	 * @throws Exception the exception
	 */
	public static String negatePredicate(String predicate) throws Exception {
		String negation = null;
		boolean found = Boolean.FALSE;
		int idx = 0;
		while (idx < COMPARATIVE_OPERATORS.length && !found) {
			String op = COMPARATIVE_OPERATORS[idx];
			if (predicate.contains(op)) {
				negation = negate(predicate, op);
				found = Boolean.TRUE;
			}
			idx++;
		}
		
		if (!found) {
			throw new Exception("The operation in this predicate is not suuported");
		}
		return negation;
	}

	/**
	 * Generate random string.
	 *
	 * @param random the random
	 * @param length the length
	 * @return the string
	 */
	public static String generateRandomString(Random random, int length) {
		return random.ints(48, 123).filter(i -> (i < 58) || (i > 64 && i < 91) || (i > 96)).limit(length)
				.collect(StringBuilder::new, (sb, i) -> sb.append((char) i), StringBuilder::append).toString();

	}

	/**
	 * Negate.
	 *
	 * @param predicate the predicate
	 * @param op the op
	 * @return the string
	 */
	private static String negate(String predicate, String op) {
		String neg = null;
		switch (op) {
		case GREQ:
			neg = LESS;
			break;
		case LEQ:
			neg = GRE;
			break;
		case EQUALS:
			neg = DISTINCT;
			break;
		case DISTINCT:
			neg = EQUALS;
			break;
		case GRE:
			neg = LEQ;
			break;
		case LESS:
			neg = GREQ;
			break;
		case NOT_LIKE:
			neg = LIKE;
			break;
		case LIKE:
			neg = NOT_LIKE;
			break;
		}
		return predicate.replace(op, neg);
	}
}
