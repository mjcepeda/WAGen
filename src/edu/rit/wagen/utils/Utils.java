package edu.rit.wagen.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

	public static final String AND = "AND";

	/** The Constant EQUALS. */
	public static final String EQUALS = "=";

	/** The Constant GRE. */
	public static final String GRE = ">";

	/** The Constant GREQ. */
	public static final String GREQ = "=>";

	/** The Constant LESS. */
	public static final String LESS = "<";

	/** The Constant LEQ. */
	public static final String LEQ = "<=";

	/** The Constant DISTINCT. */
	public static final String DISTINCT = "!=";

	/** The Constant LIKE. */
	public static final String LIKE = "like";
	
	/** The Constant NOT LIKE. */
	public static final String NOT_LIKE = "not like";
	
	public static final String[] operators = { DISTINCT, EQUALS, GREQ, LEQ, GRE, LESS, LIKE, NOT_LIKE };
	

	public enum ConstraintType {
		// so far, I just contemplate check constraints
		// pk, fk, unique and not null are enforced by default
		/* PK, FK, UNIQUE, NOT_NULL, */ CHECK
	}

	public static Map<String, List<String>> parsePredicate(String predicate) throws Exception {
		Map<String, List<String>> mapPredicate = new HashMap<>();
		//all the predicates are in CNF (conjunctive normal form)
		String[] conditions = predicate.split(AND);
		for (String c : conditions) {
			boolean foundOP = false;
			int index = 0;
			String left=null;
			while (!foundOP && index < operators.length) {
				if (c.toLowerCase().contains(operators[index])) {
					//Assume that the predicate always has the attribute name in the left
					//getting the attribute involve in the predicate
					left = c.substring(0, c.toLowerCase().indexOf(operators[index])).trim();
//					right = c.substring(c.toLowerCase().indexOf(operators[index]), c.length()).trim().replaceAll(operators[index], "");
					List<String> predicates = null;
					//if an attribute appears in more than one condition
					//update its list of predicates
					if (mapPredicate.containsKey(left)) {
						predicates = mapPredicate.get(left);
					} else {
						predicates = new ArrayList<>();
					}
					predicates.add(c.replaceAll("'", "\""));
					//map with attribute as key, list of conditions as value
					mapPredicate.put(left, predicates);
					//activate flag
					foundOP = true;
				}
				index++;
			}
			if (!foundOP) {
				throw new Exception("Unexpected operator symbol in: " + c);
			}
		}
		return mapPredicate;
	}
	
	public static String updatePredicate(String predicate, String newLeft) {
		boolean foundOP = Boolean.FALSE;
		int index = 0;
		String right=null;
		while (!foundOP && index < operators.length) {
			if (predicate.toLowerCase().contains(operators[index])) {
				//TODO MJCG So far, I am asumming that the predicate always has the attribute name in the left
				right = predicate.substring(predicate.toLowerCase().indexOf(operators[index]), predicate.length()).trim();
				foundOP = true;
			}
			index++;
		}
		return newLeft + right;
	}
	
	public static String negatePredicate(String predicate) throws Exception {
		String negation = null;
		boolean found = Boolean.FALSE;
		for (String op: operators) {
			if (predicate.contains(op)) {
				negation = negate(predicate, op);
				found = Boolean.TRUE;
			}
		}
		if (!found) {
			throw new Exception("The operation in this predicate is not suuported");
		}
		return negation;
	}
	
	private static String negate(String predicate, String op) {
		String neg = null;
		switch (op) {
		case EQUALS:
			neg = DISTINCT;
			break;
		case DISTINCT:
			neg = EQUALS;
			break;
		case GRE:
			neg = LEQ;
			break;
		case GREQ:
			neg = LESS;
			break;
		case LESS:
			neg = GREQ;
			break;
		case LEQ:
			neg = GRE;
			break;
		case LIKE:
			neg = NOT_LIKE;
			break;	
		case NOT_LIKE:
			neg = LIKE;
			break;
		}
		return predicate.replace(op, neg);
	}

}
