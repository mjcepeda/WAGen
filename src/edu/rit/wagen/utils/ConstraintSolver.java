package edu.rit.wagen.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.chocosolver.solver.Model;
import org.chocosolver.solver.Solution;
import org.chocosolver.solver.variables.IntVar;

import edu.rit.wagen.dto.Predicate;

public class ConstraintSolver {
	public static Map<String, IntVar> solvePredicates(List<Predicate> colPredicates) throws Exception {
		// map of symbol and variables
		Map<String, IntVar> mapVars = new HashMap<>();
		Model model = createModel(colPredicates, mapVars);
		Solution solution = model.getSolver().findSolution();
		if (solution == null) {
			throw new Exception("The constraint solver has not found results for that set of constraints");
		}
		return mapVars;
	}

	public static boolean isSAT(List<Predicate> colPredicates) throws Exception {
		// map of symbol and variables
		Map<String, IntVar> mapVars = new HashMap<>();
		Model model = createModel(colPredicates, mapVars);
		Solution solution = model.getSolver().findSolution();
		return solution != null;
	}
	
	private static Model createModel(List<Predicate> colPredicates, Map<String, IntVar> mapVars) throws Exception {
		int MAX_VALUE = 100000000;
		// creating model
		Model model = new Model();
		for (Predicate p : colPredicates) {
			// creating a variable for symbol
			if (!mapVars.containsKey(p.symbol)) {
				mapVars.put(p.symbol, model.intVar(p.symbol, 0, MAX_VALUE));
			}
			// if the predicate looks like salary > min_salary * 2
			// we need the parse the right most part
			if (Arrays.asList(Utils.ARITHMETIC_OPERATORS).contains(p.condition)) {
				Predicate pc = Utils.parseCondition(p.condition);
				// if the attribute is a symbol, we create a var with name and
				// range
				// e.g min_salary * 2
				IntVar arg2 = null;
				if (pc.symbol.matches(Utils.SYMBOL_REGEX)) {
					arg2 = model.intVar(pc.symbol, 0, MAX_VALUE);
					mapVars.put(pc.symbol, arg2);
				} else {
					// the attribute is a number, we do not need to store that
					// into the map
					arg2 = model.intVar(Integer.valueOf(pc.symbol));
				}
				// same thing with the condition
				IntVar arg4 = null;
				if (pc.condition.matches(Utils.SYMBOL_REGEX)) {
					arg4 = model.intVar(pc.condition, 0, MAX_VALUE);
					mapVars.put(pc.condition, arg4);
				} else {
					// the attribute is a number, we do not need to store that
					// into the map
					arg4 = model.intVar(Integer.valueOf(pc.condition));
				}
				model.arithm(mapVars.get(p.symbol), p.op, arg2, pc.op, arg4).post();
			} else {
				// if the predicate looks like salary > 10,000
				IntVar arg2 = null;
				if (p.condition.matches(Utils.SYMBOL_REGEX)) {
					arg2 = model.intVar(p.condition, 0, MAX_VALUE);
					mapVars.put(p.condition, arg2);
				} else {
					// the attribute is a number, we do not need to store that
					// into the map
					arg2 = model.intVar(Integer.valueOf(p.condition));
				}
				model.arithm(mapVars.get(p.symbol), p.op, arg2).post();
			}
		}
		return model;
	}
}
