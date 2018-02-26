package edu.rit.wagen.instantiator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.chocosolver.solver.Model;
import org.chocosolver.solver.Solution;
import org.chocosolver.solver.variables.IntVar;

import edu.rit.wagen.database.impl.DatabaseImpl;
import edu.rit.wagen.dto.Constraint;
import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.utils.Utils;
import edu.rit.wagen.utils.Utils.ConstraintType;

public class DataInstantiator {

	private String realSchema;

	private String symbolicSchema;

	private DatabaseImpl db;

	/** The Constant SymbolValueCache table. */
	private final static String SYMBOL_CACHE = "CREATE TABLE <SCHEMA_NAME>.SYMBOL_VALUE_CACHE (SYMBOL VARCHAR(100), VALUE varchar(100))";

	/** The Constant PredicateValueCache table. */
	private final static String PREDICATE_CACHE = "CREATE TABLE <SCHEMA_NAME>.PREDICATE_VALUE_CACHE (PREDICATE VARCHAR(100), symbol varchar(100), rightmost VARCHAR(100))";

	public DataInstantiator(String realSchema, String symbolicSchema) throws SQLException {
		this.realSchema = realSchema;
		this.symbolicSchema = symbolicSchema;
		db = new DatabaseImpl();
		// create the SymbolValueCache table into the symbolic database
		db.execCommand(SYMBOL_CACHE.replaceAll("<SCHEMA_NAME>", symbolicSchema));
		// create the PredicateValueCache table into the symbolic database
		db.execCommand(PREDICATE_CACHE.replaceAll("<SCHEMA_NAME>", symbolicSchema));
	}

	public void generateData() throws Exception {
		// get all the tables from the database
		List<TableSchema> tableList = db.getTableList(realSchema);
		// for every table
		for (TableSchema tableSchema : tableList) {
			List<String> insertStmts = new ArrayList<>();
			// set the symbolic database name
			tableSchema.setSchemaName(symbolicSchema);
			// get the data
			List<Tuple> data = db.getData(tableSchema);
			// creating common part from insert sql statement
			StringBuffer insert = new StringBuffer("insert into ").append(realSchema).append(".")
					.append(tableSchema.getTableName()).append("(");
			// for every tuple
			for (Tuple t : data) {
				List<String> columnList = new ArrayList<>();
				List<String> valueList = new ArrayList<>();
				// for each symbol s in a tuple t
				for (Entry<String, String> entry : t.getValues().entrySet()) {
					String symbol = entry.getValue();
					// look up symbol in the symbol value cache table, if exists
					// get
					// that concrete value
					String value = db.getSymbolValueCache(symbolicSchema, symbol);
					if (value == null) {
						// look up the predicates P of s from PTable
						List<Predicate> predicates = db.getPredicates(symbolicSchema, symbol);
						// if there is no predicates associated with s, then
						// instantiate s by a defaut value
						if (predicates == null) {
							value = generateValue(tableSchema, entry.getKey(), symbol);
							// insert a tuple in the symbol value cache table
							db.insertSymbolicValueCache(symbolicSchema, symbol, value);
						} else {
							// compute predicate closure (conjuntive
							// propositional
							// formula)
							List<Predicate> colClosure = new ArrayList<>();
							predicateClosure(symbol, predicates, colClosure);
							// look up in the predicate value cache table for a
							// predicate with the same pattern
							// if exists reuse the value
							String pattern = predicates.stream().map(p -> {
								return p.symbol.replaceAll("[0-9]", "") + p.op + p.condition;
							}).collect(Collectors.joining(" AND "));
							// int index = 0;
							// while (index < colClosure.size() && value ==
							// null) {
							// Predicate p = colClosure.get(index);
							// String pattern = p.symbol.replaceAll("[0-9]", "")
							// + p.op + p.condition;
							String s = db.getPredicateValueCache(symbolicSchema, pattern);
							// look up the value of the symbol s in the
							// symbol value cache
							if (s != null) {
								value = db.getSymbolValueCache(symbolicSchema, s);
							}
							// index++;
							// }
							// if not, call the constraint solver
							if (value == null) {
								// replace symbols with their symbol value cache
								for (Predicate p : colClosure) {
									String cachedValue = db.getSymbolValueCache(symbolicSchema, p.symbol);
									if (cachedValue != null) {
										p.condition = p.condition.replaceAll(p.symbol, cachedValue);
									}
								}
								// call the constraint solver
								Map<String, IntVar> mapData = solvePredicates(colClosure);
								// set the value returned by the constraint
								// solver
								value = String.valueOf(mapData.get(symbol).getValue());
								// insert into the symbol value cache all the
								// generated values (for this symbol and
								// correlated symbols)
								for (Entry<String, IntVar> result : mapData.entrySet()) {
									db.insertSymbolicValueCache(symbolicSchema, result.getKey(),
											String.valueOf(result.getValue().getValue()));
								}
								// insert the predicate into the predicate value
								// cache
								// String pattern = predicates.stream().map(p ->
								// {
								// return p.symbol.replaceAll("[0-9]", "") +
								// p.op + p.condition;
								// }).collect(Collectors.joining(" AND "));
								StringBuffer insertPredicateCache = new StringBuffer("insert into ")
										.append(symbolicSchema)
										.append(".PREDICATE_VALUE_CACHE (predicate, symbol) values ('").append(pattern)
										.append("', '").append(symbol).append("')");
								db.execCommand(insertPredicateCache.toString());
							}
						}
					}
					// update tuple with concrete value
					t.getValues().put(entry.getKey(), value);
					// add column to the list
					columnList.add(entry.getKey());
					// add value to the list
					valueList.add(value);
				}
				// adding column names to insert statement
				StringBuffer rowInsert = new StringBuffer(insert.toString());
				rowInsert.append(columnList.stream().collect(Collectors.joining(", ")));
				rowInsert.append(") values (");
				// adding column values to insert statement
				rowInsert.append(valueList.stream().collect(Collectors.joining(", ")));
				rowInsert.append(")");
				// adding insert statement to the list
				insertStmts.add(rowInsert.toString());
			}
			// inserting all the rows into the concrete database
			db.execCommands(insertStmts);
		}
	}

	private Map<String, IntVar> solvePredicates(List<Predicate> colPredicates) throws Exception {
		int MAX_VALUE = 100000000;
		// map of symbol and variables
		Map<String, IntVar> mapVars = new HashMap<>();
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
		Solution solution = model.getSolver().findSolution();
		if (solution == null) {
			throw new Exception("The constraint solver has not found results for that set of constraints");
		}
		return mapVars;
	}

	private List<Predicate> predicateClosure(String symbol, List<Predicate> predicates, List<Predicate> closure)
			throws Exception {
		// add list of predicates into the closure list
		closure.addAll(predicates);
		for (Predicate p : predicates) {
			// list of symbols indirectly correlated with predicates of symbol
			List<String> listIndirect = Utils.getCorrelatedSymbols(p);
			for (String iSymbol : listIndirect) {
				// look up the predicates P of iSymbol from PTable
				List<Predicate> iPredicates = db.getPredicates(symbolicSchema, iSymbol);
				if (iPredicates != null && iPredicates.size() > 0) {
					predicateClosure(iSymbol, iPredicates, closure);
				}
			}
		}
		return closure;
	}

	private String generateValue(TableSchema table, String attribute, String symbol) {
		String value = null;
		Random r = new Random();
		boolean isUnique = Boolean.FALSE;
		// generate default value that matches the actual domain of s in the
		// input schema
		switch (table.getColTypes().get(table.getColNames().indexOf(attribute))) {
		case "INT":
			isUnique = isUnique(table, attribute);
			if (isUnique) {
				// extract the number from the symbol, that will be the value
				value = symbol.replaceAll(attribute, "").trim();
			} else {
				value = String.valueOf(ThreadLocalRandom.current().nextInt(0, 1000));
			}
			break;
		case "VARCHAR":
			isUnique = isUnique(table, attribute);
			// if the value must be unique, use the value generated from the SQP
			if (isUnique) {
				value = symbol;
			} else {
				value = Utils.generateRandomString(r, 50);
			}
			value = "'" + value + "'";
			break;
		// TODO MJCG Include cases for float, double, and date
		case "NUMERIC":
			break;
		default:
			break;
		}
		return value;
	}

	private boolean isUnique(TableSchema table, String attribute) {
		boolean isUnique = Boolean.FALSE;
		int index = 0;
		while (!isUnique && index < table.getConstraints().size()) {
			Constraint c = table.getConstraints().get(index);
			if (c.column.equals(attribute)
					&& (c.type.equals(ConstraintType.PK) || c.type.equals(ConstraintType.UNIQUE))) {
				isUnique = Boolean.TRUE;
			}
			index++;
		}
		return isUnique;
	}
}
