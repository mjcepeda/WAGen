package edu.rit.wagen.instantiator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.chocosolver.solver.variables.IntVar;

import edu.rit.wagen.database.impl.DatabaseImpl;
import edu.rit.wagen.dto.Constraint;
import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.utils.ConstraintSolver;
import edu.rit.wagen.utils.Utils;
import edu.rit.wagen.utils.Utils.ConstraintType;

/**
 * The Class DataInstantiator.
 * @author Maria Cepeda
 */
public class DataInstantiator {

	/** The real schema. */
	private String realSchema;

	/** The symbolic schema. */
	private String symbolicSchema;

	/** The db. */
	private DatabaseImpl db = new DatabaseImpl();

	/** The Constant SYMBOL_CACHE. */
	private final static String SYMBOL_CACHE = "CREATE TABLE <SCHEMA_NAME>.SYMBOL_VALUE_CACHE (SYMBOL VARCHAR(100), VALUE varchar(100))";
	
	/** The Constant SYMBOL_INDEX. */
	private final static String SYMBOL_INDEX = "CREATE INDEX IDX_SYMBOL ON <SCHEMA_NAME>.SYMBOL_VALUE_CACHE (SYMBOL)";

	/** The Constant PREDICATE_CACHE. */
	private final static String PREDICATE_CACHE = "CREATE TABLE <SCHEMA_NAME>.PREDICATE_VALUE_CACHE (PREDICATE VARCHAR(100), symbol varchar(100), rightmost VARCHAR(100))";
	
	/** The Constant PREDICATE_INDEX. */
	private final static String PREDICATE_INDEX = "CREATE INDEX IDX_PREDICATE ON <SCHEMA_NAME>.PREDICATE_VALUE_CACHE (PREDICATE)";

	/**
	 * Instantiates a new data instantiator.
	 */
	public DataInstantiator() {
	}

	/**
	 * Instantiates a new data instantiator.
	 *
	 * @param realSchema the real schema
	 * @param symbolicSchema the symbolic schema
	 * @throws SQLException the SQL exception
	 */
	public DataInstantiator(String realSchema, String symbolicSchema) throws SQLException {
		this.realSchema = realSchema;
		this.symbolicSchema = symbolicSchema;
		List<String> stmts = new ArrayList<>();
		// create the SymbolValueCache table and PredicateValueCache into the symbolic database
		stmts.add(SYMBOL_CACHE.replaceAll("<SCHEMA_NAME>", symbolicSchema));
		stmts.add(SYMBOL_INDEX.replaceAll("<SCHEMA_NAME>", symbolicSchema));
		stmts.add(PREDICATE_CACHE.replaceAll("<SCHEMA_NAME>", symbolicSchema));
		stmts.add(PREDICATE_INDEX.replaceAll("<SCHEMA_NAME>", symbolicSchema));
		db.execCommands(stmts);
	}

	/**
	 * Generate data.
	 *
	 * @param schema the schema
	 * @throws Exception the exception
	 */
	public void generateData(List<String> schema) throws Exception {
		// we used the input schema definition to avoid problems with references
		// constraints
		for (String stmt : schema) {
			// extract from the create table statement the name of the table
			stmt = stmt.replace("create table ", "");
			String[] tokens = stmt.split(" ");
			String table = tokens[0];
			List<String> insertStmts = new ArrayList<>();
			// get table definition
			TableSchema tableSchema = db.getOutputSchema(realSchema, table);
			// set the symbolic database name
			tableSchema.setSchemaName(symbolicSchema);
			if (db.existTable(tableSchema)) {
//				System.out.println(new Date() + " Processing table " + tableSchema.getTableName());
				// if that table exists in the SDB, get the data
				List<Tuple> data = db.getData(tableSchema);
				//creating insert statement
				String insertStmt = getInsertStmt(tableSchema);
				//create list of data to insert in the final database
				List<List<String>> insertData = new ArrayList<>();
				// creating common part from insert sql statement
//				StringBuffer insert = new StringBuffer("insert into ").append(realSchema).append(".")
//						.append(tableSchema.getTableName()).append("(");
				// get the list of attributes from this table that has
				// associated predicates
				List<String> attsList = db.getAttributesPredicates(tableSchema.getColNames(), symbolicSchema);
				boolean tupleInsertion = Boolean.TRUE;
				List<String> tableStmts = new ArrayList();
				// if there if no predicates for this table, insertions to the
				// cache is done once for the entire table
				if (attsList == null || attsList.isEmpty()) {
					tupleInsertion = Boolean.FALSE;
				}
				Map<String, String> mapPredicatesCache = new HashMap<>();
				Map<String, String> mapSymbolCache = new HashMap<>();
				// for every tuple
				for (Tuple t : data) {
//					List<String> columnList = new ArrayList<>();
//					List<String> valueList = new ArrayList<>();
					List<String> tupleStmts = new ArrayList<>();
					List<String> insertTuple = new ArrayList<>();
					for (String key : tableSchema.getColNames()) {
						String symbol = t.getValues().get(key);
						
					// for each symbol s in a tuple t
//					for (Entry<String, String> entry : t.getValues().entrySet()) {
//						String symbol = entry.getValue();
						// look up symbol in the symbol value cache table, if
						// exists
						// get
						// that concrete value
						String value = mapSymbolCache.get(symbol);
						if (value == null) {
							value = db.getSymbolValueCache(symbolicSchema, symbol);
							mapSymbolCache.put(symbol, value);
						}
						if (value == null) {
							// look up the predicates P of s from PTable
							List<Predicate> predicates = null;
							// if there are predicates into the PTable, look up
							// if there are predicates for this symbol
							if (attsList != null && !attsList.isEmpty()) {
								predicates = db.getPredicates(symbolicSchema, symbol);
							}
							// if there is no predicates associated with s, then
							// instantiate s by a defaut value
							if (predicates == null) {
								value = generateValue(tableSchema, /*entry.getKey()*/key, symbol);
								if (insertInCache(tableSchema, /*entry.getKey()*/key)) {
									// insert a tuple in the symbol value cache
									// table
									// add statement to the table list
									tableStmts.add(symbolValueCacheStmt(symbolicSchema, symbol, value));
								}
								// db.insertSymbolicValueCache(symbolicSchema,
								// symbol, value);
							} else {
								// compute predicate closure (conjuntive
								// propositional
								// formula)
								List<Predicate> colClosure = new ArrayList<>();
								predicateClosure(symbol, predicates, colClosure);
								// look up in the predicate value cache table
								// for a
								// predicate with the same pattern
								// if exists reuse the value
								String pattern = predicates.stream().map(p -> {
									return p.getPattern();
								}).collect(Collectors.joining(" AND "));
								String s = mapPredicatesCache.get(pattern);
								// String s =
								// db.getPredicateValueCache(symbolicSchema,
								// pattern);
								// look up the value of the symbol s in the
								// symbol value cache
								if (s != null) {
									value = mapSymbolCache.get(s);
									// value =
									// db.getSymbolValueCache(symbolicSchema,
									// s);
								}
								if (value == null) {
									// replace symbols with their symbol value
									// cache
									for (Predicate p : colClosure) {
										String cachedValue = mapSymbolCache.get(p.symbol);
										// String cachedValue =
										// db.getSymbolValueCache(symbolicSchema,
										// p.symbol);
										if (cachedValue != null) {
											p.condition = p.condition.replaceAll(p.symbol, cachedValue);
										}
									}

									// call the constraint solver
									Map<String, IntVar> mapData = ConstraintSolver.solvePredicates(colClosure);
									// set the value returned by the constraint
									// solver
									value = String.valueOf(mapData.get(symbol).getValue());
									// insert into the symbol value cache all
									// the
									// generated values (for this symbol and
									// correlated symbols)
									for (Entry<String, IntVar> result : mapData.entrySet()) {
										tupleStmts.add(symbolValueCacheStmt(symbolicSchema, result.getKey(),
												String.valueOf(result.getValue().getValue())));
										// add to map
										mapSymbolCache.put(result.getKey(),
												String.valueOf(result.getValue().getValue()));
										// db.insertSymbolicValueCache(symbolicSchema,
										// result.getKey(),
										// String.valueOf(result.getValue().getValue()));
									}
									StringBuffer insertPredicateCache = new StringBuffer("insert into ")
											.append(symbolicSchema)
											.append(".PREDICATE_VALUE_CACHE (predicate, symbol) values ('")
											.append(pattern).append("', '").append(symbol).append("')");
									tupleStmts.add(insertPredicateCache.toString());
									// insert to map
									mapPredicatesCache.put(pattern, symbol);
									// db.execCommand(insertPredicateCache.toString());
								}
							}
						}
						// update tuple with concrete value
//						t.getValues().put(/*entry.getKey()*/key, value);
						// add column to the list
//						columnList.add(key/*entry.getKey()*/);
						// add value to the list
//						valueList.add(value);
						insertTuple.add(value);
					}
					if (tupleInsertion && !tupleStmts.isEmpty()) {
						// execute the insert for all the symbols of this tuple
						db.execCommands(tupleStmts);
					} else {
						// add the list of stmts to the list of stmts of the
						// table
						tableStmts.addAll(tupleStmts);
					}
					// preparing insert stamente with concrete data to the
					// target database
					// adding column names to insert statement
//					StringBuffer rowInsert = new StringBuffer(insert.toString());
//					rowInsert.append(columnList.stream().collect(Collectors.joining(", ")));
//					rowInsert.append(") values (");
					// adding column values to insert statement
//					rowInsert.append(valueList.stream().collect(Collectors.joining(", ")));
//					rowInsert.append(")");
					// adding insert statement to the list
//					insertStmts.add(rowInsert.toString());
					insertData.add(insertTuple);
				}

				if (!tableStmts.isEmpty()) {
					// insert rows in the cache for the entire table
					db.execCommands(tableStmts);
				}

				// inserting all the concrete data into database
//				db.execCommands(insertStmts);
//				insertData.forEach(System.out::println);
				db.execUpdates(insertStmt, insertData);
//				System.out.println(new Date() + " processing table " + tableSchema.getTableName() + " finished");
			}
		}
	}

	/**
	 * Predicate closure.
	 *
	 * @param symbol the symbol
	 * @param predicates the predicates
	 * @param closure the closure
	 * @return the list
	 * @throws Exception the exception
	 */
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

	/**
	 * Generate value.
	 *
	 * @param table the table
	 * @param attribute the attribute
	 * @param symbol the symbol
	 * @return the string
	 */
	private String generateValue(TableSchema table, String attribute, String symbol) {
		// The first version of this method only support int and varchar
		// attributes
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
			// TODO It should have into account the lenght of the column
			isUnique = isUnique(table, attribute);
			// if the value must be unique, use the value generated from the SQP
			if (isUnique) {
				value = symbol;
			} else {
				value = Utils.generateRandomString(r, 30);
			}
			value = "'" + value + "'";
			break;
		default:
			break;
		}
		return value;
	}

	/**
	 * Checks if is unique.
	 *
	 * @param table the table
	 * @param attribute the attribute
	 * @return true, if is unique
	 */
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

	/**
	 * Symbol value cache stmt.
	 *
	 * @param schemaName the schema name
	 * @param symbol the symbol
	 * @param value the value
	 * @return the string
	 */
	private String symbolValueCacheStmt(String schemaName, String symbol, String value) {
		return new StringBuffer("insert into ").append(schemaName)
				.append(".symbol_value_cache (symbol, value) values ('" + symbol + "'," + value + ")").toString();
	}

	/**
	 * Insert in cache.
	 *
	 * @param table the table
	 * @param column the column
	 * @return true, if successful
	 */
	private boolean insertInCache(TableSchema table, String column) {
		int idx = 0;
		boolean insert = Boolean.FALSE;
		if (table.getConstraints().size() > 0) {
			Constraint c = null;
			while (idx < table.getConstraints().size() && !insert) {
				c = table.getConstraints().get(idx);
				if (c.column.equals(column) && (c.type.equals(ConstraintType.FK) || c.type.equals(ConstraintType.PK))) {
					insert = Boolean.TRUE;
				}
				idx++;
			}
		}
		return insert;
	}

	/**
	 * Gets the insert stmt.
	 *
	 * @param table the table
	 * @return the insert stmt
	 */
	private String getInsertStmt(TableSchema table) {
		StringBuffer insert = new StringBuffer("insert into ").append(realSchema).append(".")
				.append(table.getTableName()).append(" (");
		// adding column names to insert statement
		insert.append(table.getColNames().stream().collect(Collectors.joining(", ")));
		insert.append(") values (");
		insert.append(
				table.getColNames().stream().map(s -> "?").collect(Collectors.joining(", ")));
		insert.append(")");
		return insert.toString();
	}
}
