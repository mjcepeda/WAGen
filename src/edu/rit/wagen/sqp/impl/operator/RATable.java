package edu.rit.wagen.sqp.impl.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import ra.RAXNode.TABLE;

/**
 * Table operator
 * 
 * @author Maria Cepeda
 *
 */
public class RATable extends RAOperator {

	public TableSchema tableSchema;
	/**
	 * 0 - It does not exist into RDBMS 1 - Created and populated
	 */
	private boolean tableCreated;
	/**
	 * All the tuples from the table
	 */
	private List<Tuple> result;
	/**
	 * Index to the last retrieved tuple
	 */
	private int indexResult;

	private TABLE _node;

	public RATable(TABLE node, int cardinality, String sbSchema, String realSchema) {
		super(sbSchema, realSchema);
		this._node = node;
		this._cardinality = cardinality;
		this.tableSchema = null;
		this.tableCreated = Boolean.FALSE;
		this._results = null;
		this.indexResult = 0;
		// this operation never has data pre-grouped since it is created unique
		this._preGroupedList = null;
		this._isPreGrouped = Boolean.FALSE;
	}

	/**
	 * Creates the table and insert data until the cardinality is reached
	 */
	@Override
	public void open() throws Exception {
		// check if table is created
		if (!tableCreated) {
			// creating table
			tableSchema = db.getOutputSchema(_realSchema, _node.getTableName());
			// setting the name of the symbolic database
			tableSchema.setSchemaName(_sbSchema);
			tableCreated = db.createSymbolicTable(tableSchema);
			if (tableCreated) {
				// fill the table until it reaches cardinality value (table
				// size)
				fillTable();
				// TODO MJCG - insert constraints in PTable
				// mysql does not support check predicates, so this operation is
				// not possible
			}
		}
	}

	/**
	 * Get the next tuple in the table. If there is no more results, return
	 * null.
	 */
	@Override
	public Tuple getNext() {
		// if the data is not loaded
		// go to the database
		if (result == null) {
			// perform table scan of the table
			// and store all tuples in memory
			try {
				result = db.getData(tableSchema);
				// reset indexResult
				indexResult = 0;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		// get the next tuple
		Tuple tuple = null;
		// returns null if all tuples have been returned
		if (indexResult < result.size()) {
			tuple = result.get(indexResult);
			// increment the index
			indexResult++;
		}
		return tuple;
	}

	@Override
	public void close() {
		// reset result and the index
		result = null;
		indexResult = 0;
	}

	/**
	 * Fill table with symbolic data
	 */
	private void fillTable() throws Exception{
		// creating list of numbers
		List<Integer> numbers = Stream.iterate(1, n -> n + 1).limit(_cardinality).collect(Collectors.toList());
		List<String> inserts = new ArrayList<>();
		// creating insert sql statement
		StringBuffer insert = new StringBuffer("insert into ").append(_sbSchema).append(".")
				.append(tableSchema.getTableName()).append("(");
		// adding column names to insert statement
		insert.append(tableSchema.getColNames().stream().collect(Collectors.joining(", ")));
		insert.append(") values (");
		// adding column values to insert statement
		for (Integer i : numbers) {
			StringBuffer row = new StringBuffer(insert.toString());
			row.append(
					tableSchema.getColNames().stream().map(s -> "'" + s + i + "'").collect(Collectors.joining(", ")));
			row.append(")");
			// adding insert statement to the list
			inserts.add(row.toString());
		}

		// inserting all the rows
		db.insertSymbolicData(inserts);
	}

	private void insertConstraints() {
		// constraints are located in tableDescription object
		// for (Constraint c : tableDesc.constraints) {
		// // so far, CHECK is the only constraint supported
		// if (c.getType().equals(ConstraintType.CHECK)) {
		// // TODO MJCG Implement this
		// }
		// }
	}
}
