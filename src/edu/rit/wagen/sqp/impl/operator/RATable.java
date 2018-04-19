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

	private int page;

	private int totalPages;

	private static final int LIMIT = 1000;

	private TABLE _node;

	public RATable(TABLE node, int cardinality, String sbSchema, String realSchema) {
		super(sbSchema, realSchema);
		this._node = node;
		this._cardinality = cardinality;
		this.tableSchema = null;
		this.tableCreated = Boolean.FALSE;
		this._results = null;
		this.indexResult = 1;
		// this operation never has data pre-grouped since it is created unique
		this._preGroupedList = null;
		this._isPreGrouped = Boolean.FALSE;
		this.page = 0;
		this.totalPages = cardinality / LIMIT;
	}

	/**
	 * Creates the table and insert data until the cardinality is reached
	 */
	@Override
	public void open() throws Exception {
//		System.out.println(new Date() + " open table " + _node.getTableName());
		// creating table
		tableSchema = db.getOutputSchema(_realSchema, _node.getTableName());
		// setting the name of the symbolic database
		tableSchema.setSchemaName(_sbSchema);
		tableCreated = db.existTable(tableSchema);
		// check if table is created
		if (!tableCreated) {
			// System.out.println(new Date() + " creating table: " +
			// tableSchema.getTableName());
			tableCreated = db.createSymbolicTable(tableSchema);
			if (tableCreated) {
				// fill the table until it reaches cardinality value (table
				// size)
				// System.out.println(new Date() + " filling table");
				fillTable();
				// next, insert constraints in PTable
				// mysql does not support check predicates, so this operation is
				// not possible for this version
			}
		}
//		System.out.println(new Date() + " open table finished");
	}

	/**
	 * Get the next tuple in the table. If there is no more results, return
	 * null.
	 */
	@Override
	public Tuple getNext() {
		// get the next tuple
		Tuple tuple = null;
		// returns null if all tuples have been returned
		if (indexResult <= _cardinality) {
			// if the data is not loaded or the tuple is not in this page
			// go to the database
			if (result == null || indexResult > ((page + 1) * LIMIT)) {
				if (indexResult > ((page + 1) * LIMIT)) {
					// increment by 1
					page += 1;
				}
				// get the data for that page
				try {
					result = db.getData(tableSchema, page * LIMIT, LIMIT);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			int indexInPage = indexResult;
			if (page > 0) {
				indexInPage = (indexResult % (page * LIMIT));
				//last tuple from the list
				if (indexInPage == 0) {
					indexInPage = 1000;
				}
			}
			tuple = result.get(indexInPage - 1);
			// increment the index
			indexResult++;
		} else {
			close();
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
	private void fillTable() throws Exception {
		// creating list of numbers
		List<Integer> numbers = Stream.iterate(1, n -> n + 1).limit(_cardinality).collect(Collectors.toList());
		// List<String> inserts = new ArrayList<>();
		// creating insert sql statement
		StringBuffer insert = new StringBuffer("insert into ").append(_sbSchema).append(".")
				.append(tableSchema.getTableName()).append("(");
		// adding column names to insert statement
		insert.append(tableSchema.getColNames().stream().collect(Collectors.joining(", ")));
		insert.append(") values (");
		insert.append(tableSchema.getColNames().stream().map(s -> "?").collect(Collectors.joining(", ")));
		insert.append(")");
		List<List<String>> data = new ArrayList<>();
		// adding column values to insert statement
		for (Integer i : numbers) {
			List tuple = tableSchema.getColNames().stream().map(s -> s + i).collect(Collectors.toList());
			data.add(tuple);
			// StringBuffer row = new StringBuffer(insert.toString());
			//
			// row.append(
			// tableSchema.getColNames().stream().map(s -> "'" + s + i +
			// "'").collect(Collectors.joining(", ")));
			// row.append(")");
			// // adding insert statement to the list
			// inserts.add(row.toString());
		}
		db.execUpdates(insert.toString(), data);
		// inserting all the rows
		// db.execCommands(inserts);
	}
}
