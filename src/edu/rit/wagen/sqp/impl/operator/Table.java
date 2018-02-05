package edu.rit.wagen.sqp.impl.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.rit.wagen.database.impl.DatabaseImpl;
import edu.rit.wagen.dto.Constraint;
import edu.rit.wagen.dto.TableDescription;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.Operator;
import edu.rit.wagen.utils.Constants.ConstraintType;

public class Table extends Operator {
	/**
	 * Name of the table and its columns
	 */
	private TableDescription tableDesc;
	/**
	 * Size of the table. It is compulsory knob
	 */
	private int tableSize;
	/**
	 * 0 - It does not exist into RDBMS 1 - Created and populated
	 */
	private boolean tableCreated;

	private List<Tuple> tableScan;

	private int indexTableScan;

	private DatabaseImpl db = new DatabaseImpl();

	public Table(String name, List<String> columns, int tableSize, TableDescription tableDesc) {
		if (name == null || name.trim().equals("") || tableSize <= 0 || columns == null || columns.size() < 1) {
			throw new Error("Missing required information");
		} else {
			this.tableDesc = tableDesc;
			this.cardinality = tableSize;
			this.tableCreated = Boolean.FALSE;
			this.tableScan = null;
			this.indexTableScan = 0;
		}
	}

	@Override
	public void open() {
		// check if table is created
		if (!tableCreated) {
			// creating table
			try {
				tableCreated = db.createTable(tableDesc);
				if (tableCreated) {
					// fill the table until it reaches cardinality value (table
					// size)
					fillTable();
					// TODO MJCG - insert constraints in PTable - Use
					// PredicateListener from Streams project
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public Tuple getNext() {
		// if the data is not loaded
		// go to the database
		if (tableScan == null) {
			// perform table scan of the table
			// and store all tuples in memory
			try {
				tableScan = db.getData(tableDesc);
				// reset indexTableScan
				indexTableScan = 0;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		// get the next tuple
		Tuple tuple = null;
		// returns null if all tuples have been returned
		if (indexTableScan < tableScan.size()) {
			tuple = tableScan.get(indexTableScan);
			// increment the index
			indexTableScan++;
		}
		return tuple;
	}

	@Override
	public void close() {
		// reset tableScan and the index
		tableScan = null;
		indexTableScan = 0;
	}

	/**
	 * Fill table with symbolic data
	 */
	private void fillTable() {
		// creating list of numbers
		List<Integer> numbers = Stream.iterate(1, n -> n + 1).limit(cardinality).collect(Collectors.toList());
		List<String> inserts = new ArrayList<>();
		// creating insert sql statement
		StringBuffer insert = new StringBuffer("insert into ").append(tableDesc.schemaName).append(".")
				.append(tableDesc.name).append("(");
		// adding column names to insert statement
		insert.append(tableDesc.columns.stream().collect(Collectors.joining(", ")));
		insert.append(") values (");
		// adding column values to insert statement
		for (Integer i : numbers) {
			StringBuffer row = new StringBuffer(insert.toString());
			row.append(tableDesc.columns.stream().map(s -> "'" + s + i + "'").collect(Collectors.joining(", ")));
			row.append(")");
			// adding insert statement to the list
			inserts.add(row.toString());
		}
		try {
			// inserting all the rows
			db.insertRows(inserts);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void insertConstraints() {
		// constraints are located in tableDescription object
		for (Constraint c : tableDesc.constraints) {
			// so far, CHECK is the only constraint supported
			if (c.getType().equals(ConstraintType.CHECK)) {
				// TODO MJCG Implement this
			}
		}
	}

	/**
	 * @return the tableSize
	 */
	public int getTableSize() {
		return tableSize;
	}
}
