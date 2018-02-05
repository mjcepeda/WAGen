package edu.rit.wagen.sqp.impl.operator;

import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.Operator;
import edu.rit.wagen.sqp.iapi.operator.UnaryOperation;

public class Selection extends UnaryOperation {

	/** The predicate. */
	public String predicate;
	
	public Selection(int tableSize, Operator source, String predicate) {
		super(source);
		this.predicate = predicate;
		//cardinality is optional (default value = input size)
		if (tableSize ==0) {
			cardinality = source.cardinality;
		}
	}
	
	@Override
	public void open() {
		// TODO Auto-generated method stub
		//Use PredicateListener from Streams to parse the predicate
	}

	@Override
	public Tuple getNext() {
		Tuple t = null;
		// IF INPUT IS NOT PRE-GROUPED IN THE SELECTION ATTR
		// THIS VERSION DOES NOT SUPPORT SELECTION WITH PRE-GROUPED ATTRIBUTES
		if (!isPreGrouped()) {
			// invoke getNext() on its child
			if (cardinality > counter) {
				// process with positive tuple annotation
				// insert in PTable
			} else {
				// process with negative tuple annotation
				// the remaining tuples from its child
				// insert in PTable
				// return null to its parent
				t = null;
			}
		}
		return t;
	}

	@Override
	public void close() {

	}

	private boolean isPreGrouped() {
		// TODO MJCG Implement this method
		// iterate over pre-grouped attributes from its child operator
		// and check if the selection attributes are pre-grouped
		return false;
	}
}
