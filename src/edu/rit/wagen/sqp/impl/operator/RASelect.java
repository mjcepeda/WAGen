package edu.rit.wagen.sqp.impl.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.rit.wagen.dto.PTable;
import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import edu.rit.wagen.sqp.iapi.operator.UnaryOperation;
import edu.rit.wagen.utils.Utils;
import ra.RAXNode.SELECT;

/**
 * Selection operator
 * 
 * @author Maria Cepeda
 *
 */
public class RASelect extends UnaryOperation {

	private SELECT _node;
	/**
	 * k - name of the attribute v - list of predicates for the attribute
	 */
	public Map<String, List<String>> mapPredicate;

	public RASelect(RAOperator source, SELECT node, RAAnnotation constraints, String sbSchema, String realSchema)
			throws Exception {
		super(source, sbSchema, realSchema);
		this._node = node;
		if (constraints == null || constraints.getCardinality() < 0) {
			// cardinality is optional (default value = input size)
			_cardinality = source._cardinality;
		} else {
			_cardinality = constraints.getCardinality();
		}
		// checking source operation type
		if (source instanceof RAEquiJoin) {
			throw new Exception("Not supported selection operation on top of join operation");
		}
		//this version peration does not support pre-grouped attributes
		_preGroupedList = null;
		_isPreGrouped = Boolean.FALSE;
	}

	@Override
	public void open() throws Exception {
		// parsing predicate
		mapPredicate = Utils.parsePredicate(_node.getCondition());
	}

	@Override
	public Tuple getNext() throws Exception {
		Tuple tuple = null;
		List<PTable> constraints = null;
		// check if the source (input) is pre-grouped on the selection
		// attribute(s), do it just the first time this method is invoked
		// get the set of attributes that participate in the predicate
		// if (_isPreGrouped == null) {
		// if (source._preGroupedList != null && source._preGroupedList.size() >
		// 0) {
		// Set<String> symbols = mapPredicate.keySet();
		// List<String> common =
		// source._preGroupedList.stream().filter(symbols::contains)
		// .collect(Collectors.toList());
		// _isPreGrouped = common != null && common.size() > 0 ? Boolean.TRUE :
		// Boolean.FALSE;
		// } else {
		// _isPreGrouped = Boolean.FALSE;
		// }
		// }
		// THIS VERSION DOES NOT SUPPORT SELECTION WITH PRE-GROUPED ATTRIBUTES
		// this case happens when a selection is on top of a join and there is
		// an attribute a in the selection
		// predicate p pre-grouped
		// in our case, we assume that all selection operations are push down by
		// the user who gives the input
		// if (!_isPreGrouped) {
		// invoke getNext() on its child
		tuple = this.source.getNext();
		if (tuple != null) {
			if (_cardinality > _counter) {
				// process the tuple with positive tuple annotation
				insertPredicates(false, constraints, tuple);
				// increment the cardinality counter
				_counter++;
				// add the tuple to the result list
				this._results.add(tuple);
			} else {
				// process with negative tuple annotation
				// the remaining tuples from its child
				while (tuple != null) {
					insertPredicates(true, constraints, tuple);
					tuple = source.getNext();
				}
				// return null to its parent
				tuple = null;
			}
		}
		// } else {
		// throw new Exception(
		// "No supported pre-grouped attributes in selection operation. Do not
		// put selection operation on top of joins");
		// }
		return tuple;
	}

	@Override
	public void close() {
		source.close();
		// reset everything
		_isPreGrouped = null;
		_counter = 0;
		_results = null;
	}

	private void insertPredicates(boolean negate, List<PTable> constraints, Tuple tuple) throws Exception {
		// for each symbol s in t that participates in the
		// selection predicate
		// p, insert a corresponding tuple <s, p> to the
		// PTable
		constraints = getPredicateBySymbol(tuple);
		if (negate) {
			// negate the constraints
			negate(constraints);
		}
		// insert all constraints in PTable
		if (constraints != null && constraints.size() > 0) {
			db.insertConstraints(_sbSchema, constraints);
		}
	}

	private void negate(List<PTable> predicates) {
		predicates.forEach(ptable -> {
			try {
				ptable.predicate = Utils.negatePredicate(ptable.predicate);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	private List<PTable> getPredicateBySymbol(Tuple tuple) {
		List<PTable> constraints = new ArrayList<>();
		// iterate over all contraints and create a PTable object
		for (Map.Entry<String, List<String>> entry : mapPredicate.entrySet()) {
			for (String predicate : entry.getValue()) {
				String value = tuple.getValues().get(entry.getKey().toUpperCase());
				PTable p = new PTable(value, Utils.updatePredicate(predicate, value));
				constraints.add(p);
			}
		}
		return constraints;
	}
}
