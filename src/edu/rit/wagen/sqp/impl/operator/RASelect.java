package edu.rit.wagen.sqp.impl.operator;

import java.util.ArrayList;
import java.util.List;

import edu.rit.wagen.dto.PTable;
import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import edu.rit.wagen.sqp.iapi.operator.UnaryOperation;
import edu.rit.wagen.utils.Utils;
import ra.RAXNode.SELECT;

/**
 * The Class RASelect.
 * @author Maria Cepeda
 */
public class RASelect extends UnaryOperation {

	/** The node. */
	private SELECT _node;
	// this approach does not work for predicates like age > 20 and age < 40
	// the negation of that predicate is age <20 or age>40, but we do not
	// contemplate 'or' conditional formulas
	/** The predicates. */
	// the system generatas age < 20 and age > 40, which it is a contradiction
	public List<Predicate> predicates;
	
	//this list will contain all the insert stmts to the PTable
	//this operator performs just one insert to the Ptable at the end
	/** The stmts. */
	//that improves performance
	public List<PTable> stmts;

	/**
	 * Instantiates a new RA select.
	 *
	 * @param source the source
	 * @param node the node
	 * @param constraints the constraints
	 * @param sbSchema the sb schema
	 * @param realSchema the real schema
	 * @throws Exception the exception
	 */
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
		// this version peration does not support pre-grouped attributes
		_preGroupedList = null;
		_isPreGrouped = Boolean.FALSE;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.sqp.iapi.operator.RAOperator#open()
	 */
	@Override
	public void open() throws Exception {
		// parsing predicate
		predicates = Utils.parsePredicate(_node.getCondition().toUpperCase());
		//init the list of inserts to the PTable
		stmts = new ArrayList<>();
		
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.sqp.iapi.operator.RAOperator#getNext()
	 */
	@Override
	public Tuple getNext() throws Exception {
		Tuple tuple = null;
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
//				System.out.println(new Date() + " select getNext() cardinality " + predicates);
				// process the tuple with positive tuple annotation
				List<PTable> constraints = insertPredicates(false, tuple);
				//add the constraints to the list of PTable inserts
				stmts.addAll(constraints);
				// increment the cardinality counter
				_counter++;
				// add the tuple to the result list
				this._results.add(tuple);
//				System.out.println(new Date() + " select getNext() cardinality finished");
			} else {
//				System.out.println(new Date() + " select getNext() cardinality reached " + predicates);
				// process with negative tuple annotation
				// the remaining tuples from its child
				while (tuple != null) {
					List<PTable> c = insertPredicates(true, tuple);
					stmts.addAll(c);
					tuple = source.getNext();
				}
				//insert predicates of the remaining tuples
				db.insertConstraints(_sbSchema, stmts);
				// return null to its parent
				tuple = null;
				close();
//				System.out.println(new Date() + " select getNext() cardinality reached finished");
			}
		}
		return tuple;
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.sqp.iapi.operator.RAOperator#close()
	 */
	@Override
	public void close() {
		source.close();
		// reset everything
		_isPreGrouped = null;
		_counter = 0;
		_results = null;
		stmts = null;
	}

	/**
	 * Insert predicates.
	 *
	 * @param negate the negate
	 * @param tuple the tuple
	 * @return the list
	 * @throws Exception the exception
	 */
	private List<PTable> insertPredicates(boolean negate, Tuple tuple) throws Exception {
		// for each symbol s in t that participates in the
		// selection predicate
		// p, insert a corresponding tuple <s, p> to the
		// PTable
		List<PTable> constraints = getPredicateBySymbol(tuple);
		if (negate) {
			// negate the constraints
			negate(constraints);
		}
		return constraints;
	}

	/**
	 * Negate.
	 *
	 * @param predicates the predicates
	 */
	private void negate(List<PTable> predicates) {
		predicates.forEach(ptable -> {
			try {
				ptable.predicate = Utils.negatePredicate(ptable.predicate);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	/**
	 * Gets the predicate by symbol.
	 *
	 * @param tuple the tuple
	 * @return the predicate by symbol
	 * @throws Exception the exception
	 */
	private List<PTable> getPredicateBySymbol(Tuple tuple) throws Exception {
		List<PTable> constraints = new ArrayList<>();
		// iterate over all contraints and create a PTable object
		for (Predicate predicate : predicates) {
			String value = tuple.getValues().get(predicate.symbol);
			if (value == null) {
				throw new Exception("Column " + predicate.symbol + " does not found in the database");
			}
			PTable p = new PTable(predicate.attribute, value, value + predicate.op + predicate.condition);
			constraints.add(p);
		}
		return constraints;
	}

}
