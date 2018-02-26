package edu.rit.wagen.sqp.impl.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;

import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.RAAnnotation.DistributionType;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.BinaryOperation;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import edu.rit.wagen.sqp.iapi.operator.UnaryOperation;
import edu.rit.wagen.utils.Utils;
import ra.RAXNode.JOIN;

/**
 * Join Operator. We consider only left-deep query plans, i.e., the outer node
 * of each join is always a leaf node or a select operation
 * 
 * @author Maria Cepeda
 *
 */
public class RAEquiJoin extends BinaryOperation {

	private JOIN _node;
	private int _counterDist;
	private DistributionType _disType;
	// distribution values list
	private int[] _disValues;
	// distribution value
	private int _m;
	// join attributes (k from right source S), (j from left source R)
	private String k, j;
	// right source table (S) description
	private TableSchema tableS;
	// left source table (R) description
	private TableSchema tableR;
	private Tuple r;

	public RAEquiJoin(JOIN node, RAOperator leftSource, RAOperator rightSource, RAAnnotation constraints,
			String sbSchemaName, String realSchema) throws Exception{
		super(leftSource, rightSource, sbSchemaName, realSchema);
		reset();
		_results = new ArrayList<>();
		this._node = node;
		// setting cardinality constraints
		if (constraints == null) {
			// cardinality is optional (default value = size of the non-distinct
			// input)
			_cardinality = leftSource._cardinality;
			// default distribution type
			_disType = DistributionType.UNIFORM;
		} else if (constraints.getCardinality() < 0) {
			// cardinality is optional (default value = size of the non-distinct
			// input)
			_cardinality = rightSource._cardinality;
		} else if (constraints.getDistType() == null || constraints.getDistType().equals(DistributionType.NA)) {
			// default distribution type
			_disType = DistributionType.UNIFORM;
			_cardinality = constraints.getCardinality();
		} else {
			_cardinality = constraints.getCardinality();
			_disType = constraints.getDistType();
		}
		RATable tableOp = null;
		// if the two inputs are base tables, the output cardinality must be the
		// same as the size of right source (S) (QAGen: 348)
		if (getRightSource() instanceof RATable && getLeftSource() instanceof RATable) {
			tableOp = (RATable) getRightSource();
			_cardinality = getRightSource()._cardinality;
			// if both sources are base tables, there is no attribute
			// pre-grouped
			_isPreGrouped = Boolean.FALSE;
			_preGroupedList = null;
		}
		// symbol of the join key attribute from left source (R)
		j = _node.getCondition().substring(0, _node.getCondition().toLowerCase().indexOf(Utils.EQUALS)).trim()
				.toUpperCase();
		// symbol of the join key attribute from right source (S)
		k = _node.getCondition()
				.substring(_node.getCondition().toLowerCase().indexOf(Utils.EQUALS), _node.getCondition().length())
				.trim().replaceAll(Utils.EQUALS, "").trim().toUpperCase();
		// since we consider only left-deep query plans
		// right source must be a table operator or a select operation with a
		// table operator as a source
		// getting the table description of right source (S)
		if (tableOp == null) {
			if (getRightSource() instanceof RATable) {
				tableOp = (RATable) getRightSource();
			} else {
				tableOp = (RATable) ((UnaryOperation) getRightSource()).getSource();
			}
		}
		tableS = tableOp.tableSchema;
		// getting the table description from the join attribute of the left
		// source (R)
		tableR = db.getReferencedTable(_realSchema, tableS, j);
		if (tableR == null) {
			throw new Exception("Column " + j + " not found in table " + tableS.getTableName());
		}
		tableR.setSchemaName(sbSchemaName);

	}

	@Override
	public void open() {
		_isPreGrouped = isPreGrouped();
		// assume the left source is the R table (the referenced table)
		// the right source is the table with the foreign key constraint
		// in the join condition the attribute on the left (j) belongs to R
		// and the right attribute (k) belongs to S
		// predicate j == k
		if (_isPreGrouped) {
			// materialize the input S of the join operator
			// extract the pre-grouped size of each symbol ki
			// invoke dp function with the pre-grouped sizes and the output
			// cardinality
			// if dp cannot find any solution, report an error
		} else {
			// TODO MJCG Sometimes this distribution generator may leave some
			// tuples from right source table (S)
			// without setting a real join id, test this solution
			// instantiate a distribution generator D (QAGen:347)
			AbstractIntegerDistribution distribution = null;
			if (_disType.equals(DistributionType.ZIFPS)) {
				distribution = new ZipfDistribution(_cardinality, 1);
			} else {
				distribution = new UniformIntegerDistribution(1, _cardinality);
			}
			// generating the distribution values for every tuple from left
			// source (R)
			_disValues = distribution.sample(getLeftSource()._cardinality);
		}
	}

	@Override
	public Tuple getNext() throws Exception {
		Tuple t = null;
		if (_isPreGrouped) {
			t = joinCase2();
		} else {
			t = joinCase1();
		}
		// add tuple to the result list
		if (t != null) {
			_results.add(t);
		}
		return t;
	}

	@Override
	public void close() {
		getLeftSource().close();
		getRightSource().close();
		reset();
	}

	private void reset() {
		_isPreGrouped = null;
		_cardinality = 0;
		_counter = 0;
		_disValues = null;
		_counterDist = -1;
		_m = 0;
		k = null;
		j = null;
		_results = null;
	}

	private boolean isPreGrouped() {
		if (_isPreGrouped == null) {
			// if the cardinality is equal to the input cardinality of right
			// source (S)
			// we can skip the dp function
			// treat this case as not pre-grouped
			if (_cardinality != getRightSource()._cardinality) {
				// since we only consider left-deep query plans
				// the right source may be a base table or a selection operation
				// neither both operations can be pre-grouped (not supported)
				// so, we do not check the right source
				// left source may be a join, so we need to check if it is
				// pre-grouped
				if (getLeftSource() instanceof RAEquiJoin) {
					// check is the join attribute j is pre-grouped or not
					// to be pre-grouped must belong to the left source (R)
					_isPreGrouped = ((RAEquiJoin) getLeftSource()).tableR.getColNames().contains(j);
				} else {
					_isPreGrouped = Boolean.FALSE;
					_preGroupedList = null;
				}
			} else {
				_isPreGrouped = Boolean.FALSE;
				_preGroupedList = null;
			}
		}
		return _isPreGrouped;
	}

	private Tuple joinCase1() throws Exception {
		Tuple t = null;
		// if cardinality has not been reached
		if (_cardinality > _counter) {
			// if the distribution value is 0
			if (_m == 0) {
				// increment the distribution counter
				_counterDist++;
				// call the distribution generator and get the new distribution
				// value for R
				_m = _disValues[_counterDist];
				r = getLeftSource().getNext();
			}
			// m is the number of tuples from S that should be
			// join with R
			Tuple s = getRightSource().getNext();
			if (r != null && s != null) {
				// decrease m
				_m--;
				// join r and s with positive tuple joining
				// return this tuple to its parent
				t = positiveJoinCase1(r, s);
			}
			// increment cardinality counter
			_counter++;
		} else {
			// process negative tuple joining
			// return null to its parent
			negativeJoinCase1();
		}
		return t;
	}

	private Tuple joinCase2() throws Exception {
		Tuple t = null;
		// if cardinality has not been reached
		if (_cardinality > _counter) {
			// for each symbol ki
			// read all tuples from S with value ki
			// get tuple from R
			// join all tuples from S with R
			//
		} else {
			// process negative tuple joining
			// return null to its parent
			negativeJoinCase1();
		}
		return t;
	}

	private Tuple positiveJoinCase1(Tuple r, Tuple s) throws Exception {
		Tuple join = new Tuple();
		// replace s.k for the symbol r.j
		Map<String, String> mapS = s.getValues();
		Map<String, String> mapR = r.getValues();
		// updating symbol from the join attribute
		String replaced = mapS.replace(k, mapR.get(j));
		// update the database
		updateBaseTable(mapR.get(j), replaced);
		Map<String, String> joinMap = new HashMap<>(mapR);
		// perform an equi-join on tuple r and s
		mapS.forEach((k, v) -> joinMap.putIfAbsent(k, v));
		join.setValues(joinMap);
		return join;
	}

	private void negativeJoinCase1() throws Exception {
		// check if the left source is also a EquiJoin
		if (getLeftSource() instanceof RAEquiJoin) {
			// force to process the rest of its tuples in negative way
			getLeftSource()._counter = getLeftSource()._cardinality;
			getLeftSource().getNext();
		}
		Tuple t = getRightSource().getNext();
		if (t != null) {
			// extract from the results list the list of js already used
			List<String> jUsedList = new ArrayList<>();
			_results.forEach(tuple -> jUsedList.add(tuple.getValues().get(j)));
			// get all ids from database
			// TODO MJCG Not very efficient, time to change it?
			List<Tuple> tableScan = db.getData(tableR);
			List<String> jRemainingList = new ArrayList<>();
			tableScan.forEach(tuple -> jRemainingList.add(tuple.getValues().get(j)));
			jRemainingList.removeAll(jUsedList);
			UniformIntegerDistribution distribution = new UniformIntegerDistribution(0, jRemainingList.size() - 1);
			// get form the database the remaining js
			// fetch the remaining tuples from S
			while (t != null) {
				// for each tuple, look a symbol j- from the set minus
				// between the base table where the join attribute j
				// originates from and
				// left source (R)
				int index = distribution.sample();
				// update the right source base table (S)
				updateBaseTable(jRemainingList.get(index), t.getValues().get(k));
				t = getRightSource().getNext();
			}
		}
	}

	private void updateBaseTable(String newValue, String oldValue) throws Exception {
		StringBuffer sb = new StringBuffer("UPDATE ");
		db.execCommand(sb.append(_sbSchema).append(".").append(tableS.getTableName()).append(" set ").append(k)
				.append("='").append(newValue).append("' where ").append(k).append("='").append(oldValue).append("'")
				.toString());
	}

}
