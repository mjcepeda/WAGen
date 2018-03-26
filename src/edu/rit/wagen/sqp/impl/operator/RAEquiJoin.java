package edu.rit.wagen.sqp.impl.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;

import edu.rit.wagen.dto.Constraint;
import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.RAAnnotation.DistributionType;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.BinaryOperation;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import edu.rit.wagen.sqp.iapi.operator.UnaryOperation;
import edu.rit.wagen.utils.SubsetSum;
import edu.rit.wagen.utils.Utils;
import edu.rit.wagen.utils.Utils.ConstraintType;
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
	// table S with foreign key to R table
	private TableSchema tableS;
	// S operator
	private RAOperator opS;
	// R operator
	private RAOperator opR;
	// table R
	private TableSchema tableR;
	private Tuple r;
	// subset of ks that sum the cardinality
	private List<Long> distKs;
	private List<Tuple> listInputS;

	public RAEquiJoin(JOIN node, RAOperator leftSource, RAOperator rightSource, RAAnnotation constraints,
			String sbSchemaName, String realSchema) throws Exception {
		super(leftSource, rightSource, sbSchemaName, realSchema);
		reset();
		_results = new ArrayList<>();
		this._node = node;
		// determine tables R and S, S has a foreign key to R
		setRAndS();
		// setting cardinality and distribution type
		setConstraints(constraints);
	}

	@Override
	public void open() throws Exception {
		_isPreGrouped = isPreGrouped();
		if (_isPreGrouped) {
			// this is a blocking operator, it reads all the input from S first
			List<Tuple> inputS = new ArrayList<>();
			Tuple t = null;
			while ((t = opS.getNext()) != null) {
				inputS.add(t);
			}
			// extract the lists of k from the input
			List<String> kS = inputS.stream().map(tuple -> tuple.getValues().get(k)).collect(Collectors.toList());
			// extract the pre-grouped size of each symbol k_i
			Map<String, Long> countedMap = kS.stream()
					.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
			// invoke dp function with the pre-grouped sizes and the output
			// cardinatily
			distKs = SubsetSum.appOverweightSubsetSum(new ArrayList<Long>(countedMap.values()), _cardinality, 0.1);
			Collections.sort(distKs);
			Map<String, List<Tuple>> mapResultS = new HashMap<>();
			for (int i = 0; i < distKs.size(); i++) {
				// looking for the ids that has the frequency returned by the
				// subset-sum
				for (Entry<String, Long> e : countedMap.entrySet()) {
					// getting the tules with those ids
					if (e.getValue() == distKs.get(i) && !mapResultS.containsKey(e.getKey())) {
						mapResultS.put(e.getKey(), inputS.stream()
								.filter(tuple -> tuple.getValues().get(k) == e.getKey()).collect(Collectors.toList()));
					}
				}
			}
			// this list will contain c tuples (c = cardinality)
			listInputS = mapResultS.values().stream().flatMap(l -> l.stream()).collect(Collectors.toList());
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
			// generating the distribution values for every tuple from S
			// the domain is the size of R
			_disValues = distribution.sample(opR._cardinality);
			// _disValues = getUniformDist(opR._cardinality, _cardinality);
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

	private void setRAndS() throws SQLException {
		// symbol of the join key attribute from left source
		String leftColumn = _node.getCondition().substring(0, _node.getCondition().toLowerCase().indexOf(Utils.EQUALS))
				.trim().toUpperCase();
		// symbol of the join key attribute from right source
		String rightColumn = _node.getCondition()
				.substring(_node.getCondition().toLowerCase().indexOf(Utils.EQUALS), _node.getCondition().length())
				.trim().replaceAll(Utils.EQUALS, "").trim().toUpperCase();
		// since we consider only left-deep query plans
		// right source must be a table operator or a select operation with a
		// table operator as a source
		// getting the table description of right source
		RATable tableOp = null;
		if (getRightSource() instanceof RATable) {
			tableOp = (RATable) getRightSource();
		} else {
			tableOp = (RATable) ((UnaryOperation) getRightSource()).getSource();
		}
		String tableReferenced = getReferenceTable(tableOp.tableSchema, leftColumn);
		if (tableReferenced != null) {
			// the right source has the foreign key to the left source
			tableS = tableOp.tableSchema;
			tableR = db.getOutputSchema(_realSchema, tableReferenced);
			j = leftColumn;
			k = rightColumn;
			opS = getRightSource();
			opR = getLeftSource();
		} else {
			// the left source has the foreign key to the right source
			tableR = tableOp.tableSchema;
			j = rightColumn;
			k = leftColumn;
			opS = getLeftSource();
			opR = getRightSource();
			if (getLeftSource() instanceof RATable || getLeftSource() instanceof RASelect) {
				RATable tableRight = null;
				if (getLeftSource() instanceof RATable) {
					tableRight = (RATable) getLeftSource();
				} else {
					tableRight = (RATable) ((UnaryOperation) getLeftSource()).getSource();
				}
				tableS = tableRight.tableSchema;
			} else {
				// getting the table description from the join attribute
				tableS = db.getReferencerTable(_realSchema, tableR, rightColumn);
			}
		}
		// set the symbolic database name
		tableS.setSchemaName(_sbSchema);
		tableR.setSchemaName(_sbSchema);
	}

	private void setConstraints(RAAnnotation constraints) {
		// setting cardinality constraints
		if (constraints == null) {
			// cardinality is optional (default value = size of the non-distinct
			// input)
			// the non-distinct input may be a RAEquiJoin, EquiJoin only happens
			// in the left side
			if (getLeftSource() instanceof RAEquiJoin) {
				_cardinality = getLeftSource()._cardinality;
			} else {
				_cardinality = opR._cardinality;
			}
			// default distribution type
			_disType = DistributionType.UNIFORM;
		} else if (constraints.getCardinality() < 0) {
			// cardinality is optional (default value = size of the non-distinct
			// input)
			if (getLeftSource() instanceof RAEquiJoin) {
				_cardinality = getLeftSource()._cardinality;
			} else {
				_cardinality = opR._cardinality;
			}
		} else if (constraints.getDistType() == null || constraints.getDistType().equals(DistributionType.NA)) {
			// default distribution type
			_disType = DistributionType.UNIFORM;
			_cardinality = constraints.getCardinality();
		} else {
			_cardinality = constraints.getCardinality();
			_disType = constraints.getDistType();
		}
		// if the two inputs are base tables, the output cardinality must be the
		// same as the size of S (QAGen: 348)
		if (getRightSource() instanceof RATable && getLeftSource() instanceof RATable) {
			_cardinality = opS._cardinality;
			// if both sources are base tables, there is no attribute
			// pre-grouped
			_isPreGrouped = Boolean.FALSE;
			_preGroupedList = null;
		}

	}

	private String getReferenceTable(TableSchema table, String joinColumn) {
		String t = null;
		boolean found = Boolean.FALSE;
		int index = 0;
		List<Constraint> listFK = table.getConstraints().stream().filter(c -> c.type == ConstraintType.FK)
				.collect(Collectors.toList());
		while (!found && index < listFK.size()) {
			if (listFK.get(index).referencedColumn.equals(joinColumn)) {
				t = listFK.get(index).referencedTable;
				found = Boolean.TRUE;
			}
			index++;
		}
		return t;
	}

	private boolean isPreGrouped() {
		if (_isPreGrouped == null) {
			// if the cardinality is equal to the input cardinality of S
			// we can skip the dp function
			// treat this case as not pre-grouped
			if (_cardinality != opS._cardinality) {
				// check if the input S is pre-grouped on the join attribute k
				if (opS instanceof RAEquiJoin) {
					// _isPreGrouped = opS._isPreGrouped;
					// if (!_isPreGrouped) {
					_isPreGrouped = ((RAEquiJoin) opS).tableS.getColNames().contains(k)
							&& opS._cardinality != ((RAEquiJoin) opS).opS._cardinality;
					// }
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
				r = opR.getNext();
			}
			// m is the number of tuples from S that should be
			// join with R
			Tuple s = opS.getNext();
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
			// if the distribution value is 0
			if (_m == 0) {
				// increment the distribution counter
				_counterDist++;
				// call the subset of ks returned by dp
				_m = distKs.get(_counterDist).intValue();
				r = opR.getNext();
			}
			// m is the number of tuples from S that should be
			// join with R
			Tuple s = listInputS.get(_counter);
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
		// get the remaining results from R
		// and collect them
		Tuple rt = opR.getNext();
		List<Tuple> colR = new ArrayList<>();
		while (rt != null) {
			colR.add(rt);
			rt = opR.getNext();
		}
		// get the list of unused ids from base table R
		List<String> jRemainingList = getRemainingIdsFromR(colR);
		// join the remaining tuple from S with unused ids from R
		Tuple t = null;
		if ((t = opS.getNext()) != null) {
			negativeJoinUpdate(jRemainingList, t);
		}
		// update any tuple from S that remains no updated
		List<Tuple> listS = db.getTuplesForUpdate(tableS, k);
		for (Tuple tuple : listS) {
			negativeJoinUpdate(jRemainingList, tuple);
		}
	}

	private void negativeJoinUpdate(List<String> jRemainingIds, Tuple t) throws Exception {
		if (jRemainingIds.size() > 0) {
			UniformIntegerDistribution distribution = new UniformIntegerDistribution(0, jRemainingIds.size() - 1);
			int index = distribution.sample();
			// update table (S)
			updateBaseTable(jRemainingIds.get(index), t.getValues().get(k));
		} else {
			// TODO Change this please
			throw new Exception("There is no ids from " + tableR.getTableName() + " to perform the negative join");
		}
	}

	private List<String> getRemainingIdsFromR(List<Tuple> inputR) throws SQLException {
		// extract from the results list the list of js already used
		List<String> jUsedList = new ArrayList<>();
		_results.forEach(tuple -> jUsedList.add(tuple.getValues().get(j)));
		// do the same thing with the list of remaining results from the
		// left source
		inputR.forEach(tuple -> jUsedList.add(tuple.getValues().get(j)));
		// get all ids from database
		// TODO MJCG Not very efficient, time to change it?
		List<Tuple> tableScan = db.getData(tableR);
		List<String> jRemainingList = new ArrayList<>();
		tableScan.forEach(tuple -> jRemainingList.add(tuple.getValues().get(j)));
		jRemainingList.removeAll(jUsedList);
		return jRemainingList;
	}

	private void updateBaseTable(String newValue, String oldValue) throws Exception {
		StringBuffer sb = new StringBuffer("UPDATE ");
		db.execCommand(sb.append(_sbSchema).append(".").append(tableS.getTableName()).append(" set ").append(k)
				.append("='").append(newValue).append("' where ").append(k).append("='").append(oldValue).append("'")
				.toString());
	}
}
