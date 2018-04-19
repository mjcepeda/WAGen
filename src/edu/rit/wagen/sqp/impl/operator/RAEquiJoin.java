package edu.rit.wagen.sqp.impl.operator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
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
 * The Class RAEquiJoin.
 * @author Maria Cepeda
 */
public class RAEquiJoin extends BinaryOperation {

	/** The node. */
	private JOIN _node;
	
	/** The counter dist. */
	private int _counterDist;
	
	/** The dis type. */
	private DistributionType _disType;
	
	/** The dis values. */
	// distribution values list
	private int[] _disValues;
	
	/** The m. */
	// distribution value
	private int _m;
	
	/** The j. */
	// join attributes (k from right source S), (j from left source R)
	private String k, j;
	
	/** The table S. */
	// table S with foreign key to R table
	private TableSchema tableS;
	
	/** The op S. */
	// S operator
	private RAOperator opS;
	
	/** The op R. */
	// R operator
	private RAOperator opR;
	
	/** The table R. */
	// table R
	private TableSchema tableR;
	
	/** The r. */
	private Tuple r;
	
	/** The dist ks. */
	// subset of ks that sum the cardinality
	private List<Long> distKs;
	
	/** The list input S. */
	private List<Tuple> listInputS;
	// store all the update stmts for this operation
	// we only access to the DB once
	/** The data. */
	// private List<String> stmts = new ArrayList<>();
	private List<List<String>> data = new ArrayList<>();

	/**
	 * Instantiates a new RA equi join.
	 *
	 * @param node the node
	 * @param leftSource the left source
	 * @param rightSource the right source
	 * @param constraints the constraints
	 * @param sbSchemaName the sb schema name
	 * @param realSchema the real schema
	 * @throws Exception the exception
	 */
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

	/* (non-Javadoc)
	 * @see edu.rit.wagen.sqp.iapi.operator.RAOperator#open()
	 */
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
			// instantiate a distribution generator D (QAGen:347)
			// generating the distribution values for every tuple from S
			// the domain is the size of R
			AbstractIntegerDistribution distribution = getDistributionGenerator(_cardinality, opR._cardinality);
			_disValues = distribution.sample(opR._cardinality);
			// _disValues = getUniformDist(opR._cardinality, _cardinality);
		}
	}

	/* (non-Javadoc)
	 * @see edu.rit.wagen.sqp.iapi.operator.RAOperator#getNext()
	 */
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

	/* (non-Javadoc)
	 * @see edu.rit.wagen.sqp.iapi.operator.RAOperator#close()
	 */
	@Override
	public void close() {
		getLeftSource().close();
		getRightSource().close();
		reset();
	}

	/**
	 * Reset.
	 */
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
		data = new ArrayList<>();
		distKs = null;
		listInputS = null;
		opR = null;
		opS = null;
		tableR = null;
		tableS = null;
	}

	/**
	 * Sets the R and S.
	 *
	 * @throws SQLException the SQL exception
	 */
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
			UnaryOperation op = (UnaryOperation) getRightSource();
			while (tableOp == null) {
				if (op.getSource() instanceof RATable) {
					tableOp = (RATable) op.getSource();
				} else {
					op = (UnaryOperation) op.getSource();
				}
			}
			// tableOp = (RATable) ((UnaryOperation)
			// getRightSource()).getSource();
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
				tableS = db.getReferencerTable(_realSchema, tableR, rightColumn, leftColumn);
			}
		}
		// set the symbolic database name
		tableS.setSchemaName(_sbSchema);
		tableR.setSchemaName(_sbSchema);
	}

	/**
	 * Sets the constraints.
	 *
	 * @param constraints the new constraints
	 */
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

	/**
	 * Gets the reference table.
	 *
	 * @param table the table
	 * @param joinColumn the join column
	 * @return the reference table
	 */
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

	/**
	 * Checks if is pre grouped.
	 *
	 * @return true, if is pre grouped
	 */
	private boolean isPreGrouped() {
		if (_isPreGrouped == null) {
			// if the cardinality is equal to the input cardinality of S
			// we can skip the dp function
			// treat this case as not pre-grouped
			if (_cardinality != opS._cardinality) {
				// check if the input S is pre-grouped on the join attribute k
				if (opS instanceof RAEquiJoin) {
					List<String> childTableS = new ArrayList();
					RAOperator aux = opS;
					while (aux instanceof RAEquiJoin) {
						childTableS.add(((RAEquiJoin) opS).tableS.getTableName());
						aux = ((RAEquiJoin) opS).opS;
					}
					_isPreGrouped = childTableS.contains(tableR.getTableName());
					// _isPreGrouped = opS._isPreGrouped;
					// if (!_isPreGrouped) {
					// not sure about the following assigment
					// _isPreGrouped = ((RAEquiJoin)
					// opS).tableS.getColNames().contains(k)
					// && opS._cardinality != ((RAEquiJoin)
					// opS).opS._cardinality;
					// _isPreGrouped = ((RAEquiJoin) opS).k.equals(k);
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

	/**
	 * Join case 1.
	 *
	 * @return the tuple
	 * @throws Exception the exception
	 */
	private Tuple joinCase1() throws Exception {
		Tuple t = null;
		// if cardinality has not been reached
		if (_cardinality > _counter) {
//			System.out.println(
//					new Date() + " join getNext() " + k + " cardinality " + _cardinality + " counter " + _counter);
			// if the distribution value is 0
			if (_m == 0) {
				// increment the distribution counter
				_counterDist++;
				// call the distribution generator and get the new distribution
				// value for R
				if (_counterDist < _disValues.length) {
					_m = _disValues[_counterDist];
				}
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
				// increment cardinality counter
				_counter++;
			} else {
				System.err.println("Join operation not possible, there is no more tuples to join");
				close();
			}
			//
//			System.out.println(new Date() + " join getNext() finished " + k);
		} else {
			// process negative tuple joining
			// return null to its parent
			negativeJoinCase1();
		}
		return t;
	}

	/**
	 * Join case 2.
	 *
	 * @return the tuple
	 * @throws Exception the exception
	 */
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
				// increment cardinality counter
				_counter++;
			} else {
				System.err.println("Join operation not possible, there is no more tuples to join");
				close();
			}
		} else {
			// process negative tuple joining
			// return null to its parent
			negativeJoinCase1();
		}
		return t;
	}

	/**
	 * Positive join case 1.
	 *
	 * @param r the r
	 * @param s the s
	 * @return the tuple
	 * @throws Exception the exception
	 */
	private Tuple positiveJoinCase1(Tuple r, Tuple s) throws Exception {
		Tuple join = new Tuple();
		// replace s.k for the symbol r.j
		Map<String, String> mapS = s.getValues();
		Map<String, String> mapR = r.getValues();
		// updating symbol from the join attribute
		String replaced = mapS.replace(k, mapR.get(j));
		// add update statement to the list
		data.add(Arrays.asList(mapR.get(j), replaced));
		Map<String, String> joinMap = new HashMap<>(mapR);
		// perform an equi-join on tuple r and s
		mapS.forEach((k, v) -> joinMap.putIfAbsent(k, v));
		join.setValues(joinMap);
		return join;
	}

	/**
	 * Negative join case 1.
	 *
	 * @throws Exception the exception
	 */
	private void negativeJoinCase1() throws Exception {
//		 System.out.println(new Date() + " join getNext() cardinality reached " + k);
		int counter = 0;
		// count the remaining results from R
		while (opR.getNext() != null) {
			counter++;
		}
		// count the total number of results from R base table
		int totalRows = db.countData(tableR);
		// number of js in the result list
		Set<String> jUsedSet = new HashSet<>();
		_results.forEach(tuple -> jUsedSet.add(tuple.getValues().get(j)));
		int jUsed = jUsedSet.size();
		// from number cannot be equal to the last j used
		if (counter == 0) {
			counter = 1;
		}
		int from = jUsed + counter;
		if ((opS instanceof RAEquiJoin || opS instanceof RASelect) && (totalRows == from || from > totalRows)) {
			// join S with any j from base table
			from = 1;
		}
		// create a random with the remaining ids
		Random rd = new Random();
		// join the remaining tuple from S with unused ids from base table R
		Tuple t = null;
		while ((t = opS.getNext()) != null) {
			if (totalRows == from || from > totalRows) {
				throw new Exception("There is no remaining js to perform the negative join");
			}
			int id = rd.nextInt((totalRows - from) + 1) + from;
			data.add(Arrays.asList(j + id, t.getValues().get(k)));
		}
//		 System.out.println(new Date() + " join getNext() executing updates: " 	+ data.size());
		// count the total number of results from R base table
		int totalRowsS = db.countData(tableS);
		// get the number of updated tuples from S
		int updatedS = data.size();
		// execute updates
		if (!data.isEmpty()) {
			db.execUpdates(updateStmt2(), data);
			data.clear();
		}
		for (int i = updatedS + 1; i <= totalRowsS; i++) {
			// update any tuple from S that remains no updated
			// join it with any j from base table
			int id = rd.nextInt((totalRows - 1) + 1) + 1;
			data.add(Arrays.asList(j + id, k + i));
		}
//		 System.out.println(new Date() + " join getNext() executing remaining updates: " + data.size());
		// update the remaining tuples
		if (!data.isEmpty()) {
			db.execUpdates(updateStmt2(), data);
			data.clear();
		}
		close();
//		 System.out.println(new Date() + " join getNext() cardinality reached finished");
	}

	/**
	 * Update stmt 2.
	 *
	 * @return the string
	 * @throws Exception the exception
	 */
	private String updateStmt2() throws Exception {
		StringBuffer sb = new StringBuffer("UPDATE ");
		return sb.append(_sbSchema).append(".").append(tableS.getTableName()).append(" set ").append(k)
				.append("= ? where ").append(k).append("= ?").toString();
	}

	/**
	 * Gets the distribution generator.
	 *
	 * @param cardinality the cardinality
	 * @param childCardinality the child cardinality
	 * @return the distribution generator
	 */
	private AbstractIntegerDistribution getDistributionGenerator(int cardinality, int childCardinality) {
		AbstractIntegerDistribution distribution = null;
		if (_disType.equals(DistributionType.ZIFPS)) {
			distribution = new ZipfDistribution(cardinality, 1);
		} else {
			double frequency = ((double) cardinality / (double) childCardinality);
			int min = ((int) Math.floor(frequency)) == 0 ? 1 : (int) Math.floor(frequency);
//			distribution = new UniformIntegerDistribution(min, (int) Math.ceil(frequency));
			distribution = new UniformIntegerDistribution((int) Math.ceil(frequency), (int) Math.ceil(frequency));
			// System.out.println(new Date() + " join creating distribution for
			// " + k + "=" + j + " frequency " + frequency
			// + ", min " + min + ", sample " + childCardinality);
		}
		return distribution;
	}
}
