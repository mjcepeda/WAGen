package edu.rit.wagen.sqp.impl.operator;

import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.BinaryOperation;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import ra.RAXNode.JOIN;

public class RAEquiJoin extends BinaryOperation{

	private JOIN _node;
	private int _counterDist;
	
	public RAEquiJoin(JOIN node, RAOperator leftSource, RAOperator rightSource, String sbSchemaName, String realSchema) {
		super(leftSource, rightSource, sbSchemaName, realSchema);
		this._node = node;
		_cardinality = 0;
		_counterDist = 0;
	}
	
	@Override
	public void open() {
		if (isPreGrouped()) {
			
		} else {
			//instantiate a distribution generator D (more info in page 7)
			joinDistribution();
		}
		
	}

	@Override
	public Tuple getNext() {
		if (_isPreGrouped){
			
		} else {
			
		}
		return null;
	}

	@Override
	public void close() {
		getLeftSource().close();
		getRightSource().close();
		_isPreGrouped = null;
		_cardinality = 0;
		_counter = 0;
	}

	private boolean isPreGrouped() {
		// TODO MJCG Implement this method
		return false;
	}
	
	private Tuple joinDistribution() {
		Tuple t = null;
		if (_cardinality > _counter) {
			if (_counterDist == 0) {
				//call the distribution generator D
				//_counterDist =
				//_counterDist is the number of tuples from S that should be join with r
				Tuple r = getRightSource().getNext();
				Tuple s = getLeftSource().getNext();
				if (r!= null && s!= null) {
					//decrease _counterDist
					_counterDist--;
					//join r and s with positive tuple joining
					//return this tuple to its parent
					//t = positiveJoin(r,s);
					
				}
			}
		} else {
			//process negative tuple joining
			//return null to its parent
			
		}
		return t;
	}
	
	private Tuple positiveJoin(Tuple r, Tuple s) {
		Tuple join = null;
		//replace s.k for the symbol r.j
		//update the database
		//perform an equi-join on tuple r and s
		return join;
	}
	
	private void negativeJoin() {
		//fetch the remaining tuples from S
		//for each tuple, look a symbol j
		//update the base table
	}
}
