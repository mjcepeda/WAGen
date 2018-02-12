package edu.rit.wagen.sqp.impl.operator;

import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.BinaryOperation;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import ra.RAXNode.JOIN;

public class RAEquiJoin extends BinaryOperation{

	private JOIN _node;
	public RAEquiJoin(JOIN node, RAOperator leftSource, RAOperator rightSource, String sbSchemaName, String realSchema) {
		super(leftSource, rightSource, sbSchemaName, realSchema);
		this._node = node;
		_cardinality = 0;
	}
	
	@Override
	public void open() {
		if (isPreGrouped()) {
			
		} else {
			//instantiate a distribution generator D (more info in page 7)
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
	
}
