package edu.rit.wagen.sqp.impl.operator;

import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import edu.rit.wagen.sqp.iapi.operator.UnaryOperation;
import ra.RAXNode.PROJECT;

public class RAProjection extends UnaryOperation {

	private PROJECT _node;

	public RAProjection(RAOperator source, PROJECT node, String sbSchema, String realSchema) {
		super(source, sbSchema, realSchema);
		this._node = node;
		this._cardinality = source._cardinality;
	}

	@Override
	public void open() {
	}

	@Override
	public Tuple getNext() throws Exception{
		//this operation does not add additional constraints
		return source.getNext();
	}

	@Override
	public void close() {
		source.close();
	}

}
