package edu.rit.wagen.sqp.iapi.operator;

import java.util.List;

import edu.rit.wagen.dto.Attribute;
import edu.rit.wagen.dto.Tuple;

public abstract class Operator {

	public int counter;
	
	public int cardinality;
	//May I change this for a list<String>
	public List<Attribute> preGrouped;

	public abstract void open();
	
	public abstract Tuple getNext();
	
	public abstract void close();
}
