package edu.rit.wagen.sqp.iapi.operator;

import java.util.List;

import edu.rit.wagen.dto.Tuple;
/**
 * Relational Algebra Operator class
 * @author Maria Cepeda
 *
 */
public abstract class RAOperator {
	//this info is required to insert the predicates into the db
	//symbolic schema
	public String _sbSchema;
	//real schema
	public String _realSchema;
	//verify is the counter has reached the cardinality
	public int _counter;
	
	public int _cardinality;
	
	//May I change this for a list<Attribute>?
	public List<String> _preGroupedList;
	
	public Boolean _isPreGrouped;
	
	public abstract void open();
	
	public abstract Tuple getNext();
	
	public abstract void close();
	
	public RAOperator (String sbSchema, String realSchema) {
		this._sbSchema = sbSchema;
		this._realSchema = realSchema;
	}
	
}
