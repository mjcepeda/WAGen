package edu.rit.wagen.sqp.iapi.operator;

import java.util.ArrayList;
import java.util.List;

import edu.rit.wagen.database.impl.DatabaseImpl;
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
	//cardinality
	public int _cardinality;
	//tuple result list
	public List<Tuple> _results;
	
	//May I change this for a list<Attribute>?
	public List<String> _preGroupedList;
	
	public Boolean _isPreGrouped;
	
	public DatabaseImpl db = new DatabaseImpl();
	
	public abstract void open() throws Exception;
	
	public abstract Tuple getNext() throws Exception;
	
	public abstract void close();
	
	public RAOperator (String sbSchema, String realSchema) {
		this._sbSchema = sbSchema;
		this._realSchema = realSchema;
		//init variables
		_counter = 0;
		_cardinality = 0;
		_results = new ArrayList<>();
		_preGroupedList = new ArrayList<>();
		_isPreGrouped = null;
	}
	
}
