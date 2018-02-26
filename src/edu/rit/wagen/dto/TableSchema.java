package edu.rit.wagen.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import edu.rit.wagen.utils.Utils.ConstraintType;

public class TableSchema {
	protected String _schemaName;
	protected String _tableName;
	protected ArrayList<String> _colNames;
	protected ArrayList<String> _colTypes;
	protected ArrayList<String> _colValues;
	protected List<Constraint> _constraints;

	public TableSchema(String schemaName, String tableName, ArrayList<String> colNames, ArrayList<String> colTypes,
			List<Constraint> c) {
		_schemaName = schemaName;
		_tableName = tableName;
		_colNames = colNames;
		_colTypes = colTypes;
		_constraints = c;
	}

	public String getSchemaName() {
		return _schemaName;
	}

	public void setSchemaName(String name) {
		this._schemaName = name;
	}

	public String getTableName() {
		return _tableName;
	}

	public ArrayList<String> getColNames() {
		return _colNames;
	}

	public ArrayList<String> getColTypes() {
		return _colTypes;
	}

	/**
	 * @return the _constraints
	 */
	public List<Constraint> getConstraints() {
		return _constraints;
	}

	public List<Constraint> getConstraints(ConstraintType type) {
		return _constraints.stream().filter(c -> c.type == type).collect(Collectors.toList());
	}

	public String toPrintString() {
		String s = _tableName;
		s += "(";
		for (int i = 0; i < _colNames.size(); i++) {
			if (i > 0)
				s += ", ";
			s += _colNames.get(i);
			s += " ";
			s += _colTypes.get(i);
		}
		s += ")";
		return s;
	}

}
