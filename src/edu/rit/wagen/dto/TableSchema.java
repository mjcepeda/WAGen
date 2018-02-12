package edu.rit.wagen.dto;

import java.util.ArrayList;

public class TableSchema {
	protected String _schemaName;
	protected String _tableName;
	protected ArrayList<String> _colNames;
	protected ArrayList<String> _colTypes;

	public TableSchema(String schemaName, String tableName, ArrayList<String> colNames, ArrayList<String> colTypes) {
		_schemaName = schemaName;
		_tableName = tableName;
		_colNames = colNames;
		_colTypes = colTypes;
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
