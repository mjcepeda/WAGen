package ra;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import edu.rit.wagen.sqp.impl.operator.RAEquiJoin;
import edu.rit.wagen.sqp.impl.operator.RAProjection;
import edu.rit.wagen.sqp.impl.operator.RASelect;
import edu.rit.wagen.sqp.impl.operator.RATable;

/**
 * The Class RAXNode.
 */
public abstract class RAXNode {

	/** The out. */
	protected static PrintStream out = System.out;
	
	/** The err. */
	protected static PrintStream err = System.err;
	
	/** The view generated count. */
	protected static int _viewGeneratedCount = 0;
	// WAGen relational algebra id
	// with this id we can identify the operations
	/** The ra id. */
	// and set its cardinality constraints
	protected int _raId;
	
	/** The ra id count. */
	protected static int _raIdCount = 0;

	/**
	 * Generated ra id.
	 *
	 * @return the int
	 */
	public static int generatedRaId() {
		_raIdCount++;
		return _raIdCount;
	}

	/**
	 * Prints the ra id.
	 *
	 * @return the string
	 */
	public String printRaId() {
		return "[id=" + _raId + "]";
	}

	/**
	 * Reset ra id.
	 */
	public static void resetRaId() {
		_raIdCount = 0;
	}

	/**
	 * Gets the operator.
	 *
	 * @param mapConstraints the map constraints
	 * @param sbSchema the sb schema
	 * @param realSchema the real schema
	 * @return the operator
	 * @throws Exception the exception
	 */
	public abstract RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception;

	/**
	 * Generate view name.
	 *
	 * @return the string
	 */
	public static String generateViewName() {
		_viewGeneratedCount++;
		return "RA_TMP_VIEW_" + _viewGeneratedCount;
	}

	/**
	 * Reset view name generator.
	 */
	public static void resetViewNameGenerator() {
		_viewGeneratedCount = 0;
	}

	/**
	 * The Enum Status.
	 */
	public enum Status {
		
		/** The error. */
		ERROR, 
 /** The unchecked. */
 UNCHECKED, 
 /** The correct. */
 CORRECT
	}

	/** The status. */
	protected Status _status;
	
	/** The view name. */
	protected String _viewName;
	
	/** The output schema. */
	protected DB.TableSchema _outputSchema;
	
	/** The children. */
	protected ArrayList<RAXNode> _children;

	/**
	 * Instantiates a new RAX node.
	 *
	 * @param children the children
	 */
	protected RAXNode(ArrayList<RAXNode> children) {
		_raId = generatedRaId();
		_status = Status.UNCHECKED;
		_viewName = generateViewName();
		_outputSchema = null;
		_children = children;
	}

	/**
	 * Gets the view name.
	 *
	 * @return the view name
	 */
	public String getViewName() {
		return _viewName;
	}

	/**
	 * Gets the num children.
	 *
	 * @return the num children
	 */
	public int getNumChildren() {
		return _children.size();
	}

	/**
	 * Gets the child.
	 *
	 * @param i the i
	 * @return the child
	 */
	public RAXNode getChild(int i) {
		return _children.get(i);
	}

	/**
	 * Gen view def.
	 *
	 * @param db the db
	 * @return the string
	 * @throws SQLException the SQL exception
	 * @throws ValidateException the validate exception
	 */
	public abstract String genViewDef(DB db) throws SQLException, ValidateException;

	/**
	 * Gen view create statement.
	 *
	 * @param db the db
	 * @return the string
	 * @throws SQLException the SQL exception
	 * @throws ValidateException the validate exception
	 */
	public String genViewCreateStatement(DB db) throws SQLException, ValidateException {
		return "CREATE VIEW " + _viewName + " AS " + genViewDef(db);
	}

	/**
	 * To print string.
	 *
	 * @return the string
	 */
	public abstract String toPrintString();

	/**
	 * Prints the.
	 *
	 * @param verbose the verbose
	 * @param indent the indent
	 * @param out the out
	 */
	public void print(boolean verbose, int indent, PrintStream out) {
		for (int i = 0; i < indent; i++)
			out.print(" ");
		out.print(toPrintString());
		if (verbose) {
			if (_status == Status.CORRECT) {
				out.print(" <- output schema: " + _outputSchema.toPrintString());
			} else if (_status == Status.ERROR) {
				out.print(" <- ERROR!");
			}
		}
		out.println();
		for (int i = 0; i < getNumChildren(); i++) {
			getChild(i).print(verbose, indent + 4, out);
		}
		return;
	}

	/**
	 * Validate.
	 *
	 * @param db the db
	 * @throws ValidateException the validate exception
	 */
	public void validate(DB db) throws ValidateException {
		// Validate children first; any exception thrown there
		// will shortcut the call.
		for (int i = 0; i < getNumChildren(); i++) {
			getChild(i).validate(db);
		}
		try {
			// Drop the view, just in case it is left over from
			// a previous run (shouldn't have happened if it was
			// a clean run):
			db.dropView(_viewName);
		} catch (SQLException e) {
			// Simply ignore; this is probably not safe. I would
			// imagine that we are trying to drop view8 as the root
			// view, but in a previous run view8 is used to define
			// view9, so view8 cannot be dropped before view9. A
			// robust solution seems nasty.
		}
		try {
			db.createView(genViewCreateStatement(db));
			_outputSchema = db.getTableSchema(_viewName);
			assert (_outputSchema != null);
		} catch (SQLException e) {
			_status = Status.ERROR;
			// Wrap and re-throw the exception for caller to handle.
			throw new ValidateException(e, this);
		}
		// Everything rooted at this node went smoothly.
		_status = Status.CORRECT;
		return;
	}

	/**
	 * Execute.
	 *
	 * @param db the db
	 * @param out the out
	 * @throws SQLException the SQL exception
	 */
	public void execute(DB db, PrintStream out) throws SQLException {
		assert (_status == Status.CORRECT);
		db.execQueryAndOutputResult(out, "SELECT * FROM " + _viewName);
		return;
	}

	/**
	 * Clean.
	 *
	 * @param db the db
	 * @throws SQLException the SQL exception
	 */
	public void clean(DB db) throws SQLException {
		if (_status == Status.UNCHECKED) {
			// Should be the case that the view wasn't actually created.
		} else if (_status == Status.CORRECT) {
			db.dropView(_viewName);
			_status = Status.UNCHECKED;
		} else if (_status == Status.ERROR) {
			// The view shouldn't have been created successfully; no
			// need to drop.
			_status = Status.UNCHECKED;
		}
		for (int i = 0; i < getNumChildren(); i++) {
			getChild(i).clean(db);
		}
		return;
	}

	/**
	 * The Class ValidateException.
	 */
	public static class ValidateException extends Exception {
		
		/** The sql exception. */
		protected SQLException _sqlException;
		
		/** The error node. */
		protected RAXNode _errorNode;

		/**
		 * Instantiates a new validate exception.
		 *
		 * @param sqlException the sql exception
		 * @param errorNode the error node
		 */
		public ValidateException(SQLException sqlException, RAXNode errorNode) {
			_sqlException = sqlException;
			_errorNode = errorNode;
		}

		/**
		 * Instantiates a new validate exception.
		 *
		 * @param message the message
		 * @param errorNode the error node
		 */
		public ValidateException(String message, RAXNode errorNode) {
			super(message);
			_sqlException = null;
			_errorNode = errorNode;
		}

		/**
		 * Gets the SQL exception.
		 *
		 * @return the SQL exception
		 */
		public SQLException getSQLException() {
			return _sqlException;
		}

		/**
		 * Gets the error node.
		 *
		 * @return the error node
		 */
		public RAXNode getErrorNode() {
			return _errorNode;
		}
	}

	/**
	 * The Class TABLE.
	 */
	public static class TABLE extends RAXNode {
		
		/** The table name. */
		protected String _tableName;

		/**
		 * Instantiates a new table.
		 *
		 * @param tableName the table name
		 */
		public TABLE(String tableName) {
			super(new ArrayList<RAXNode>());
			_tableName = tableName;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException {
			return "SELECT DISTINCT * FROM " + _tableName;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			return _tableName + printRaId();
		}

		/**
		 * Gets the table name.
		 *
		 * @return the table name
		 */
		public String getTableName() {
			return _tableName;
		}
		
		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			RAAnnotation annotation = mapConstraints.get(_raId);
			if (annotation== null) {
				throw new Exception("Cardinality for operator Table [" + _raId + "] not found");
			} else if (annotation.getCardinality()<0) {
				throw new Exception("Cardinality for operator Table cannot be less than zero");
			}
			RATable op = new RATable(this, annotation.getCardinality(), sbSchema, realSchema);
			//initiate the operation
			op.open();
			return op;
		}
	}

	/**
	 * The Class SELECT.
	 */
	public static class SELECT extends RAXNode {
		
		/** The condition. */
		protected String _condition;

		/**
		 * Instantiates a new select.
		 *
		 * @param condition the condition
		 * @param input the input
		 */
		public SELECT(String condition, RAXNode input) {
			super(new ArrayList<RAXNode>(Arrays.asList(input)));
			_condition = condition;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException {
			return "SELECT * FROM " + getChild(0).getViewName() + " WHERE " + _condition;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			return "\\select_" + printRaId() + "{" + _condition + "}";
		}
		
		/**
		 * Gets the condition.
		 *
		 * @return the condition
		 */
		public String getCondition() {
			return _condition;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			RAAnnotation annotation = mapConstraints.get(_raId);
			RASelect op = new RASelect(getChild(0).getOperator(mapConstraints, sbSchema, realSchema), this, annotation, sbSchema, realSchema);
			//initiate operation
			op.open();
			return op;
		}
	}

	/**
	 * The Class PROJECT.
	 */
	public static class PROJECT extends RAXNode {
		
		/** The columns. */
		protected String _columns;

		/**
		 * Instantiates a new project.
		 *
		 * @param columns the columns
		 * @param input the input
		 */
		public PROJECT(String columns, RAXNode input) {
			super(new ArrayList<RAXNode>(Arrays.asList(input)));
			_columns = columns;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException {
			return "SELECT DISTINCT " + _columns + " FROM " + getChild(0).getViewName();
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			// TODO MJCG Maybe projection does not need an id
			return "\\project_" + printRaId() + "{" + _columns + "}";
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			RAProjection op = new RAProjection(getChild(0).getOperator(mapConstraints, sbSchema, realSchema), this, sbSchema, realSchema);
			//initiate operation
			op.open();
			return op;
		}
	}

	/**
	 * The Class JOIN.
	 */
	public static class JOIN extends RAXNode {
		
		/** The condition. */
		protected String _condition;

		/**
		 * Instantiates a new join.
		 *
		 * @param condition the condition
		 * @param input1 the input 1
		 * @param input2 the input 2
		 */
		public JOIN(String condition, RAXNode input1, RAXNode input2) {
			super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
			_condition = condition;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException {
			if (_condition == null) {
				// Natural join:
				DB.TableSchema input1Schema = db.getTableSchema(getChild(0).getViewName());
				DB.TableSchema input2Schema = db.getTableSchema(getChild(1).getViewName());
				List<String> input1ColumnNames = input1Schema.getColNames();
				List<String> input2ColumnNames = input2Schema.getColNames();
				List<String> joinColumnNames = new ArrayList<String>();
				List<String> moreColumnNames = new ArrayList<String>();
				for (String col : input2ColumnNames) {
					if (input1ColumnNames.contains(col)) {
						joinColumnNames.add(col);
					} else {
						moreColumnNames.add(col);
					}
				}
				if (joinColumnNames.isEmpty()) {
					// Basically a cross product:
					return "SELECT * FROM " + getChild(0).getViewName() + ", " + getChild(1).getViewName();
				} else {
					String viewDef = "SELECT ";
					for (int i = 0; i < input1ColumnNames.size(); i++) {
						if (i > 0)
							viewDef += ", ";
						viewDef += "V1.\"" + input1ColumnNames.get(i) + "\"";
					}
					for (String col : moreColumnNames) {
						viewDef += ", V2.\"" + col + "\"";
					}
					viewDef += " FROM " + getChild(0).getViewName() + " AS V1, " + getChild(1).getViewName()
							+ " AS V2 WHERE ";
					for (int i = 0; i < joinColumnNames.size(); i++) {
						if (i > 0)
							viewDef += " AND ";
						viewDef += "V1.\"" + joinColumnNames.get(i) + "\"=V2.\"" + joinColumnNames.get(i) + "\"";
					}
					return viewDef;
				}
			} else {
				// Theta-join:
				return "SELECT * FROM " + getChild(0).getViewName() + ", " + getChild(1).getViewName() + " WHERE "
						+ _condition;
			}
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			return "\\join_" + printRaId() + "{" + _condition + "}";
		}
		
		/**
		 * Gets the condition.
		 *
		 * @return the condition
		 */
		public String getCondition() {
			return _condition;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			if (_condition == null || _condition.trim().equals("")) {
				throw new Exception("Join condition not found");
			}
			RAAnnotation annotation = mapConstraints.get(_raId);
			RAOperator op = new RAEquiJoin(this, getChild(0).getOperator(mapConstraints, sbSchema, realSchema), getChild(1).getOperator(mapConstraints, sbSchema, realSchema), annotation, sbSchema, realSchema);
			op.open();
			return op; 
		}
	}

	/**
	 * The Class CROSS.
	 */
	public static class CROSS extends RAXNode {
		
		/**
		 * Instantiates a new cross.
		 *
		 * @param input1 the input 1
		 * @param input2 the input 2
		 */
		public CROSS(RAXNode input1, RAXNode input2) {
			super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException {
			return "SELECT * FROM " + getChild(0).getViewName() + ", " + getChild(1).getViewName();
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			return "\\cross" + printRaId();
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			err.println("Cross operation not supported");
			return null;
		}
	}

	/**
	 * The Class UNION.
	 */
	public static class UNION extends RAXNode {
		
		/**
		 * Instantiates a new union.
		 *
		 * @param input1 the input 1
		 * @param input2 the input 2
		 */
		public UNION(RAXNode input1, RAXNode input2) {
			super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException {
			return "SELECT * FROM " + getChild(0).getViewName() + " UNION SELECT * FROM " + getChild(1).getViewName();
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			return "\\union" + printRaId();
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			err.println("Union operation not supported");
			return null;
		}
	}

	/**
	 * The Class DIFF.
	 */
	public static class DIFF extends RAXNode {
		
		/**
		 * Instantiates a new diff.
		 *
		 * @param input1 the input 1
		 * @param input2 the input 2
		 */
		public DIFF(RAXNode input1, RAXNode input2) {
			super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException, ValidateException {
			if (db.getDriverName().equals("com.mysql.jdbc.Driver")) {
				// MySQL doesn't support EXCEPT, so we need a workaround.
				// First, get the input schema of the children, which
				// should have already been validated so their views
				// have been created at this point:
				DB.TableSchema input1Schema = db.getTableSchema(getChild(0).getViewName());
				DB.TableSchema input2Schema = db.getTableSchema(getChild(1).getViewName());
				if (input1Schema.getColNames().size() != input2Schema.getColNames().size()) {
					throw new ValidateException(
							"taking the difference between relations with different numbers of columns", this);
				}
				String viewDef = "SELECT * FROM " + getChild(0).getViewName() + " WHERE NOT EXISTS (SELECT * FROM "
						+ getChild(1).getViewName() + " WHERE ";
				for (int i = 0; i < input1Schema.getColNames().size(); i++) {
					if (i > 0)
						viewDef += " AND ";
					viewDef += getChild(0).getViewName() + ".\"" + input1Schema.getColNames().get(i) + "\"="
							+ getChild(1).getViewName() + ".\"" + input2Schema.getColNames().get(i) + "\"";
				}
				viewDef += ")";
				return viewDef;
			} else {
				return "SELECT * FROM " + getChild(0).getViewName() + " EXCEPT SELECT * FROM "
						+ getChild(1).getViewName();
			}
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			return "\\diff" + printRaId();
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			err.println("Difference operation not supported");
			return null;
		}
	}

	/**
	 * The Class INTERSECT.
	 */
	public static class INTERSECT extends RAXNode {
		
		/**
		 * Instantiates a new intersect.
		 *
		 * @param input1 the input 1
		 * @param input2 the input 2
		 */
		public INTERSECT(RAXNode input1, RAXNode input2) {
			super(new ArrayList<RAXNode>(Arrays.asList(input1, input2)));
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException, ValidateException {
			if (db.getDriverName().equals("com.mysql.jdbc.Driver")) {
				// MySQL doesn't support INTERSECT, so we need a workaround.
				// First, get the input schema of the children, which
				// should have already been validated so their views
				// have been created at this point:
				DB.TableSchema input1Schema = db.getTableSchema(getChild(0).getViewName());
				DB.TableSchema input2Schema = db.getTableSchema(getChild(1).getViewName());
				if (input1Schema.getColNames().size() != input2Schema.getColNames().size()) {
					throw new ValidateException("intersecting relations with different numbers of columns", this);
				}
				String viewDef = "SELECT DISTINCT * FROM " + getChild(0).getViewName() + " WHERE EXISTS (SELECT * FROM "
						+ getChild(1).getViewName() + " WHERE ";
				for (int i = 0; i < input1Schema.getColNames().size(); i++) {
					if (i > 0)
						viewDef += " AND ";
					viewDef += getChild(0).getViewName() + ".\"" + input1Schema.getColNames().get(i) + "\"="
							+ getChild(1).getViewName() + ".\"" + input2Schema.getColNames().get(i) + "\"";
				}
				viewDef += ")";
				return viewDef;
			} else {
				return "SELECT * FROM " + getChild(0).getViewName() + " INTERSECT SELECT * FROM "
						+ getChild(1).getViewName();
			}
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			return "\\intersect" + printRaId();
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			err.println("Intersect operation not supported");
			return null;
		}
	}

	/**
	 * The Class RENAME.
	 */
	public static class RENAME extends RAXNode {
		
		/** The columns. */
		protected String _columns;

		/**
		 * Instantiates a new rename.
		 *
		 * @param columns the columns
		 * @param input the input
		 */
		public RENAME(String columns, RAXNode input) {
			super(new ArrayList<RAXNode>(Arrays.asList(input)));
			_columns = columns;
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewDef(ra.DB)
		 */
		public String genViewDef(DB db) throws SQLException, ValidateException {
			if (db.getDriverName().equals("org.sqlite.JDBC")) {
				// SQLite doesn't allows view column names to be
				// specified, so we have to dissect the list of new
				// column names and build the SELECT clause.
				// First, get the input schema of the child, which
				// should have already been validated so its view
				// has been created at this point:
				DB.TableSchema inputSchema = db.getTableSchema(getChild(0).getViewName());
				// Next, parse the list of new column names:
				List<String> columnNames = parseColumnNames(_columns);
				if (inputSchema.getColNames().size() != columnNames.size()) {
					throw new ValidateException("renaming an incorrect number of columns", this);
				}
				String viewDef = "SELECT ";
				for (int i = 0; i < columnNames.size(); i++) {
					if (i > 0)
						viewDef += ", ";
					viewDef += "\"" + inputSchema.getColNames().get(i) + "\" AS " + columnNames.get(i);
				}
				viewDef += " FROM " + getChild(0).getViewName();
				return viewDef;
			} else {
				return "SELECT * FROM " + getChild(0).getViewName();
			}
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#genViewCreateStatement(ra.DB)
		 */
		public String genViewCreateStatement(DB db) throws SQLException, ValidateException {
			if (db.getDriverName().equals("org.sqlite.JDBC")) {
				// See comments in genViewDef(DB):
				return "CREATE VIEW " + _viewName + " AS " + genViewDef(db);
			} else {
				return "CREATE VIEW " + _viewName + "(" + _columns + ") AS " + genViewDef(db);
			}
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#toPrintString()
		 */
		public String toPrintString() {
			return "\\rename_{" + _columns + "}";
		}

		/* (non-Javadoc)
		 * @see ra.RAXNode#getOperator(java.util.Map, java.lang.String, java.lang.String)
		 */
		@Override
		public RAOperator getOperator(Map<Integer, RAAnnotation> mapConstraints, String sbSchema, String realSchema) throws Exception {
			err.println("Rename operation not supported");
			return null;
		}
	}

	/**
	 * Parses the column names.
	 *
	 * @param columns the columns
	 * @return the list
	 */
	public static List<String> parseColumnNames(String columns) {
		String[] columnNames = columns.split("\\s*,\\s*");
		return Arrays.asList(columnNames);
	}
}
