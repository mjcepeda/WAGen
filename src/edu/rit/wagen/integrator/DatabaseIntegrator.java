package edu.rit.wagen.integrator;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;

import edu.rit.wagen.database.impl.DatabaseBO;
import edu.rit.wagen.database.impl.DatabaseImpl;
import edu.rit.wagen.dto.IntegratedSDB;
import edu.rit.wagen.dto.PTable;
import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.graph.GraphNode;
import edu.rit.wagen.graph.GraphUtils;
import edu.rit.wagen.instantiator.DataInstantiator;
import edu.rit.wagen.integrator.NodeSI.NodeType;
import edu.rit.wagen.utils.Utils;

/**
 * The Class DatabaseIntegrator.
 * @author Maria Cepeda
 */
public class DatabaseIntegrator {

	/** The Constant db. */
	// TODO fix these nonsense of classes
	private static final DatabaseImpl db = new DatabaseImpl();
	
	/** The Constant dbBO. */
	private static final DatabaseBO dbBO = new DatabaseBO();
	
	/** The Constant err. */
	private static final PrintStream err = System.err;
	
	/** The counter SDB. */
	private AtomicInteger counterSDB = new AtomicInteger(0);

	/** The schema name. */
	private String schemaName;
	
	/** The threshold. */
	private int threshold;
	
	/** The schema definition. */
	private List<String> schemaDefinition;

	/**
	 * Instantiates a new database integrator.
	 *
	 * @param schema the schema
	 * @param threshold the threshold
	 * @param schemaDefinition the schema definition
	 */
	public DatabaseIntegrator(String schema, int threshold, List<String> schemaDefinition) {
		this.schemaName = schema;
		this.threshold = threshold;
		this.schemaDefinition = schemaDefinition;
	}

	/**
	 * Executing plan.
	 *
	 * @param plan the plan
	 * @return the list
	 */
	public List<String> executingPlan(NodeSI plan) {
		List<IntegratedSDB> listSDBs = new ArrayList<>();
		// run SI operations according to the tree
		IntegratedSDB isdb = traverseTree(plan, listSDBs);
		listSDBs.add(isdb);
		// return the name of the integrated databases
		return listSDBs.stream().map(ra -> ra.getSchema()).collect(Collectors.toList());
	}

	/**
	 * Traverse tree.
	 *
	 * @param node the node
	 * @param integratedSDBList the integrated SDB list
	 * @return the integrated SDB
	 */
	private IntegratedSDB traverseTree(NodeSI node, List<IntegratedSDB> integratedSDBList) {
		IntegratedSDB integratedSDB = null;
		if (node.getType().equals(NodeType.LEAF)) {
			try {
				integratedSDB = symbolicIntegration(node.getLeftSDB(), node.getRightSDB());
			} catch (Exception e) {
				e.printStackTrace();
				integratedSDBList.add(node.getLeftSDB());
				integratedSDB = node.getRightSDB();
			}
		} else {
			IntegratedSDB leftSDB = null;
			try {
				leftSDB = traverseTree(node.getLeftNode(), integratedSDBList);
				integratedSDB = symbolicIntegration(leftSDB, node.getRightSDB());
			} catch (Exception e) {
				e.printStackTrace();
				integratedSDBList.add(leftSDB);
				integratedSDB = node.getRightSDB();
			}
		}
		return integratedSDB;
	}

	/**
	 * Symbolic integration.
	 *
	 * @param query1 the query 1
	 * @param query2 the query 2
	 * @return the integrated SDB
	 * @throws Exception the exception
	 */
	private IntegratedSDB symbolicIntegration(IntegratedSDB query1, IntegratedSDB query2) throws Exception {
		IntegratedSDB integratedDB = null;
		String isdb = null;
		String tmpDB = null;
		try {
			System.out.println(new Date() + " Integrating " + query1.getSchema() + " and " + query2.getSchema());
			String sdb1 = query1.getSchema();
			String sdb2 = query2.getSchema();
			// get the list of tables from every SDB
			List<TableSchema> sb1SchemaList = db.getTableList(sdb1);
			List<TableSchema> sb2SchemaList = db.getTableList(sdb2);
			// get the list of tables to integrate
			List<TableSchema> commonTablesList = new ArrayList<>();
			sb1SchemaList.forEach(x -> {
				sb2SchemaList.forEach(y -> {
					if (x.getTableName().equals(y.getTableName())) {
						commonTablesList.add(x);
					}
				});
			});
			// creating symbolic database SDB1 U SDB2
			isdb = createSchema();
			// integrating common tables
			for (TableSchema table : commonTablesList) {
//				System.out.println(new Date() + " Integrating table " + table.getTableName());
				integrateCommonTable(sdb1, sdb2, isdb, table);
			}
			// insert remaining tables from the left SDB
			insertTables(sb1SchemaList, commonTablesList, sdb1, isdb);
			// insert remaining tables from the right SDB
			insertTables(sb2SchemaList, commonTablesList, sdb2, isdb);
			// create concrete database
			tmpDB = dbBO.createDB(schemaDefinition, "tmp");
			// instantiate the database
			DataInstantiator instantiator = new DataInstantiator(tmpDB, isdb);
			instantiator.generateData(schemaDefinition);
			List<RAQuery> queries = new ArrayList<>();
			queries.addAll(query1.getSdbs());
			queries.addAll(query2.getSdbs());
			integratedDB = new IntegratedSDB(isdb, queries);
			// check quality
			boolean isBelow = isBelowThreshold(integratedDB, tmpDB);
			if (!isBelow) {
				// the integration is not possible
				throw new Exception("Integration between " + sdb1 + " and " + sdb2
						+ " not possible. The quality is below the threshold");
			}
			// delete cache tables from the integrated database
			StringBuffer drop = new StringBuffer("drop table ").append(isdb).append(".");
			List<String> stmts = Arrays.asList(drop.toString() + "SYMBOL_VALUE_CACHE",
					drop.toString() + "PREDICATE_VALUE_CACHE");
			db.execCommands(stmts);
		} catch (Exception e) {
			throw (e);
		}
		return integratedDB;
	}

	/**
	 * Creates the schema.
	 *
	 * @return the string
	 * @throws SQLException the SQL exception
	 */
	private String createSchema() throws SQLException {
		String isdb = "isdb" + counterSDB.incrementAndGet();
		db.createSchema(isdb);
		db.createPTable(isdb);
		return isdb;
	}

	/**
	 * Insert tables.
	 *
	 * @param list1 the list 1
	 * @param list2 the list 2
	 * @param sdb the sdb
	 * @param isdb the isdb
	 * @throws SQLException the SQL exception
	 */
	private void insertTables(List<TableSchema> list1, List<TableSchema> list2, String sdb, String isdb)
			throws SQLException {
		List<String> tables1 = list1.stream().map(t -> t.getTableName()).collect(Collectors.toList());
		List<String> tables2 = list2.stream().map(t -> t.getTableName()).collect(Collectors.toList());
		// get and insert the remaining tables
		tables1.removeAll(tables2);
		if (!tables1.isEmpty()) {
			for (String tableName : tables1) {
				Optional<TableSchema> table = list1.stream().filter(t -> t.getTableName().equals(tableName))
						.findFirst();
				if (table.isPresent()) {
					createAndFill(sdb, isdb, table.get());
				}
			}
		}
	}

	/**
	 * Creates the and fill.
	 *
	 * @param from the from
	 * @param to the to
	 * @param table the table
	 * @throws SQLException the SQL exception
	 */
	private void createAndFill(String from, String to, TableSchema table) throws SQLException {
		// create symbolic relation
		table.setSchemaName(to);
		boolean tableCreated = db.createSymbolicTable(table);
		if (tableCreated) {
			// perform an insert-select
			insertSelect(from, to, table.getTableName());
		}
	}

	/**
	 * Integrate common table.
	 *
	 * @param sdb1 the sdb 1
	 * @param sdb2 the sdb 2
	 * @param isdb the isdb
	 * @param table the table
	 * @throws Exception the exception
	 */
	private void integrateCommonTable(String sdb1, String sdb2, String isdb, TableSchema table) throws Exception {
		List<Predicate> listPredicates = null;
		// get the list of atts with predicates for every sdb
		List<String> attsListSDB1 = db.getAttributesPredicates(table.getColNames(), sdb1);
		List<String> attsListSDB2 = db.getAttributesPredicates(table.getColNames(), sdb2);
		if ((attsListSDB1 != null && !attsListSDB1.isEmpty() && (attsListSDB2 == null))
				|| (attsListSDB2 != null && !attsListSDB2.isEmpty() && (attsListSDB1 == null))) {
			// only one of the SDBs has predicates associated with it for this
			// table
			// there is no need to create the graph
			String from = null;
			List<String> attsList = null;
			if (attsListSDB1 != null && !attsListSDB1.isEmpty()) {
				from = sdb1;
				attsList = attsListSDB1;
			}
			if (attsListSDB2 != null && !attsListSDB2.isEmpty()) {
				from = sdb2;
				attsList = attsListSDB2;
			}
			// perform an insert select to the PTable
			insertSelectPTable(from, isdb, attsList);
		} else {
			// the integration is done tuple by tuple for every table
			// get the nodes for the bipartite graph
			if (attsListSDB1 != null && attsListSDB2 != null) {
				// List<String> sdb1Nodes = getConstraintedNodes(sdb1, table,
				// attsListSDB1);
				// List<String> sdb2Nodes = getConstraintedNodes(sdb2, table,
				// attsListSDB2);
				List<GraphNode> sdb1Nodes = getConstraintedNodes2(sdb1, table, attsListSDB1);
				List<GraphNode> sdb2Nodes = getConstraintedNodes2(sdb2, table, attsListSDB2);
				if (sdb1Nodes != null && !sdb1Nodes.isEmpty() && sdb2Nodes != null && !sdb2Nodes.isEmpty()) {
					// both symbolic databases have predicates for this table
					if (sdb1Nodes.size() == sdb2Nodes.size()) {
						// create a complete constrainted bipartite graph
//						System.out.println(new Date() + " generating graph");
						long startTime = System.nanoTime();
						Graph<GraphNode, DefaultEdge/* List<Predicate> */> graph = GraphUtils
								.generateConstrainedBipartiteGraph(sdb1Nodes, sdb2Nodes);
//						System.out.println(new Date() + " finding maximum satisfiable");
						listPredicates = GraphUtils.findMaximumSatisfiableMatching(graph, sdb1Nodes, sdb2Nodes);
						long duration = System.nanoTime() - startTime;
						long estimatedTime = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
//						System.out.println(new Date() + " K-SAT-Matching time: " + estimatedTime);
//						System.out.println(new Date() + " finding maximum satisfiable finished");
						if (listPredicates == null || listPredicates.isEmpty()) {
							throw new Exception("Error integrating databases");
						} else {
							// insert the predicates into the PTable relation
							insertPTable(listPredicates, isdb);
						}
					} else {
						throw new Exception("Unbalanced bipartite graph not supported");
					}
				}
			}
		}
		// create symbolic relation
		createAndFill(sdb1, isdb, table);
	}

	/**
	 * Gets the constrainted nodes.
	 *
	 * @param schema the schema
	 * @param table the table
	 * @param attsList the atts list
	 * @return the constrainted nodes
	 * @throws Exception the exception
	 */
	private List<String> getConstraintedNodes(String schema, TableSchema table, List<String> attsList)
			throws Exception {
		List<String> nodes = null;
		// get the list of attributes that have predicates associated in the
		// predicate table
		// List<String> attsList =
		// db.getAttributesPredicates(table.getColNames(), schema);
		if (attsList != null && !attsList.isEmpty()) {
			nodes = new ArrayList<>();
			// get the data from the table
			List<Tuple> tuples = db.getData(table);
			for (Tuple t : tuples) {
				List<Predicate> tuplePredicates = new ArrayList<>();
				// get the symbols with predicates
				List<String> symbols = t.getValues().entrySet().stream()
						.filter(entry -> attsList.contains(entry.getKey())).map(Map.Entry::getValue)
						.collect(Collectors.toList());
				for (String s : symbols) {
					// get the predicates for each symbol
					tuplePredicates.addAll(db.getPredicates(schema, s));
				}
				// create one single predicate (CNF formula) with all the
				// predicates for this tuple
				String constrainedNode = tuplePredicates.stream().map(p -> p.getPredicate())
						.collect(Collectors.joining(" and "));
				nodes.add(constrainedNode);
			}
		}
		return nodes;
	}

	/**
	 * Gets the constrainted nodes 2.
	 *
	 * @param schema the schema
	 * @param table the table
	 * @param attsList the atts list
	 * @return the constrainted nodes 2
	 * @throws Exception the exception
	 */
	private List<GraphNode> getConstraintedNodes2(String schema, TableSchema table, List<String> attsList)
			throws Exception {
		List<GraphNode> nodes = null;
		// get the list of attributes that have predicates associated in the
		// predicate table
		// List<String> attsList =
		// db.getAttributesPredicates(table.getColNames(), schema);
		if (attsList != null && !attsList.isEmpty()) {
			nodes = new ArrayList<>();
			// get the data from the table
			List<Tuple> tuples = db.getData(table);
			for (Tuple t : tuples) {
				List<Predicate> tuplePredicates = new ArrayList<>();
				// get the symbols with predicates
				List<String> symbols = t.getValues().entrySet().stream()
						.filter(entry -> attsList.contains(entry.getKey())).map(Map.Entry::getValue)
						.collect(Collectors.toList());
				for (String s : symbols) {
					// get the predicates for each symbol
					tuplePredicates.addAll(db.getPredicates(schema, s));
				}
				nodes.add(new GraphNode(schema, table.getTableName(), tuplePredicates));
			}
		}
		return nodes;
	}

	/**
	 * Insert P table.
	 *
	 * @param listPredicates the list predicates
	 * @param schemaName the schema name
	 * @throws Exception the exception
	 */
	private void insertPTable(List<Predicate> listPredicates, String schemaName) throws Exception {
		List<PTable> constraints = new ArrayList<>();
		// iterate over all contraints and create a PTable
		// object
		for (Predicate predicate : listPredicates) {
			List<Predicate> listP = Utils.parsePredicate(predicate.getPredicate());
			for (Predicate p : listP) {
				PTable pTable = new PTable(p.attribute, p.symbol, p.getPredicate());
				constraints.add(pTable);
			}
		}
		db.insertConstraints(schemaName, constraints);
	}

	/**
	 * Insert select.
	 *
	 * @param from the from
	 * @param to the to
	 * @param table the table
	 * @throws SQLException the SQL exception
	 */
	private void insertSelect(String from, String to, String table) throws SQLException {
		StringBuffer stmt = new StringBuffer("insert into ");
		stmt.append(to).append(".").append(table);
		stmt.append(" select * from ").append(from).append(".").append(table);
		db.execCommand(stmt.toString());
	}

	/**
	 * Insert select P table.
	 *
	 * @param from the from
	 * @param to the to
	 * @param atts the atts
	 * @throws SQLException the SQL exception
	 */
	private void insertSelectPTable(String from, String to, List<String> atts) throws SQLException {
		StringBuffer stmt = new StringBuffer("insert into ");
		stmt.append(to).append(".").append("PTable");
		String att = atts.stream().map(a -> "'" + a + "'").collect(Collectors.joining(", "));
		stmt.append(" select * from ").append(from).append(".").append("PTable where attribute in (").append(att)
				.append(")");
		db.execCommand(stmt.toString());
	}

	/**
	 * Checks if is below threshold.
	 *
	 * @param isdb the isdb
	 * @param concreteDB the concrete DB
	 * @return true, if is below threshold
	 */
	private boolean isBelowThreshold(IntegratedSDB isdb, String concreteDB) {
		boolean isBelow = Boolean.TRUE;
		int idx = 0;
		while (isBelow && idx < isdb.getSdbs().size()) {
			RAQuery inputQuery = isdb.getSdbs().get(idx);
			try {
				int resultingCardinality = db
						.poseQuery(inputQuery.getSqlQuery().replaceAll("<SCHEMA_NAME>", concreteDB));
				// absolute error
				double absError = Math.abs(resultingCardinality - inputQuery.getCardinality());
				// calculate relative error
				double relativeError = (absError / inputQuery.getCardinality()) * 100;
//				System.out.println("Validating query " + inputQuery.getSqlQuery());
//				System.out.println("Input cardinality " + inputQuery.getCardinality() + ", output cardinality:"
//						+ resultingCardinality);
//				System.out.println("Absolute error " + absError + ", Relative error " + relativeError + ", threshold "
//						+ threshold);
				if (relativeError > threshold) {
					isBelow = Boolean.FALSE;
				}
				idx++;
			} catch (SQLException e) {
				e.printStackTrace();
				isBelow = Boolean.FALSE;
			}
		}
		return isBelow;
	}
}
