package edu.rit.wagen.integrator;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.jgrapht.Graph;

import edu.rit.wagen.database.impl.DatabaseBO;
import edu.rit.wagen.database.impl.DatabaseImpl;
import edu.rit.wagen.dto.PTable;
import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.dto.TableSchema;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.graph.GraphUtils;
import edu.rit.wagen.integrator.NodeSI.NodeType;
import edu.rit.wagen.utils.Utils;

public class DatabaseIntegrator {

	// TODO fix these nonsense of classes
	private static final DatabaseImpl db = new DatabaseImpl();
	private static final DatabaseBO dbBO = new DatabaseBO();
	private static final PrintStream err = System.err;
	private AtomicInteger counterSDB = new AtomicInteger(0);

	public List<String> executingPlan(NodeSI plan) {
		List<String> listSDBs = new ArrayList<>();
		// run SI operations according to the tree
		String isdb = traverseTree(plan, listSDBs);
		listSDBs.add(isdb);
		return listSDBs;
	}

	private String traverseTree(NodeSI node, List<String> integratedSDBList) {
		String integratedSDB = null;
		if (node.getType().equals(NodeType.LEAF)) {
			try {
				integratedSDB = symbolicIntegration(node.getLeftSDB(), node.getRightSDB());
			} catch (Exception e) {
				e.printStackTrace();
				integratedSDBList.add(node.getLeftSDB());
			}
		} else {
			String leftSDB = traverseTree(node.getLeftNode(), integratedSDBList);
			try {
				integratedSDB = symbolicIntegration(leftSDB, node.getRightSDB());
			} catch (Exception e) {
				e.printStackTrace();
				integratedSDBList.add(leftSDB);
			}
		}
		return integratedSDB;
	}

	private String symbolicIntegration(String sdb1, String sdb2) throws Exception {
		String isdb = null;
		try {
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
				integrateCommonTable(sdb1, sdb2, isdb, table);
			}
			// insert remaining tables from the left SDB
			insertTables(sb1SchemaList, commonTablesList, sdb1, isdb);
			// insert remaining tables from the right SDB
			insertTables(sb2SchemaList, commonTablesList, sdb2, isdb);

		} catch (Exception e) {
			try {
				dbBO.dropSchema(isdb);
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			throw (e);
		}
		return isdb;
	}

	private String createSchema() throws SQLException {
		String isdb = "isdb" + counterSDB.incrementAndGet();
		db.createSchema(isdb);
		db.createPTable(isdb);
		return isdb;
	}

	private void insertTables(List<TableSchema> list1, List<TableSchema> list2, String sdb, String isdb)
			throws SQLException {
		List<TableSchema> tablesList = new ArrayList<>();
		list1.forEach(x -> {
			list2.forEach(y -> {
				if (!x.getTableName().equals(y.getTableName())) {
					tablesList.add(x);
				}
			});
		});
		if (tablesList != null && !tablesList.isEmpty()) {
			for (TableSchema table : tablesList) {
				createAndFill(sdb, isdb, table);
			}
		}
	}

	private void createAndFill(String from, String to, TableSchema table) throws SQLException {
		// create symbolic relation
		table.setSchemaName(to);
		boolean tableCreated = db.createSymbolicTable(table);
		if (tableCreated) {
			// perform an insert-select
			insertSelect(from, to, table.getTableName());
		}
	}

	private void integrateCommonTable(String sdb1, String sdb2, String isdb, TableSchema table) throws Exception {
		// the integration is done tuple by tuple for every table
		// get the nodes for the bipartite graph
		List<String> sdb1Nodes = getConstraintedNodes(sdb1, table);
		List<String> sdb2Nodes = getConstraintedNodes(sdb2, table);
		if (sdb1Nodes.size() == sdb2Nodes.size()) {
			// create a complete constrainted bipartite graph
			Graph<String, List<Predicate>> graph = GraphUtils.generateConstrainedBipartiteGraph(sdb1Nodes, sdb2Nodes);
			List<Predicate> listPredicates = GraphUtils.findMaximumSatisfiableMatching(graph, sdb1Nodes, sdb2Nodes);
			if (listPredicates != null && !listPredicates.isEmpty()) {
				// create symbolic relation
				createAndFill(sdb1, isdb, table);
				// insert the predicates from the edges into the
				// PTable relation
				insertPTable(listPredicates, isdb);
			} else {
				throw new Exception("Error integrating databases");
			}
		} else {
			throw new Exception("Unbalanced bipartite graph not supported");
		}
	}

	private List<String> getConstraintedNodes(String schema, TableSchema table) throws Exception {
		List<String> nodes = new ArrayList<>();
		// get the list of attributes that have predicates associated in the
		// predicate table
		List<String> attsList = db.getAttributesPredicates(table.getColNames(), schema);
		if (attsList != null && !attsList.isEmpty()) {
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

	private void insertSelect(String from, String to, String table) throws SQLException {
		StringBuffer stmt = new StringBuffer("insert into ");
		stmt.append(to).append(".").append(table);
		stmt.append(" select * from ").append(from).append(".").append(table);
		db.execCommand(stmt.toString());
	}

}
