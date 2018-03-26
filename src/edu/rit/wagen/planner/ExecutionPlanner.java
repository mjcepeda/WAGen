package edu.rit.wagen.planner;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import antlr.CommonAST;
import antlr.RecognitionException;
import antlr.TokenStreamException;
import edu.rit.wagen.database.impl.DatabaseBO;
import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.instantiator.DataInstantiator;
import edu.rit.wagen.integrator.DatabaseIntegrator;
import edu.rit.wagen.integrator.NodeSI;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import ra.RALexer;
import ra.RAParser;
import ra.RAXConstructor;
import ra.RAXNode;

public class ExecutionPlanner {

	private final DatabaseBO db = new DatabaseBO();
	private final DatabaseIntegrator integrator = new DatabaseIntegrator();
	private static final PrintStream out = System.out;
	private static final PrintStream err = System.err;
	private final AtomicInteger counterSDB = new AtomicInteger(0);
	// list of symbolic databases
	private List<String> colSDBs = new ArrayList<>();

	public void init(List<String> schema, List<RAQuery> queries) {
		if (schema != null && schema.size() > 0) {
			// first, we create the real database
			// this list must have only create table statements
			// the system will create the schema automatically
			try {
				String schemaName = db.createDB(schema);
				// parser the queries and create the symbolic databases
				// the queries are relational algebra expressions, no SQL
				// queries
				// run them in parallel
				// TODO FIX the parallelization
				// queries.parallelStream().forEach(ra -> {
				// createSymbolicDB(ra, schemaName);
				// });
				queries.forEach(ra -> {
					createSymbolicDB(ra, schemaName);
				});
				List<String> integratedSDB = new ArrayList<>();
				if (colSDBs.size() > 1) {
					// call the integrator here that returns the tree plan
					// SDB
					NodeSI plan = getIntegrationPlan(colSDBs);
					integratedSDB = integrator.executingPlan(plan);
				} else {
					integratedSDB.addAll(colSDBs);
				}
				// iterate for the list of final databases, create a concrete DB
				// for each and generate the data
				for (String sbSchema : integratedSDB) {
					// creating a concreate database per integrated symbolic
					// database
					String finalSchema = schemaName;
					if (integratedSDB.size() > 1) {
						finalSchema = db.createDB(schema);
					}
					DataInstantiator instantiator = new DataInstantiator(finalSchema, sbSchema);
					instantiator.generateData();
				}
				// delete all the symbolic databases
				// TODO Uncomment this once everything is implemeted
				// rollback();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			err.println("Missing Schema definition");
		}
	}

	/**
	 * Print the parsed relational algebra query. This method helps to identify
	 * the id numbers of every operator.
	 * 
	 * @param query
	 */
	public void printQuery(String query) {
		// creating file and insert the query on it
		File f;
		try {
			f = createFile(query);
			// parser the query using ra interpreter
			RAXNode rax = invokeParser(f);
			printQuery(rax);
		} catch (IOException e) {
			err.println("Error creating temp file");
			e.printStackTrace();
		} catch (RecognitionException e) {
			err.println("Error parsing query");
			e.printStackTrace();
		} catch (TokenStreamException e) {
			err.println("Error parsing query");
			e.printStackTrace();
		}
	}

	private void createSymbolicDB(RAQuery ra, String realSchema) {
		try {
			if (ra.getConstraints() != null) {
				// creating name for the symbolic database
				String sbSchema = "sb" + counterSDB.incrementAndGet();
				// create temp file with the query on it
				File tmpFile = createFile(ra.getQuery());
				// invoke RA.parser
				RAXNode rax = invokeParser(tmpFile);
				// create new symbolic db
				db.createSchema(sbSchema);
				// add schema to the list
				colSDBs.add(sbSchema);
				// create PTable
				db.createPTable(sbSchema);
				// get the relational algebra tree and set the constrainst for
				// every operation
				RAOperator raNode = rax.getOperator(ra.getConstraints(), sbSchema, realSchema);
				Tuple t = raNode.getNext();
				// traverse the tree
				while (t != null) {
					t = raNode.getNext();
				}

			} else {
				err.println("Size of the tables not specified");
			}
		} catch (IOException e) {
			// delete all the databases
			colSDBs.add(realSchema);
			rollback();
			colSDBs = null;
			e.printStackTrace();
		} catch (RecognitionException e) {
			// delete all the databases
			colSDBs.add(realSchema);
			rollback();
			colSDBs = null;
			e.printStackTrace();
		} catch (TokenStreamException e) {
			// delete all the databases
			colSDBs.add(realSchema);
			rollback();
			colSDBs = null;
			e.printStackTrace();
		} catch (Exception e) {
			// delete all the databases
			colSDBs.add(realSchema);
			rollback();
			colSDBs = null;
			e.printStackTrace();
		}
	}

	private NodeSI getIntegrationPlan(List<String> colSDBs) {
		Collections.reverse(colSDBs);
		return buildTree(colSDBs);
	}

	private NodeSI buildTree(List<String> colsSDBs) {
		NodeSI node = null;
		if (colsSDBs.size() == 2) {
			node = new NodeSI(colsSDBs.get(1), colsSDBs.get(0));
		} else {
			node = new NodeSI(buildTree(colsSDBs.subList(1, colsSDBs.size())), colsSDBs.get(0));
		}
		return node;
	}

	private File createFile(String ra) throws IOException {
		File tmpFile = File.createTempFile("queryFile" + counterSDB.get(), "ra");
		tmpFile.deleteOnExit();
		FileWriter writer = new FileWriter(tmpFile);
		writer.write(ra);
		writer.close();
		return tmpFile;
	}

	private RAXNode invokeParser(File in) throws FileNotFoundException, RecognitionException, TokenStreamException {
		InputStream targetStream = new FileInputStream(in);
		DataInputStream din = new DataInputStream(targetStream);
		RALexer lexer = new RALexer(din);
		RAParser parser = new RAParser(lexer);
		parser.start();
		CommonAST ast = (CommonAST) parser.getAST();
		RAXConstructor constructor = new RAXConstructor();
		RAXNode.resetViewNameGenerator();
		RAXNode.resetRaId();
		RAXNode rax = constructor.expr(ast);
		return rax;
	}

	private void printQuery(RAXNode rax) {
		out.println("Parsed query:");
		rax.print(true, 0, out);
		out.println("=====");
	}

	/**
	 * Delete the list of databases
	 */
	private void rollback() {
		colSDBs.forEach(schema -> {
			try {
				db.dropSchema(schema);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
	}
}
