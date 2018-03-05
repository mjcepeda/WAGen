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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import antlr.CommonAST;
import antlr.RecognitionException;
import antlr.TokenStreamException;
import edu.rit.wagen.database.impl.DatabaseBO;
import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.graph.GraphUtils;
import edu.rit.wagen.graph.dto.Graph;
import edu.rit.wagen.instantiator.DataInstantiator;
import edu.rit.wagen.integrator.DatabaseIntegrator;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import ra.RALexer;
import ra.RAParser;
import ra.RAXConstructor;
import ra.RAXNode;

public class ExecutionPlanner {

	private final DatabaseBO db = new DatabaseBO();
	private static final PrintStream out = System.out;
	private static final PrintStream err = System.err;
	private final AtomicInteger counterSDB = new AtomicInteger(0);
	// list of symbolic databases
	private final List<String> colSDBs = new ArrayList<>();

	/** The Constant PTABLE. */
	private final static String PTABLE = "CREATE TABLE <SCHEMA_NAME>.PTABLE (SYMBOL VARCHAR(100), PREDICATE varchar(200))";

	public void init(List<String> sqlCommands, List<RAQuery> queries) {
		if (sqlCommands != null && sqlCommands.size() > 0) {
			// first, we create the real database
			// this list must have only create table statements
			// the system will create the schema automatically
			String schemaName = db.createDB(sqlCommands);
			// parser the queries and create the symbolic databases
			// the queries are relational algebra expressions, no SQL queries
			// run them in parallel
			queries.parallelStream().forEach(ra -> createSymbolicDB(ra, schemaName));
			// finding a good integration plan
			// mst_sequence is the sequence of SI operations
			String[] mst_sequence = getIntegrationPlan(colSDBs);
			// run SI operations according to the sequence
			DatabaseIntegrator.executingPlan(mst_sequence);
			// delete all the symbolic databases
			rollback();
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
				db.executeCommand(PTABLE.replaceAll("<SCHEMA_NAME>", sbSchema));
				// get the relational algebra tree and set the constrainst for
				// every operation
				RAOperator raNode = rax.getOperator(ra.getConstraints(), sbSchema, realSchema);
				Tuple t = raNode.getNext();
				//traverse the tree
				while (t != null) {
					t = raNode.getNext();
				}
				// TODO MJCG This is a temporal location for this call
				DataInstantiator instantiator = new DataInstantiator(realSchema, sbSchema);
				instantiator.generateData();
			} else {
				err.println("Size of the tables not specified");
			}
		} catch (IOException e) {
			// delete all the databases
			colSDBs.add(realSchema);
			rollback();
			e.printStackTrace();
		} catch (RecognitionException e) {
			// delete all the databases
			colSDBs.add(realSchema);
			rollback();
			e.printStackTrace();
		} catch (TokenStreamException e) {
			// delete all the databases
			colSDBs.add(realSchema);
			rollback();
			e.printStackTrace();
		} catch (Exception e) {
			// delete all the databases
			colSDBs.add(realSchema);
			rollback();
			e.printStackTrace();
		}
	}

	private String[] getIntegrationPlan(List<String> colSDBs) {
		String[] sequence = new String[colSDBs.size()];
		// build a summary graph of MSM (Maximum Satisfiable Matching) size by
		// running SI on every pair of SDBs
		Graph<String> graph = GraphUtils.buildSummaryGraph(colSDBs);
		// suggest a plan P by finding a Maximum Spanning Tree from the graph
		sequence = GraphUtils.mst(graph);
		return sequence;
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
