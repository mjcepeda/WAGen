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
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

/**
 * The Class ExecutionPlanner.
 * @author Maria Cepeda
 */
public class ExecutionPlanner {

	/** The db. */
	private final DatabaseBO db = new DatabaseBO();
	
	/** The integrator. */
	private DatabaseIntegrator integrator;
	
	/** The Constant out. */
	private static final PrintStream out = System.out;
	
	/** The Constant err. */
	private static final PrintStream err = System.err;
	
	/** The counter SDB. */
	private final AtomicInteger counterSDB = new AtomicInteger(0);
	// list of symbolic databases
/** The schema. */
	//	private List<RAQuery> listQueries = new ArrayList<>();
	private List<String> schema;
	
	/** The list queries. */
	private List<RAQuery> listQueries;
	
	/** The threshold. */
	private int threshold;
	
	/**
	 * Instantiates a new execution planner.
	 *
	 * @param schema the schema
	 * @param threshold the threshold
	 * @param queries the queries
	 */
	public ExecutionPlanner(List<String> schema, int threshold, List<RAQuery> queries) {
		this.schema = schema;
		this.threshold = threshold;
		this.listQueries = queries;
	}
	
	/**
	 * Inits the.
	 */
	public void init() {
		if (schema != null && schema.size() > 0) {
			// first, we create the real database
			// this list must have only create table statements
			// the system will create the schema automatically
			try {
				String schemaName = db.createDB(schema, "wagen");
				// parser the queries and create the symbolic databases
				// the queries are relational algebra expressions, no SQL
				// queries
				// run them in parallel
				// TODO FIX the parallelization
				// queries.parallelStream().forEach(ra -> {
				// createSymbolicDB(ra, schemaName);
				// });
				listQueries.forEach(ra -> {
					createSymbolicDB(ra, schemaName);
				});
				List<String> integratedSDB = new ArrayList<>();
				if (listQueries.size() > 1) {
					long startTime = System.nanoTime();
					// integrating symbolic databases
					integrator = new DatabaseIntegrator(schemaName, threshold, schema); 
					NodeSI plan = getIntegrationPlan(listQueries);
					integratedSDB = integrator.executingPlan(plan);
					long duration = System.nanoTime() - startTime;
					long estimatedTime = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
					System.out.println(new Date() + " Database integration time: " + estimatedTime);
				} else {
					integratedSDB.add(listQueries.get(0).getSdbName());
				}
				long startTime = System.nanoTime();
				// iterate for the list of integrated databases, create a concrete DB
				// for each and generate the data
				for (String sbSchema : integratedSDB) {
					// creating a concreate database per integrated symbolic
					// database
					String finalSchema = schemaName;
					if (integratedSDB.size() > 2) {
						finalSchema = db.createDB(schema, "wagen");
					}
					DataInstantiator instantiator = new DataInstantiator(finalSchema, sbSchema);
					instantiator.generateData(schema);
				}
				long duration = System.nanoTime() - startTime;
				long estimatedTime = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
				System.out.println(new Date() + " Data instantiaton time: " + estimatedTime);
				// delete all the symbolic databases
				// TODO Uncomment this once everything is implemeted
				// rollback();
			} catch (Exception e) {
				e.printStackTrace();
				rollback();
				// TODO MJCG Include here a rollback of everything done so far
			} finally {
				try {
					db.closeConnection();
				} catch (SQLException e) {

				}
			}
		} else {
			err.println("Missing Schema definition");
		}
	}

	/**
	 * Prints the query.
	 *
	 * @param query the query
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

	/**
	 * Creates the symbolic DB.
	 *
	 * @param ra the ra
	 * @param realSchema the real schema
	 */
	private void createSymbolicDB(RAQuery ra, String realSchema) {
		try {
			if (ra.getConstraints() != null) {
				// creating name for the symbolic database
				String sbSchema = "sdb" + counterSDB.incrementAndGet();
				// create temp file with the query on it
				File tmpFile = createFile(ra.getQuery());
				// invoke RA.parser
				RAXNode rax = invokeParser(tmpFile);
				// create new symbolic db
				db.createSchema(sbSchema);
				//update the query object with the name of its symbolic database
				ra.setSdbName(sbSchema);
				// add schema to the list
//				listQueries.add(ra);
				// create PTable
				db.createPTable(sbSchema);
				// get the relational algebra tree and set the constrainst for
				// every operation
				RAOperator raNode = rax.getOperator(ra.getConstraints(), sbSchema, realSchema);
				long startTime = System.nanoTime();
				Tuple t = raNode.getNext();
				// traverse the tree
				while (t != null) {
					t = raNode.getNext();
				}
				long duration = System.nanoTime() - startTime;
				long estimatedTime = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
				System.out.println(new Date() + " Symbolic execution time: " + estimatedTime);
			} else {
				err.println("Size of the tables not specified");
			}
		} catch (IOException e) {
			// delete all the databases
			dropSchema(realSchema);
			rollback();
			listQueries = null;
			e.printStackTrace();
		} catch (RecognitionException e) {
			// delete all the databases
			dropSchema(realSchema);
			rollback();
			listQueries = null;
			e.printStackTrace();
		} catch (TokenStreamException e) {
			// delete all the databases
			dropSchema(realSchema);
			rollback();
			listQueries = null;
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			// delete all the databases
			dropSchema(realSchema);
			rollback();
			listQueries = null;
			
		}
	}

	/**
	 * Gets the integration plan.
	 *
	 * @param colSDBs the col SD bs
	 * @return the integration plan
	 */
	private NodeSI getIntegrationPlan(List<RAQuery> colSDBs) {
		Collections.reverse(colSDBs);
		return buildTree(colSDBs);
	}

	/**
	 * Builds the tree.
	 *
	 * @param colsSDBs the cols SD bs
	 * @return the node SI
	 */
	private NodeSI buildTree(List<RAQuery> colsSDBs) {
		NodeSI node = null;
		if (colsSDBs.size() == 2) {
			node = new NodeSI(colsSDBs.get(1), colsSDBs.get(0));
		} else {
			node = new NodeSI(buildTree(colsSDBs.subList(1, colsSDBs.size())), colsSDBs.get(0));
		}
		return node;
	}

	/**
	 * Creates the file.
	 *
	 * @param ra the ra
	 * @return the file
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private File createFile(String ra) throws IOException {
		File tmpFile = File.createTempFile("queryFile" + counterSDB.get(), "ra");
		tmpFile.deleteOnExit();
		FileWriter writer = new FileWriter(tmpFile);
		writer.write(ra);
		writer.close();
		return tmpFile;
	}

	/**
	 * Invoke parser.
	 *
	 * @param in the in
	 * @return the RAX node
	 * @throws FileNotFoundException the file not found exception
	 * @throws RecognitionException the recognition exception
	 * @throws TokenStreamException the token stream exception
	 */
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

	/**
	 * Prints the query.
	 *
	 * @param rax the rax
	 */
	private void printQuery(RAXNode rax) {
		out.println("Parsed query:");
		rax.print(true, 0, out);
		out.println("=====");
	}

	/**
	 * Rollback.
	 */
	private void rollback() {
		listQueries.forEach(query -> {
			dropSchema(query.getSdbName());
		});
	}
	
	/**
	 * Drop schema.
	 *
	 * @param schema the schema
	 */
	private void dropSchema (String schema) {
		try {
			db.dropSchema(schema);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
