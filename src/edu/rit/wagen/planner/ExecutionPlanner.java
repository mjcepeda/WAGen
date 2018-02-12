package edu.rit.wagen.planner;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Random;

import antlr.CommonAST;
import antlr.RecognitionException;
import antlr.TokenStreamException;
import edu.rit.wagen.database.impl.DatabaseBO;
import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.dto.Tuple;
import edu.rit.wagen.sqp.iapi.operator.RAOperator;
import ra.RALexer;
import ra.RAParser;
import ra.RAXConstructor;
import ra.RAXNode;

public class ExecutionPlanner {

	DatabaseBO db = new DatabaseBO();
	protected static PrintStream out = System.out;
	protected static PrintStream err = System.err;
	protected static Random random = new Random();
	protected int _count;
	// we need to store the temo schemas to delete them at the end
	protected List<String> listSchemas;

	/** The Constant PTABLE. */
	private final static String PTABLE = "CREATE TABLE NAME.PTABLE (SYMBOL VARCHAR(100), PREDICATE varchar(200))";

	public void init(List<String> sqlCommands, List<RAQuery> queries) {
		_count = 0;
		if (sqlCommands != null && sqlCommands.size() > 0) {
			//first, we create the real database 
			// this list must have only the create table statements
			// the system will create the schema automatically
			String schemaName = db.createDB(sqlCommands);
			// time to parser the queries and create the symbolic databases
			// the queries are relational algebra expressions, no SQL queries
			// run in parallel
			queries.parallelStream().forEach(ra -> createSymbolicDB(ra, schemaName));

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
			//TODO MJCG If there is an exception the symbolic database must be deleted
			//and the real database too
			if (ra.getConstraints() != null) {
				_count++;
				//creating name for the symbolic database
				String sbSchema = "sb" + _count;
				// create temp file with the query on it
				File tmpFile = createFile(ra.getQuery());
				// invoke RA.parser
				RAXNode rax = invokeParser(tmpFile);
				//create new symbolic db
				db.createSchema(sbSchema);
				//create PTable
				db.executeCommand(PTABLE.replaceAll("NAME", sbSchema));
				// get the relational algebra tree and set the constrainst for every operation
				RAOperator raNode = rax.getOperator(ra.getConstraints(), sbSchema, realSchema);
				Tuple t = raNode.getNext();
				while (t != null) {
					t = raNode.getNext();
				}
			} else {
				err.println("Size of the tables not specified");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RecognitionException e) {
			e.printStackTrace();
		} catch (TokenStreamException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private File createFile(String ra) throws IOException {
		File tmpFile = File.createTempFile("queryFile" + _count, "ra");
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

	private void tranverseTree(RAOperator raOp, Map<Integer, RAAnnotation> mapConstraints) {
		
	}
}
