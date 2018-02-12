package edu.rit.wagen.test;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import antlr.CommonAST;
import antlr.RecognitionException;
import antlr.TokenStreamException;
import ra.DB;
import ra.RALexer;
import ra.RALexerTokenTypes;
import ra.RAParser;
import ra.RAXConstructor;
import ra.RAXNode;

public class RelationalAlgebraTest {

	public static final String DRIVER = "com.mysql.jdbc.Driver";
	public static final String URL = "jdbc:mysql://localhost/?zeroDateTimeBehavior=convertToNull";
	public static final String USER = "root";
	public static final String PWD = "mientras";
	protected static PrintStream out = System.out;
	protected static PrintStream err = System.err;
	
	@Test
	public void test() {

		FileInputStream in;
		try {
			in = new FileInputStream("C:\\Users\\Maria\\git\\WAGen\\src\\edu\\rit\\wagen\\test\\test2.ra");
//			in = new FileInputStream("C:\\Users\\Maria\\git\\WAGen\\src\\edu\\rit\\wagen\\test\\customer_db.ra");
			DataInputStream din = new DataInputStream(in);
			Properties props = new Properties();
			props.setProperty("url", URL);
			props.setProperty("user", USER);
			props.setProperty("password", PWD);
			DB db = new DB(URL, USER, PWD, props);
			RALexer lexer = new RALexer(din);
			
			RAParser parser = new RAParser(lexer);

			parser.start();
			CommonAST ast = (CommonAST) parser.getAST();
			RAXNode rax = null;

			if (ast.getType() == RALexerTokenTypes.SQLEXEC) {
	            try {
	                assert(ast.getFirstChild().getType() == RALexerTokenTypes.OPERATOR_OPTION);
	                String sqlCommands = ast.getFirstChild().getText();
	                db.execCommands(out, sqlCommands);
	            } catch (SQLException e) {
	                err.println("Error executing SQL commands");
	                db.printSQLExceptionDetails(e, err, true);
	                err.println();
	            }
	        }
			RAXConstructor constructor = new RAXConstructor();
			RAXNode.resetViewNameGenerator();
			rax = constructor.expr(ast);
			if (true) {
				out.println("Parsed query:");
				rax.print(true, 0, out);
				out.println("=====");
			}
           
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	
}
