package edu.rit.wagen.test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import edu.rit.wagen.database.impl.DatabaseImpl;
import edu.rit.wagen.dto.Schema;
import edu.rit.wagen.dto.TableDescription;
import edu.rit.wagen.dto.TableSchema;

public class DatabaseTest {

	//@Test
	public void test() {
		Schema schema = new Schema("sampleWAGen");
		List<String> columns = Arrays.asList("id", "name", "age");
		TableDescription table = new TableDescription("sampleWAGen", "customer", columns);
		schema.addTable(table);
		DatabaseImpl db = new DatabaseImpl();
		try {
			db.createSchema("sampleWAGen");
//			Table t = new Table("customer", 10, table);
//			t.open();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void getTableList() {
		DatabaseImpl db = new DatabaseImpl();
		try {
			List<TableSchema> l = db.getTableList("sampleWAGen");
			if (l ==null) {
				System.out.println("no working");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
