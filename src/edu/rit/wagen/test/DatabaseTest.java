package edu.rit.wagen.test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import edu.rit.wagen.database.impl.DatabaseImpl;
import edu.rit.wagen.dto.Schema;
import edu.rit.wagen.dto.TableDescription;
import edu.rit.wagen.sqp.impl.operator.Table;

public class DatabaseTest {

	@Test
	public void test() {
		Schema schema = new Schema("sampleWAGen");
		List<String> columns = Arrays.asList("id", "name", "age");
		TableDescription table = new TableDescription("sampleWAGen", "customer", columns);
		schema.addTable(table);
		DatabaseImpl db = new DatabaseImpl();
		try {
			db.createSchema(schema);
			Table t = new Table("customer", columns, 10, table);
			t.open();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
