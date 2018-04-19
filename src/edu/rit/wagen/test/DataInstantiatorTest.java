package edu.rit.wagen.test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import edu.rit.wagen.instantiator.DataInstantiator;

public class DataInstantiatorTest {

	@Test
	public void test() {
		String finalSchema = "wagen444";
		String symbolicSchema = "sb1";
		List<String> schemaDefinition = Arrays.asList(SchemaTest.TABLE_REGION, SchemaTest.TABLE_PART,
				SchemaTest.TABLE_NATION, SchemaTest.TABLE_SUPPLIER, SchemaTest.TABLE_CUSTOMER, SchemaTest.TABLE_ORDERS,
				SchemaTest.TABLE_LINEITEM);
		try {
			long startTime = System.nanoTime();
			DataInstantiator instantiator = new DataInstantiator(finalSchema, symbolicSchema);
			instantiator.generateData(schemaDefinition);
			long duration = System.nanoTime() - startTime;
			long estimatedTime = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
			System.out.println("Data instantiaton time: " + estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
