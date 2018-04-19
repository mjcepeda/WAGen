package edu.rit.wagen.test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.RAAnnotation.DistributionType;
import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.planner.ExecutionPlanner;

public class Experiment5 {

	// Query number 8 from TPC-H benchmark
	
//	@Test
	public void test_QUERY8_50M() {
		System.out.println(new Date() + " Query 8 50 M");
		List<String> schema = Arrays.asList(SchemaTest.TABLE_REGION, SchemaTest.TABLE_PART, SchemaTest.TABLE_NATION, SchemaTest.TABLE_SUPPLIER, SchemaTest.TABLE_CUSTOMER,
				SchemaTest.TABLE_ORDERS, SchemaTest.TABLE_LINEITEM);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// R1 selection region table - 1
		constraints.put(2, new RAAnnotation(1, DistributionType.NA));
		// nation table size - 25
		constraints.put(3, new RAAnnotation(25, DistributionType.NA));
		// R2 join region, nation - 5
		constraints.put(4, new RAAnnotation(5, DistributionType.NA));
		// customer table size
		constraints.put(5, new RAAnnotation(7500, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(1500, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(75000, DistributionType.NA));
		// R4 selection orders
		constraints.put(8, new RAAnnotation(22500, DistributionType.NA));
		constraints.put(9, new RAAnnotation(4500, DistributionType.NA));
		//lineitem table size
		constraints.put(10, new RAAnnotation(300000, DistributionType.NA));
		constraints.put(11, new RAAnnotation(18000, DistributionType.NA));
		constraints.put(12, new RAAnnotation(10000, DistributionType.NA));
		constraints.put(13, new RAAnnotation(60, DistributionType.NA));
		constraints.put(14, new RAAnnotation(200, DistributionType.NA));
		constraints.put(15, new RAAnnotation(500, DistributionType.NA));
		constraints.put(16, new RAAnnotation(200, DistributionType.NA));
		constraints.put(17, new RAAnnotation(25, DistributionType.NA));
		constraints.put(18, new RAAnnotation(200, DistributionType.NA));
		// the cardinality of the final sql must be 200
		List<RAQuery> queries = Arrays.asList(new RAQuery(SchemaTest.TPC_H_Q8, SchemaTest.SQL_TPC_H_Q8, 200, constraints));
		ExecutionPlanner planner = new ExecutionPlanner(schema, 0, queries);
		planner.init();
		// planner.printQuery(TPC_H_Q8);
	}


//	@Test
	public void test_QUERY3_50M() {
		System.out.println(new Date() + " Query 3 50 M");
		List<String> schema = Arrays.asList(SchemaTest.TABLE_REGION, SchemaTest.TABLE_PART, SchemaTest.TABLE_NATION, SchemaTest.TABLE_SUPPLIER, SchemaTest.TABLE_CUSTOMER,
				SchemaTest.TABLE_ORDERS, SchemaTest.TABLE_LINEITEM);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 25
		constraints.put(2, new RAAnnotation(25, DistributionType.NA));
		// R2 join region, nation - 5
		constraints.put(3, new RAAnnotation(5, DistributionType.NA));
		// customer table size
		constraints.put(4, new RAAnnotation(7500, DistributionType.NA));
		//select customer
		constraints.put(5, new RAAnnotation(4500, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(4500, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(75000, DistributionType.NA));
		// R4 selection orders
		constraints.put(8, new RAAnnotation(22500, DistributionType.NA));
		constraints.put(9, new RAAnnotation(4500, DistributionType.NA));
		// lineitem
		constraints.put(10, new RAAnnotation(300000, DistributionType.NA));
		// selection lineitem
		constraints.put(11, new RAAnnotation(25000, DistributionType.NA));
		constraints.put(12, new RAAnnotation(18000, DistributionType.NA));
		// part table size
		constraints.put(13, new RAAnnotation(10000, DistributionType.NA));
		constraints.put(14, new RAAnnotation(100, DistributionType.NA));
		// supplier table size
		constraints.put(15, new RAAnnotation(500, DistributionType.NA));
		constraints.put(16, new RAAnnotation(100, DistributionType.NA));
		// nation table size
		constraints.put(17, new RAAnnotation(25, DistributionType.NA));
		constraints.put(18, new RAAnnotation(100, DistributionType.NA));
		// the cardinality of the final sql must be 18000
		List<RAQuery> queries = Arrays.asList(new RAQuery(SchemaTest.TPC_H_Q3, SchemaTest.SQL_TPC_H_Q3, 18000, constraints));
		ExecutionPlanner planner = new ExecutionPlanner(schema, 0, queries);
		planner.init();
		// planner.printQuery(TPC_H_Q3);
	}

	 @Test
	public void test_QUERY1_50M() {
		System.out.println(new Date() + " Query 1 50 M");
		List<String> schema = Arrays.asList(SchemaTest.TABLE_REGION, SchemaTest.TABLE_PART, SchemaTest.TABLE_NATION, SchemaTest.TABLE_SUPPLIER, SchemaTest.TABLE_CUSTOMER,
				SchemaTest.TABLE_ORDERS, SchemaTest.TABLE_LINEITEM);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 25
		constraints.put(2, new RAAnnotation(25, DistributionType.NA));
		// customer table size
		constraints.put(4, new RAAnnotation(7500, DistributionType.NA));
		constraints.put(5, new RAAnnotation(7500, DistributionType.NA));
		// orders
		constraints.put(6, new RAAnnotation(75000, DistributionType.NA));
		constraints.put(7, new RAAnnotation(75000, DistributionType.NA));
		// lineitem
		constraints.put(8, new RAAnnotation(300000, DistributionType.NA));
		// selection lineitem
		constraints.put(9, new RAAnnotation(25000, DistributionType.NA));
		constraints.put(10, new RAAnnotation(18000, DistributionType.NA));
		// part table
		constraints.put(11, new RAAnnotation(10000, DistributionType.NA));
		constraints.put(12, new RAAnnotation(200, DistributionType.NA));
		// supplier table size
		constraints.put(13, new RAAnnotation(500, DistributionType.NA));
		constraints.put(14, new RAAnnotation(200, DistributionType.NA));
		// nation
		constraints.put(15, new RAAnnotation(25, DistributionType.NA));
		constraints.put(16, new RAAnnotation(200, DistributionType.NA));

		// the cardinality of the final sql must be 25000
		List<RAQuery> queries = Arrays.asList(new RAQuery(SchemaTest.TPC_H_Q1, SchemaTest.SQL_TPC_H_Q1, 25000, constraints));
		ExecutionPlanner planner = new ExecutionPlanner(schema, 0, queries);
		planner.init();
		// planner.printQuery(TPC_H_Q1);
	}
}
