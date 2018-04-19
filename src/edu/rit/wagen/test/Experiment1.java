package edu.rit.wagen.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.RAAnnotation.DistributionType;
import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.planner.ExecutionPlanner;

public class Experiment1 {

	public static final String QUERY_1 = "(" + "(" + "(" + "(\\select_{r_code=1} region) "
			+ "\\join_{r_regionkey = n_regionkey} nation" + ") " + "\\join_{n_nationkey= c_nationkey} customer" + ") "
			+ "\\join_{c_custkey = o_custkey} " + "(\\select_{o_orderdate=500} orders)" + ");\n";

	public static final String TABLE_REGION = "create table region (r_regionkey int primary key, r_name varchar(50), r_code int, r_comment varchar(100));\n";

	public static final String TABLE_PART = "create table part (p_partkey int primary key, p_partname varchar(50), p_brand varchar(50), "
			+ "p_type int, p_size int, p_container int, p_retailprice int);\n";

	public static final String TABLE_NATION = "create table nation (n_nationkey int primary key, n_name varchar(50), n_code int, n_regionkey int, "
			+ "n_comment varchar(100), FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey));\n";

	public static final String TABLE_SUPPLIER = "create table supplier (s_suppkey int primary key, s_name varchar(50), s_nationkey int, "
			+ "s_address varchar(200), s_phone int, s_acctbal int, s_comment varchar(100), FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey));\n";

	// public static final String TABLE_PARTSUPP = "create table partsupp
	// (PS_PARTSUPPKEY INT PRIMARY KEY, ps_partkey int, ps_suppkey int,
	// ps_availqty int, ps_supplycost int, "
	// + "ps_comment varchar(100), FOREIGN KEY (ps_partkey) REFERENCES
	// part(p_partkey), FOREIGN KEY (ps_suppkey) REFERENCES
	// supplier(s_suppkey));\n";

	public static final String TABLE_CUSTOMER = "create table customer (c_custkey int primary key, c_name varchar(50), c_address varchar(200), c_nationkey int, "
			+ "c_phone int, c_acctbal int, c_mktsegment int, c_comment varchar(100), foreign key(c_nationkey) references nation(n_nationkey));";

	public static final String TABLE_ORDERS = "create table orders (o_orderkey int primary key, o_custkey int, o_orderstatus int, o_totalprice int, "
			+ "o_orderdate int, foreign key (o_custkey) references customer(c_custkey));";

	public static final String TABLE_LINEITEM = "create table lineitem (l_lineitemkey int primary key, l_orderkey int, l_partkey int, l_suppkey int, "
			+ " linenumber int, quantity int, l_returnflag int, l_shipdate int, foreign key (l_orderkey) references orders(o_orderkey), "
			+ "foreign key (l_partkey) references part(p_partkey), foreign key (l_suppkey) references supplier(s_suppkey));";

	// @Test
	public void testQuery1() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_NATION, TABLE_CUSTOMER, TABLE_ORDERS);
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
		constraints.put(5, new RAAnnotation(1503, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(300, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(15000, DistributionType.NA));
		// R4 selection orders
		constraints.put(8, new RAAnnotation(4500, DistributionType.NA));
		// join R3, R4
		constraints.put(9, new RAAnnotation(900, DistributionType.NA));
		// the cardinality of the final sql must be 29
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY_1, SchemaTest.SQL_TPC_H_Q8, 29, constraints));
		ExecutionPlanner planner = new ExecutionPlanner(schema, 10, queries);
		planner.init();
		// planner.printQuery(QUERY_1);
	}

	@Test
	public void test_QUERY8_1M() {
		List<RAQuery> queries = Arrays.asList(SchemaTest.getQuery8());
		ExecutionPlanner planner = new ExecutionPlanner(SchemaTest.schema, 10, queries);
		planner.init();
	}
	
//	@Test
	public void test_QUERY3_1M() {
		List<RAQuery> queries = Arrays.asList(SchemaTest.getQuery3());
		ExecutionPlanner planner = new ExecutionPlanner(SchemaTest.schema, 1, queries);
		planner.init();
	}
	
//	 @Test
	public void test_QUERY8_10M() {
		List<String> schema = Arrays.asList(SchemaTest.TABLE_REGION, SchemaTest.TABLE_PART, SchemaTest.TABLE_NATION,
				SchemaTest.TABLE_SUPPLIER, SchemaTest.TABLE_CUSTOMER, SchemaTest.TABLE_ORDERS,
				SchemaTest.TABLE_LINEITEM);
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
		constraints.put(5, new RAAnnotation(1500, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(300, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(15000, DistributionType.NA));
		// R4 selection orders
		constraints.put(8, new RAAnnotation(4500, DistributionType.NA));
		constraints.put(9, new RAAnnotation(900, DistributionType.NA));
		constraints.put(10, new RAAnnotation(60000, DistributionType.NA));
		constraints.put(11, new RAAnnotation(3600, DistributionType.NA));
		constraints.put(12, new RAAnnotation(2000, DistributionType.NA));
		constraints.put(13, new RAAnnotation(12, DistributionType.NA));
		constraints.put(14, new RAAnnotation(29, DistributionType.NA));
		constraints.put(15, new RAAnnotation(100, DistributionType.NA));
		constraints.put(16, new RAAnnotation(29, DistributionType.NA));
		constraints.put(17, new RAAnnotation(25, DistributionType.NA));
		constraints.put(18, new RAAnnotation(29, DistributionType.NA));
		// the cardinality of the final sql must be 29
		List<RAQuery> queries = Arrays.asList(new RAQuery(SchemaTest.TPC_H_Q8,SchemaTest.SQL_TPC_H_Q8,29, constraints));
		ExecutionPlanner planner = new ExecutionPlanner(schema, 10, queries);
		planner.init();
		// planner.printQuery(TPC_H_Q8);
	}

	// @Test
	public void test_QUERY3_10M() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_PART, TABLE_NATION, TABLE_SUPPLIER, TABLE_CUSTOMER,
				TABLE_ORDERS, TABLE_LINEITEM);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 25
		constraints.put(2, new RAAnnotation(25, DistributionType.NA));
		// R2 join region, nation - 5
		constraints.put(3, new RAAnnotation(5, DistributionType.NA));
		// customer table size
		constraints.put(4, new RAAnnotation(1500, DistributionType.NA));
		constraints.put(5, new RAAnnotation(900, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(900, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(15000, DistributionType.NA));
		// R4 selection orders
		constraints.put(8, new RAAnnotation(4500, DistributionType.NA));
		constraints.put(9, new RAAnnotation(900, DistributionType.NA));
		// lineitem
		constraints.put(10, new RAAnnotation(60000, DistributionType.NA));
		// selection lineitem
		constraints.put(11, new RAAnnotation(5000, DistributionType.NA));
		constraints.put(12, new RAAnnotation(3600, DistributionType.NA));
		// part table size
		constraints.put(13, new RAAnnotation(2000, DistributionType.NA));
		constraints.put(14, new RAAnnotation(29, DistributionType.NA));
		// supplier table size
		constraints.put(15, new RAAnnotation(100, DistributionType.NA));
		constraints.put(16, new RAAnnotation(29, DistributionType.NA));
		// nation table size
		constraints.put(17, new RAAnnotation(25, DistributionType.NA));
		constraints.put(18, new RAAnnotation(29, DistributionType.NA));
		// the cardinality of the final sql must be 3600
		List<RAQuery> queries = Arrays.asList(new RAQuery(SchemaTest.TPC_H_Q3, SchemaTest.SQL_TPC_H_Q3, 3600, constraints));
		ExecutionPlanner planner = new ExecutionPlanner(schema, 10, queries);
		planner.init();
		// planner.printQuery(TPC_H_Q3);
	}

//	@Test
	public void test_QUERY1_10M() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_PART, TABLE_NATION, TABLE_SUPPLIER, TABLE_CUSTOMER,
				TABLE_ORDERS, TABLE_LINEITEM);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 25
		constraints.put(2, new RAAnnotation(25, DistributionType.NA));
		// customer table size
		constraints.put(4, new RAAnnotation(1500, DistributionType.NA));
		constraints.put(5, new RAAnnotation(1500, DistributionType.NA));
		// orders
		constraints.put(6, new RAAnnotation(15000, DistributionType.NA));
		constraints.put(7, new RAAnnotation(15000, DistributionType.NA));
		// lineitem
		constraints.put(8, new RAAnnotation(60000, DistributionType.NA));
		// selection lineitem
		constraints.put(9, new RAAnnotation(5000, DistributionType.NA));
		constraints.put(10, new RAAnnotation(3600, DistributionType.NA));
		// part table
		constraints.put(11, new RAAnnotation(2000, DistributionType.NA));
		constraints.put(12, new RAAnnotation(29, DistributionType.NA));
		// supplier table size
		constraints.put(13, new RAAnnotation(100, DistributionType.NA));
		constraints.put(14, new RAAnnotation(29, DistributionType.NA));
		// nation
		constraints.put(15, new RAAnnotation(25, DistributionType.NA));
		constraints.put(16, new RAAnnotation(29, DistributionType.NA));

		// the cardinality of the final sql must be 5000
		List<RAQuery> queries = Arrays.asList(new RAQuery(SchemaTest.TPC_H_Q1, SchemaTest.SQL_TPC_H_Q1, 5000, constraints));
		ExecutionPlanner planner = new ExecutionPlanner(schema, 10, queries);
		planner.init();
		// planner.printQuery(TPC_H_Q1);
	}
}
