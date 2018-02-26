package edu.rit.wagen.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.Test;

import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.RAAnnotation.DistributionType;
import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.planner.ExecutionPlanner;

public class ExecutionPlannerTest {
	// incorrect queries
	public static final String FAIL_QUERY1 = "\\select_{age < 20 and age > 40} customer;\n";
	// list of unsupported queries
	public static final String q3 = "\\select_{name='Amy'}(\\select_{name='Amy'} customer \\join_{c_id = o_cid} orders);\n";
	// unsupported query
	public static final String FAIL_QUERY2 = "\\select_{age > 20 and age < 40} customer;\n";

	public static final String QUERY1 = "\\select_{age > 20} customer;\n";
	public static final String QUERY2 = "\\select_{name='Amy'} customer \\join_{c_id = o_cid} orders;\n";
	public static final String QUERY3 = "customer \\join_{c_id = o_cid} orders;\n";
	// TPC-H Queries
	public static final String QUERY4 = "\\select_{r_code=1} region \\join_{regionkey = n_regionkey} nation;\n";
	public static final String QUERY5 = "\\project_{s_acctbal, s_name, n_name, s_address, s_phone, s_comment} ("
			+ "\\select_{r_code=1} region \\join_{regionkey = n_regionkey} nation \\join_{nationkey = s_nationkey} supplier);\n";
	public static final String QUERY6 = "\\project_{s_acctbal, s_name, n_name, s_address, s_phone, s_comment} ("
			+ "\\select_{r_code=1} region \\join_{r_regionkey = n_regionkey} nation \\join_{n_nationkey = s_nationkey} supplier "
			+ "\\join_{s_suppkey = ps_suppkey} partsupp);\n";

	public static final String TABLE_REGION = "create table region (r_regionkey int primary key, r_name varchar(50), r_code int, r_comment varchar(100));\n";
	public static final String TABLE_PART = "create table part (p_partkey int primary key, p_partname varchar(50), p_brand varchar(20), "
			+ "p_type int, p_size int, p_container int, p_retailprice int);\n";
	public static final String TABLE_NATION = "create table nation (n_nationkey int primary key, n_name varchar(50), n_code int, n_regionkey int, "
			+ "n_comment varchar(100), FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey));\n";
	public static final String TABLE_SUPPLIER = "create table supplier (s_suppkey int primary key, s_name varchar(50), s_nationkey int, "
			+ "s_address varchar(200), s_phone int, s_acctbal int, s_comment varchar(100), FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey));\n";
	public static final String TABLE_PARTSUPP = "create table partsupp (ps_partkey int primary key, ps_suppkey int, ps_availqty int, ps_supplycost int,  "
			+ "ps_comment varchar(100), FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey), FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey));\n";
	public static final String TABLE_CUSTOMER = "CREATE TABLE CUSTOMER (C_CUSTKEY INT PRIMARY KEY, C_NAME VARCHAR2(50), C_ADDRESS VARCHAR2(200), C_NATIONKEY INT, "
			+ "C_PHONE INT, C_ACCTBAL INT, C_MKTSEGMENT INT, C_COMMENT VARCHAR2(100), FOREIGN KEY (C_NATIONKEY) REFERENCES NATION (N_NATIONKEY));";
	public static final String TABLE_ORDERS = "CREATE TABLE ORDERS (O_ORDERKEY INT PRIMARY KEY, O_CUSTKEY INT, O_ORDERSTATUS INT, O_TOTALPRICE INT, "
			+ "O_ORDERDATE INT, FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY));";
	public static final String TABLE_LINEITEM = "CREATE TABLE LINEITEM (L_ORDERKEY INT PRIMARY KEY, O_CUSTKEY INT, O_ORDERSTATUS INT, O_TOTALPRICE INT, "
			+ "O_ORDERDATE INT, FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY));";

	// @Test
	public void test() {
		List<String> schema = Arrays.asList(
				"CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80) unique, age int)");
		// List<String> schema = Arrays.asList("CREATE TABLE CUSTOMER (c_id int
		// NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80) unique,
		// age int)",
		// "create table Orders (o_id int not null primary key, o_date date,
		// o_cid int, FOREIGN KEY (o_cid) REFERENCES customer(c_id))");
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		constraints.put(1, new RAAnnotation(15, DistributionType.NA));
		constraints.put(2, new RAAnnotation(5, DistributionType.NA));
		constraints.put(4, new RAAnnotation(8, DistributionType.ZIFPS));
		List<RAQuery> queries = Arrays.asList(new RAQuery(FAIL_QUERY1, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
		// planner.printQuery(q1);
	}

	// @Test
	public void test1() {
		// this test must fail, contradictory constraints
		List<String> schema = Arrays.asList(
				"CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80) unique, age int)");
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		constraints.put(1, new RAAnnotation(15, DistributionType.NA));
		constraints.put(2, new RAAnnotation(5, DistributionType.NA));
		List<RAQuery> queries = Arrays.asList(new RAQuery(FAIL_QUERY1, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
	}

	// @Test
	public void test2() {
		// this test must fail, contradictory constraints
		List<String> schema = Arrays.asList(
				"CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80) unique, age int)");
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		constraints.put(1, new RAAnnotation(15, DistributionType.NA));
		constraints.put(2, new RAAnnotation(5, DistributionType.NA));
		List<RAQuery> queries = Arrays.asList(new RAQuery(FAIL_QUERY2, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
	}

	// @Test
	public void test3() {
		List<String> schema = Arrays.asList(
				"CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80) unique, age int)");
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		constraints.put(1, new RAAnnotation(15, DistributionType.NA));
		constraints.put(2, new RAAnnotation(5, DistributionType.NA));
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY1, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
	}

	// @Test
	public void test4() {
		List<String> schema = Arrays.asList(
				"CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80) unique, age int)",
				"create table Orders (o_id int not null primary key, o_date date, o_cid int, FOREIGN KEY (o_cid) REFERENCES customer(c_id))");
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		constraints.put(2, new RAAnnotation(15, DistributionType.NA));
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY3, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
	}

	// @Test
	public void test5_TPCH_Q2_1join() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_NATION);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region cardinality (size=5)
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation cardinality (size=15)
		constraints.put(3, new RAAnnotation(15, DistributionType.NA));
		// no restrictions for select operation, uniform distribution
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY4, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
		// planner.printQuery(QUERY4);
	}

	@Test
	public void test5_TPCH_Q2_2joins() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_NATION, TABLE_SUPPLIER);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region cardinality (size=5)
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation cardinality (size=15)
		constraints.put(3, new RAAnnotation(15, DistributionType.NA));
		// supplier cardinality (size=30)
		constraints.put(5, new RAAnnotation(30, DistributionType.NA));
		// join id 7 cardinality (size=3)
		constraints.put(6, new RAAnnotation(3, DistributionType.NA));
		// no restrictions for select operation, uniform distribution in both
		// joins
		//the cardinality of the final sql must be 3
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY5, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
//		planner.printQuery(QUERY5);
	}
	
//	@Test
	public void test6_TPCH_Q2_3joins() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_NATION, TABLE_SUPPLIER, TABLE_PART, TABLE_PARTSUPP);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region cardinality (size=5)
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation cardinality (size=15)
		constraints.put(3, new RAAnnotation(15, DistributionType.NA));
		// supplier cardinality (size=30)
		constraints.put(5, new RAAnnotation(30, DistributionType.NA));
		// partsupp cardinality (size=50)
		constraints.put(7, new RAAnnotation(50, DistributionType.NA));
		// no restrictions for select operation, uniform distribution in all
		// joins
		//the cardinality of the final sql must be 50?
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY6, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
//		planner.printQuery(QUERY6);
	}

	// @Test
	public void distributionTest() {
		UniformIntegerDistribution uniform = new UniformIntegerDistribution(1, 2);
		int[] sampling = uniform.sample(4);
		for (int i = 0; i < sampling.length; i++) {
			System.out.println(sampling[i]);
		}
		System.out.println("------------------");
		ZipfDistribution zipfDistribution = new ZipfDistribution(4, 1);
		int[] sampling2 = zipfDistribution.sample(8);
		for (int i = 0; i < sampling2.length; i++) {
			System.out.println(sampling2[i]);
		}
	}

}
