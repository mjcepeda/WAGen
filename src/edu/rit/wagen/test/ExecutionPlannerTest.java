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
	public static final String QUERY2 = "\\select_{name='Amy'} customer \\join_{c_id = o_cid} orders;\n";

	public static final String QUERY1 = "\\select_{age > 20} customer;\n";
	
	public static final String QUERY1_2 = "\\select_{age < 30} customer;\n";
	
	// select count(*) from customer, orders where c_id = o_cid
	public static final String QUERY3 = "customer \\join_{c_id = o_cid} orders;\n";
	public static final String QUERY3_2 = "orders \\join_{o_cid = c_id} customer;\n";
	// TPC-H Queries
	// select * from region, nation where r_regionkey = n_regionkey and r_code
	// =1
	public static final String QUERY4 = "\\select_{r_code=1} region \\join_{r_regionkey = n_regionkey} nation;\n";
	// select count(*) from region, nation, supplier where
	// r_regionkey=n_regionkey and n_nationkey = s_nationkey and r_code =1
	public static final String QUERY5 = "\\project_{s_acctbal, s_name, n_name, s_address, s_phone, s_comment} ("
			+ "\\select_{r_code=1} region \\join_{r_regionkey = n_regionkey} nation \\join_{n_nationkey = s_nationkey} supplier);\n";
	// select count(*) from WAGEN523.region, WAGEN523.nation, WAGEN523.supplier,
	// WAGEN523.partsupp, WAGEN523.part
	// where r_regionkey=n_regionkey and n_nationkey = s_nationkey
	// and S_SUPPKEY = PS_SUPPKEY AND PS_PARTKEY = P_PARTKEY
	public static final String QUERY6 = "\\project_{s_acctbal, s_name, n_name, s_address, s_phone, s_comment} ("
			+ "(((\\select_{r_code=1} region \\join_{r_regionkey = n_regionkey} nation) "
			+ "\\join_{n_nationkey = s_nationkey} supplier) " + "\\join_{s_suppkey = ps_suppkey} partsupp) "
			+ "\\join_{ps_partkey = p_partkey} \\select_{p_size=50 and p_type=36} part);\n";

	public static final String QUERY7 = "\\project_{s_acctbal, s_name, n_name, s_address, s_phone, s_comment} ("
			+ "(((region \\join_{r_regionkey = n_regionkey} nation) " + "\\join_{n_nationkey = s_nationkey} supplier) "
			+ "\\join_{s_suppkey = ps_suppkey} partsupp) "
			+ "\\join_{ps_partkey = p_partkey} \\select_{p_size=50 and p_type=36} part);\n";

	public static final String QUERY8 = "(((" + "part \\join_{p_partkey=ps_partkey} partsupp) "
			+ "\\join_{ps_suppkey = s_suppkey} supplier) " + "\\join_{s_nationkey = n_nationkey} nation) "
			+ "\\join_{n_regionkey = r_regionkey} region;\n";

	public static final String TABLE_REGION = "create table region (r_regionkey int primary key, r_name varchar(50), r_code int, r_comment varchar(100));\n";
	public static final String TABLE_PART = "create table part (p_partkey int primary key, p_partname varchar(50), p_brand varchar(50), "
			+ "p_type int, p_size int, p_container int, p_retailprice int);\n";
	public static final String TABLE_NATION = "create table nation (n_nationkey int primary key, n_name varchar(50), n_code int, n_regionkey int, "
			+ "n_comment varchar(100), FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey));\n";
	public static final String TABLE_SUPPLIER = "create table supplier (s_suppkey int primary key, s_name varchar(50), s_nationkey int, "
			+ "s_address varchar(200), s_phone int, s_acctbal int, s_comment varchar(100), FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey));\n";
	public static final String TABLE_PARTSUPP = "create table partsupp (PS_PARTSUPPKEY INT PRIMARY KEY, ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost int,  "
			+ "ps_comment varchar(100), FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey), FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey));\n";
	public static final String TABLE_CUSTOMER = "CREATE TABLE CUSTOMER (C_CUSTKEY INT PRIMARY KEY, C_NAME VARCHAR2(50), C_ADDRESS VARCHAR2(200), C_NATIONKEY INT, "
			+ "C_PHONE INT, C_ACCTBAL INT, C_MKTSEGMENT INT, C_COMMENT VARCHAR2(100), FOREIGN KEY (C_NATIONKEY) REFERENCES NATION (N_NATIONKEY));";
	public static final String TABLE_ORDERS = "CREATE TABLE ORDERS (O_ORDERKEY INT PRIMARY KEY, O_CUSTKEY INT, O_ORDERSTATUS INT, O_TOTALPRICE INT, "
			+ "O_ORDERDATE INT, FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY));";
	public static final String TABLE_LINEITEM = "CREATE TABLE LINEITEM (L_LINEITEMKEY INT PRIMARY KEY, L_ORDERKEY INT, L_CUSTKEY INT, L_PARTSUPPKEY INT, "
			+ "O_ORDERDATE INT, FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS (O_ORDERKEY), FOREIGN KEY (L_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY), "
			+ "FOREIGN KEY (L_PARTSUPPKEY) REFERENCES CUSTOMER (PS_PARTSUPPKEY));";

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
	public void test4_orderChanged() {
		List<String> schema = Arrays.asList(
				"CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80) unique, age int)",
				"create table Orders (o_id int not null primary key, o_date date, o_cid int, FOREIGN KEY (o_cid) REFERENCES customer(c_id))");
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		constraints.put(1, new RAAnnotation(15, DistributionType.NA));
		constraints.put(2, new RAAnnotation(5, DistributionType.NA));
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY3_2, constraints));
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
		// select cardinality 3 (r_code =1)
		constraints.put(2, new RAAnnotation(3, DistributionType.NA));
		// join cardinality 6
		constraints.put(4, new RAAnnotation(6, DistributionType.NA));
		// no restrictions for select operation, uniform distribution
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY4, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
		// planner.printQuery(QUERY4);
	}

	// @Test
	public void test5_TPCH_Q2_2joins() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_NATION, TABLE_SUPPLIER);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region cardinality (size=5)
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation cardinality (size=15)
		constraints.put(3, new RAAnnotation(15, DistributionType.NA));
		// supplier cardinality (size=30)
		constraints.put(5, new RAAnnotation(30, DistributionType.NA));
		// select cardinality 3 (r_code=1)
		constraints.put(2, new RAAnnotation(3, DistributionType.NA));
		// join id 7 (nation, supplier) cardinality (size=12)
		constraints.put(6, new RAAnnotation(10, DistributionType.NA));
		// the cardinality of the final sql is equal to constraint 6 value
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY5, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
		// planner.printQuery(QUERY5);
	}

//	@Test
	public void test6_TPCH_Q2_4joins() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_NATION, TABLE_SUPPLIER, TABLE_PART, TABLE_PARTSUPP);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region cardinality (size=5)
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation cardinality (size=15)
		constraints.put(3, new RAAnnotation(15, DistributionType.NA));
		// supplier cardinality (size=30)
		constraints.put(5, new RAAnnotation(17, DistributionType.NA));
		// partsupp cardinality (size=50)
		constraints.put(7, new RAAnnotation(20, DistributionType.NA));
		// part cardinality (size=40)
		constraints.put(9, new RAAnnotation(22, DistributionType.NA));
		// select cardinality 3 (r_code=1)
		constraints.put(2, new RAAnnotation(3, DistributionType.NA));
		// select cardinality 17 (p_size=50 and p_type=36)
		constraints.put(10, new RAAnnotation(17, DistributionType.NA));
		// join cardinality 7 (r_regionkey = n_regionkey)
		constraints.put(4, new RAAnnotation(7, DistributionType.UNIFORM));
		// join cardinality 20 (s_suppkey = ps_suppkey)
		constraints.put(8, new RAAnnotation(20, DistributionType.UNIFORM));
		// the cardinality of the final sql must be 20
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY6, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		// planner.init(schema, queries);
		// planner.printQuery(QUERY6);
	}

//	@Test
	public void test7_TPCH_4joins_DP() {
		List<String> schema = Arrays.asList(TABLE_REGION, TABLE_NATION, TABLE_SUPPLIER, TABLE_PART, TABLE_PARTSUPP);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// part cardinality (size=40)
		constraints.put(1, new RAAnnotation(40, DistributionType.NA));
		// partsupplier cardinality (size=50)
		constraints.put(2, new RAAnnotation(50, DistributionType.NA));
		// supplier cardinality (size=30)
		constraints.put(4, new RAAnnotation(30, DistributionType.NA));
		// nation cardinality (size=15)
		constraints.put(6, new RAAnnotation(15, DistributionType.NA));
		// region cardinality (size=5)
		constraints.put(8, new RAAnnotation(5, DistributionType.NA));
		// // select cardinality 3 (r_code=1)
		// constraints.put(2, new RAAnnotation(3, DistributionType.NA));
		// // select cardinality 17 (p_size=50 and p_type=36)
		// constraints.put(10, new RAAnnotation(17, DistributionType.NA));
		// join cardinality 7 (r_regionkey = n_regionkey)
		constraints.put(9, new RAAnnotation(7, DistributionType.UNIFORM));
		// join cardinality 20 (s_suppkey = ps_suppkey)
		constraints.put(5, new RAAnnotation(20, DistributionType.UNIFORM));
		// the cardinality of the final sql must be 20
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY8, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		 planner.init(schema, queries);
//		planner.printQuery(QUERY8);
	}
	
	@Test
	public void test2Queries() {
		List<String> schema = Arrays.asList(
				"CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80) unique, age int)",
				"create table Orders (o_id int not null primary key, o_date date, o_cid int, FOREIGN KEY (o_cid) REFERENCES customer(c_id))");
		Map<Integer, RAAnnotation> constraintsQuery1 = new HashMap<>();
		//customer cardinality 4
		constraintsQuery1.put(1, new RAAnnotation(4, DistributionType.NA));
		//select cardinality 2
		constraintsQuery1.put(2, new RAAnnotation(2, DistributionType.NA));
		Map<Integer, RAAnnotation> constraintsQuery2 = new HashMap<>();
		//customer cardinality 4
		constraintsQuery2.put(1, new RAAnnotation(4, DistributionType.NA));
		//select cardinality 2
		constraintsQuery2.put(2, new RAAnnotation(2, DistributionType.NA));
		List<RAQuery> queries = Arrays.asList(new RAQuery(QUERY1, constraintsQuery1), new RAQuery(QUERY1_2, constraintsQuery1));
		ExecutionPlanner planner = new ExecutionPlanner();
//		planner.printQuery(QUERY1);
//		planner.printQuery(QUERY3);
		planner.init(schema, queries);
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
