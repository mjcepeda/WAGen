package edu.rit.wagen.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.rit.wagen.dto.RAAnnotation;
import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.dto.RAAnnotation.DistributionType;

public class SchemaTest {
	// SCHEMA DEFINITION
	public static final String TABLE_REGION = "create table region (r_regionkey int primary key, r_code int, r_comment varchar(100));\n";

	public static final String TABLE_PART = "create table part (p_partkey int primary key, p_name varchar(50), p_brand varchar(50), "
			+ "p_type int, p_size int, p_container int, p_retailprice int, p_comment varchar(100));\n";

	public static final String TABLE_NATION = "create table nation (n_nationkey int primary key, n_name varchar(50), n_code int, n_regionkey int, "
			+ "n_comment varchar(100), FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey));\n";

	public static final String TABLE_SUPPLIER = "create table supplier (s_suppkey int primary key, s_name varchar(50), s_nationkey int, "
			+ "s_address varchar(200), s_phone int, s_acctbal int, s_comment varchar(100), FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey));\n";

	public static final String TABLE_CUSTOMER = "create table customer (c_custkey int primary key, c_name varchar(50), c_address varchar(200), c_nationkey int, "
			+ "c_phone varchar(50), c_acctbal int, c_mktsegment int, c_comment varchar(100), foreign key(c_nationkey) references nation(n_nationkey));";

	public static final String TABLE_ORDERS = "create table orders (o_orderkey int primary key, o_custkey int, o_orderstatus int, o_totalprice int, "
			+ "o_orderdate int, o_orderpriority varchar(50), o_clerk varchar(50), o_shippriority int, o_comment varchar(100), "
			+ "foreign key (o_custkey) references customer(c_custkey));";

	public static final String TABLE_LINEITEM = "create table lineitem (l_lineitemkey int primary key, l_orderkey int, l_partkey int, l_suppkey int, "
			+ " l_linenumber int, l_quantity int, l_extendedprice int, l_discount int, l_tax int, l_returnflag int, l_linestatus int, l_shipdate int, "
			+ " l_commitdate int, l_receiptdate int, l_shipinstruct int, l_shipmode varchar(50), l_comment varchar(100), "
			+ "foreign key (l_orderkey) references orders(o_orderkey), "
			+ "foreign key (l_partkey) references part(p_partkey), foreign key (l_suppkey) references supplier(s_suppkey));";

	// QUERIES DEFINITION
	public static final String TPC_H_Q1 = "(" + "(" + "(" + "(" + "(" + "(" + "(" + "region "
			+ "\\join_{r_regionkey = n_regionkey} nation" + ") " + "\\join_{n_nationkey= c_nationkey} customer" + ") "
			+ "\\join_{c_custkey = o_custkey} orders" + ") " + "\\join_{o_orderkey = l_orderkey} "
			+ "(\\select_{l_shipdate <= 50} lineitem)" + ") " + "\\join_{l_partkey = p_partkey} part" + ") "
			+ "\\join_{l_suppkey = s_suppkey} supplier" + ") " + "\\join_{s_nationkey = n_nationkey} nation" + ");\n";

	public static final String SQL_TPC_H_Q1 = "select count(*) from	<SCHEMA_NAME>.lineitem where l_shipdate <= 50";

	public static final String TPC_H_Q3 = "(" + "(" + "(" + "(" + "(" + "(" + "(" + "region "
			+ "\\join_{r_regionkey = n_regionkey} nation" + ") "
			+ "\\join_{n_nationkey= c_nationkey} (\\select_{c_mktsegment=2} customer)" + ") "
			+ "\\join_{c_custkey = o_custkey} " + "(\\select_{o_orderdate <5000} orders)" + ") "
			+ "\\join_{o_orderkey = l_orderkey} " + "(\\select_{l_shipdate < 560} lineitem)" + ") "
			+ "\\join_{l_partkey = p_partkey} part" + ") " + "\\join_{l_suppkey = s_suppkey} supplier" + ") "
			+ "\\join_{s_nationkey = n_nationkey} nation" + ");\n";

	public static final String SQL_TPC_H_Q3 = "select count(*) from" + " <SCHEMA_NAME>.customer,"
			+ " <SCHEMA_NAME>.orders," + " <SCHEMA_NAME>.lineitem" + "	where" + " c_mktsegment = '2'"
			+ " and c_custkey = o_custkey" + " and l_orderkey = o_orderkey" + " and o_orderdate < 5000"
			+ " and l_shipdate < 560";

	public static final String TPC_H_Q5 = "(" + "(" + "(" + "(" + "(" + "(" + "(" + "(\\select_{r_code=1} region) "
			+ "\\join_{r_regionkey = n_regionkey} nation" + ") " + "\\join_{n_nationkey= c_nationkey} customer" + ") "
			+ "\\join_{c_custkey = o_custkey} " + "(\\select_{o_orderdate<4500} orders)" + ") "
			+ "\\join_{o_orderkey = l_orderkey} lineitem" + ") " + "\\join_{l_partkey = p_partkey} part)"
			+ "\\join_{l_suppkey = s_suppkey} supplier" + ") " + "\\join_{s_nationkey = n_nationkey} nation" + ");\n";

	// select count(*) from <SCHEMA_NAME>.supplier, <SCHEMA_NAME>.lineitem,
	// <SCHEMA_NAME>.orders, <SCHEMA_NAME>.customer, <SCHEMA_NAME>.nation,
	// <SCHEMA_NAME>.region where s_suppkey = l_suppkey and l_orderkey =
	// o_orderkey and o_custkey
	// = c_custkey and c_nationkey = n_nationkey and n_regionkey = r_regionkey
	// and r_code = 1 and o_orderdate<4500
	public static final String SQL_TPC_H_Q5 = "select count(*) from" + " <SCHEMA_NAME>.supplier,"
			+ " <SCHEMA_NAME>.lineitem," + " <SCHEMA_NAME>.orders," + "	<SCHEMA_NAME>.customer,"
			+ " <SCHEMA_NAME>.nation," + "	<SCHEMA_NAME>.region" + " where" + " s_suppkey = l_suppkey"
			+ "	and l_orderkey = o_orderkey" + " and o_custkey = c_custkey" + "	and c_nationkey = n_nationkey"
			+ "	and n_regionkey = r_regionkey" + " and r_code = 1" + "	and o_orderdate<4500";

	public static final String TPC_H_Q6 = "(" + "(" + "(" + "(" + "(" + "(" + "(" + "region "
			+ "\\join_{r_regionkey = n_regionkey} nation" + ") " + "\\join_{n_nationkey= c_nationkey} customer" + ") "
			+ "\\join_{c_custkey = o_custkey} orders" + ") " + "\\join_{o_orderkey = l_orderkey} "
			+ "(\\select_{l_discount <=50 and l_quantity < 600} lineitem)" + ") "
			+ "\\join_{l_partkey = p_partkey} part" + ") " + "\\join_{l_suppkey = s_suppkey} supplier" + ") "
			+ "\\join_{s_nationkey = n_nationkey} nation" + ");\n";

	public static final String SQL_TPC_H_Q6 = "select count(*) from	<SCHEMA_NAME>.lineitem where l_quantity < 600 and l_discount <=50";

	public static final String TPC_H_Q7 = "(" + "(" + "(" + "(" + "(" + "(" + "("
			+ "region \\join_{r_regionkey = n_regionkey} (\\select_{n_name=3} nation)" + ") "
			+ "\\join_{n_nationkey= c_nationkey} customer" + ") " + "\\join_{c_custkey = o_custkey} orders)"
			+ "\\join_{o_orderkey = l_orderkey} (\\select_{l_shipdate <= 50} lineitem)" + ") "
			+ "\\join_{l_partkey = p_partkey} part)" + "\\join_{l_suppkey = s_suppkey} supplier" + ") "
			+ "\\join_{s_nationkey = n_nationkey} (\\select_{n_name=4} nation)" + ");\n";

	public static final String SQL_TPC_H_Q7 = "select count(*) from <SCHEMA_NAME>.supplier, <SCHEMA_NAME>.lineitem, <SCHEMA_NAME>.orders, <SCHEMA_NAME>.customer, <SCHEMA_NAME>.nation n1, <SCHEMA_NAME>.nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and n1.n_name = 3 and n2.n_name = 4 and l_shipdate < 1995";

	public static final String TPC_H_Q8 = "(" + "(" + "(" + "(" + "(" + "(" + "(" + "(\\select_{r_code=1} region) "
			+ "\\join_{r_regionkey = n_regionkey} nation" + ") " + "\\join_{n_nationkey= c_nationkey} customer" + ") "
			+ "\\join_{c_custkey = o_custkey} " + "(\\select_{o_orderdate<500} orders)" + ") "
			+ "\\join_{o_orderkey = l_orderkey} lineitem" + ") " + "\\join_{l_partkey = p_partkey} "
			+ "(\\select_{p_type = 6} part)" + ") " + "\\join_{l_suppkey = s_suppkey} supplier" + ") "
			+ "\\join_{s_nationkey = n_nationkey} nation" + ");\n";

	public static final String SQL_TPC_H_Q8 = "select count(*) from" + " <SCHEMA_NAME>.part,"
			+ "	<SCHEMA_NAME>.supplier," + " <SCHEMA_NAME>.lineitem," + " <SCHEMA_NAME>.orders,"
			+ "	<SCHEMA_NAME>.customer," + " <SCHEMA_NAME>.nation n1," + " <SCHEMA_NAME>.nation n2,"
			+ "	<SCHEMA_NAME>.region" + " where" + "	p_partkey = l_partkey" + " and s_suppkey = l_suppkey"
			+ "	and l_orderkey = o_orderkey" + " and o_custkey = c_custkey" + "	and c_nationkey = n1.n_nationkey"
			+ "	and n1.n_regionkey = r_regionkey" + " and r_code = 1" + " and s_nationkey = n2.n_nationkey"
			+ "	and o_orderdate<500" + " and p_type = 6";

	public static final String TPC_H_Q9 = "(" + "(" + "(" + "(" + "(" + "(" + "("
			+ "region \\join_{r_regionkey = n_regionkey} nation" + ") " + "\\join_{n_nationkey= c_nationkey} customer"
			+ ") " + "\\join_{c_custkey = o_custkey} orders)" + "\\join_{o_orderkey = l_orderkey} lineitem" + ") "
			+ "\\join_{l_partkey = p_partkey} " + "(\\select_{p_name = 10} part)" + ") "
			+ "\\join_{l_suppkey = s_suppkey} supplier" + ") " + "\\join_{s_nationkey = n_nationkey} nation" + ");\n";

	public static final String SQL_TPC_H_Q9 = "select count(*) from " + "<SCHEMA_NAME>.part,"
			+ "<SCHEMA_NAME>.supplier," + "<SCHEMA_NAME>.lineitem," + "<SCHEMA_NAME>.partsupp,"
			+ "<SCHEMA_NAME>.orders," + "<SCHEMA_NAME>.nation" + "where" + "s_suppkey = l_suppkey"
			+ "and ps_suppkey = l_suppkey" + "and ps_partkey = l_partkey" + "and p_partkey = l_partkey"
			+ "and o_orderkey = l_orderkey" + "and s_nationkey = n_nationkey" + "and p_name = 10";

	public static final List<String> schema = Arrays.asList(TABLE_REGION, TABLE_PART, TABLE_NATION, TABLE_SUPPLIER,
			TABLE_CUSTOMER, TABLE_ORDERS, TABLE_LINEITEM);

	public static final RAQuery getQuery8() {
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// R1 selection region table - 1
		constraints.put(2, new RAAnnotation(1, DistributionType.NA));
		// nation table size - 25
		constraints.put(3, new RAAnnotation(15, DistributionType.NA));
		// R2 join region, nation - 5
		constraints.put(4, new RAAnnotation(5, DistributionType.NA));
		// customer table size
		constraints.put(5, new RAAnnotation(25, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(7, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(50, DistributionType.NA));
		// R4 selection orders
		constraints.put(8, new RAAnnotation(20, DistributionType.NA));
		// join customer orders
		constraints.put(9, new RAAnnotation(11, DistributionType.NA));
		// lineitem
		constraints.put(10, new RAAnnotation(70, DistributionType.NA));
		// join orders lineitem
		constraints.put(11, new RAAnnotation(15, DistributionType.NA));
		// part
		constraints.put(12, new RAAnnotation(25, DistributionType.NA));
		// selection part
		constraints.put(13, new RAAnnotation(5, DistributionType.NA));
		constraints.put(14, new RAAnnotation(10, DistributionType.NA));
		// supplier
		constraints.put(15, new RAAnnotation(25, DistributionType.NA));
		constraints.put(16, new RAAnnotation(10, DistributionType.NA));
		constraints.put(17, new RAAnnotation(15, DistributionType.NA));
		constraints.put(18, new RAAnnotation(10, DistributionType.NA));
		return new RAQuery(SchemaTest.TPC_H_Q8, SQL_TPC_H_Q8, 10, constraints);
	}

	public static final RAQuery getQuery3() {
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 25
		constraints.put(2, new RAAnnotation(15, DistributionType.NA));
		// R2 join region, nation - 5
		constraints.put(3, new RAAnnotation(15, DistributionType.NA));
		// customer table size
		constraints.put(4, new RAAnnotation(25, DistributionType.NA));
		// selection customer
		constraints.put(5, new RAAnnotation(12, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(12, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(50, DistributionType.NA));
		// R4 selection orders
		constraints.put(8, new RAAnnotation(20, DistributionType.NA));
		// join customer orders
		constraints.put(9, new RAAnnotation(17, DistributionType.NA));
		// lineitem
		constraints.put(10, new RAAnnotation(70, DistributionType.NA));
		// selection lineitem
		constraints.put(11, new RAAnnotation(15, DistributionType.NA));
		// join lineitem orders
		constraints.put(12, new RAAnnotation(15, DistributionType.NA));
		// part table size
		constraints.put(13, new RAAnnotation(25, DistributionType.NA));
		constraints.put(14, new RAAnnotation(15, DistributionType.NA));
		// supplier table size
		constraints.put(15, new RAAnnotation(25, DistributionType.NA));
		constraints.put(16, new RAAnnotation(15, DistributionType.NA));
		// nation table size
		constraints.put(17, new RAAnnotation(15, DistributionType.NA));
		constraints.put(18, new RAAnnotation(15, DistributionType.NA));
		// the cardinality of the final sql must be 15 (join lineitem orders)
		return new RAQuery(SchemaTest.TPC_H_Q3, SQL_TPC_H_Q3, 15, constraints);
	}

	public static final RAQuery getQuery1() {
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 15
		constraints.put(2, new RAAnnotation(15, DistributionType.NA));
		// customer table size
		constraints.put(4, new RAAnnotation(25, DistributionType.NA));
		// join customer nation
		constraints.put(5, new RAAnnotation(25, DistributionType.NA));
		// orders
		constraints.put(6, new RAAnnotation(50, DistributionType.NA));
		// join orders customer
		constraints.put(7, new RAAnnotation(50, DistributionType.NA));
		// lineitem
		constraints.put(8, new RAAnnotation(70, DistributionType.NA));
		// selection lineitem
		constraints.put(9, new RAAnnotation(15, DistributionType.NA));
		// join orders lineitem
		constraints.put(10, new RAAnnotation(15, DistributionType.NA));
		// part table
		constraints.put(11, new RAAnnotation(25, DistributionType.NA));
		// join part lineitem
		constraints.put(12, new RAAnnotation(15, DistributionType.NA));
		// supplier table size
		constraints.put(13, new RAAnnotation(25, DistributionType.NA));
		// join lineitem supplier
		constraints.put(14, new RAAnnotation(15, DistributionType.NA));
		// nation
		constraints.put(15, new RAAnnotation(15, DistributionType.NA));
		// join nation supplier
		constraints.put(16, new RAAnnotation(15, DistributionType.NA));

		// the cardinality of the final sql must be 15 (selection lineitem
		// cardinality)
		return new RAQuery(SchemaTest.TPC_H_Q1, SQL_TPC_H_Q1, 15, constraints);
	}

	public static final RAQuery getQuery5() {
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// R1 selection region table - 1
		constraints.put(2, new RAAnnotation(1, DistributionType.NA));
		// nation table size - 25
		constraints.put(3, new RAAnnotation(15, DistributionType.NA));
		// R2 join region, nation - 5
		constraints.put(4, new RAAnnotation(5, DistributionType.NA));
		// customer table size
		constraints.put(5, new RAAnnotation(25, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(7, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(50, DistributionType.NA));
		// R4 selection orders
		constraints.put(8, new RAAnnotation(20, DistributionType.NA));
		// join customer orders
		constraints.put(9, new RAAnnotation(11, DistributionType.NA));
		// lineitem
		constraints.put(10, new RAAnnotation(70, DistributionType.NA));
		// join orders lineitem
		constraints.put(11, new RAAnnotation(15, DistributionType.NA));
		// part
		constraints.put(12, new RAAnnotation(25, DistributionType.NA));
		constraints.put(13, new RAAnnotation(15, DistributionType.NA));
		// supplier
		constraints.put(14, new RAAnnotation(25, DistributionType.NA));
		constraints.put(15, new RAAnnotation(15, DistributionType.NA));
		constraints.put(16, new RAAnnotation(15, DistributionType.NA));
		constraints.put(17, new RAAnnotation(15, DistributionType.NA));
		// cardinality must be the selection orders cardinality
		return new RAQuery(SchemaTest.TPC_H_Q5, SQL_TPC_H_Q5, 20, constraints);
	}

	public static final RAQuery getQuery6() {
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 25
		constraints.put(2, new RAAnnotation(15, DistributionType.NA));
		// customer table size
		constraints.put(4, new RAAnnotation(25, DistributionType.NA));
		// join customer nation
		constraints.put(5, new RAAnnotation(25, DistributionType.NA));
		// orders
		constraints.put(6, new RAAnnotation(50, DistributionType.NA));
		// join orders customer
		constraints.put(7, new RAAnnotation(50, DistributionType.NA));
		// lineitem
		constraints.put(8, new RAAnnotation(70, DistributionType.NA));
		// selection lineitem l_shipdate
		constraints.put(9, new RAAnnotation(35, DistributionType.NA));
		// join orders lineitem
		constraints.put(10, new RAAnnotation(29, DistributionType.NA));
		// part table
		constraints.put(11, new RAAnnotation(25, DistributionType.NA));
		// join part lineitem
		constraints.put(12, new RAAnnotation(29, DistributionType.NA));
		// supplier table size
		constraints.put(13, new RAAnnotation(25, DistributionType.NA));
		// join lineitem supplier
		constraints.put(14, new RAAnnotation(29, DistributionType.NA));
		// nation
		constraints.put(15, new RAAnnotation(15, DistributionType.NA));
		// join nation supplier
		constraints.put(16, new RAAnnotation(29, DistributionType.NA));

		// the cardinality of the final sql must be 70 (selection lineitem
		// cardinality)
		return new RAQuery(SchemaTest.TPC_H_Q6, SQL_TPC_H_Q6, 35, constraints);
	}

	public static final RAQuery getQuery7() {
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 25
		constraints.put(2, new RAAnnotation(25, DistributionType.NA));
		// R1 selection nation table - 1
		constraints.put(3, new RAAnnotation(1, DistributionType.NA));
		// R2 join region, nation - 5
		constraints.put(4, new RAAnnotation(1, DistributionType.NA));
		// customer table size
		constraints.put(5, new RAAnnotation(1500, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(6, new RAAnnotation(30, DistributionType.NA));
		// Orders
		constraints.put(7, new RAAnnotation(2000, DistributionType.NA));
		// join customer orders
		constraints.put(8, new RAAnnotation(40, DistributionType.NA));
		// lineitem
		constraints.put(9, new RAAnnotation(2000, DistributionType.NA));
		// lineitem selection
		constraints.put(10, new RAAnnotation(200, DistributionType.NA));
		// join orders lineitem
		constraints.put(11, new RAAnnotation(32, DistributionType.NA));
		// part
		constraints.put(12, new RAAnnotation(2000, DistributionType.NA));
		constraints.put(13, new RAAnnotation(19, DistributionType.NA));
		// supplier
		constraints.put(14, new RAAnnotation(100, DistributionType.NA));
		constraints.put(15, new RAAnnotation(19, DistributionType.NA));
		constraints.put(16, new RAAnnotation(25, DistributionType.NA));
		// nation selection
		constraints.put(17, new RAAnnotation(1, DistributionType.NA));
		constraints.put(18, new RAAnnotation(19, DistributionType.NA));
		return new RAQuery(SchemaTest.TPC_H_Q7, SQL_TPC_H_Q7, 29, constraints);
	}

	public static final RAQuery getQuery9() {
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		// region table size - 5
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		// nation table size - 25
		constraints.put(2, new RAAnnotation(25, DistributionType.NA));
		// R2 join region, nation - 5
		constraints.put(3, new RAAnnotation(25, DistributionType.NA));
		// customer table size
		constraints.put(4, new RAAnnotation(1500, DistributionType.NA));
		// R3 join customer, R2
		constraints.put(5, new RAAnnotation(1500, DistributionType.NA));
		// Orders
		constraints.put(6, new RAAnnotation(2000, DistributionType.NA));
		// order customer
		constraints.put(7, new RAAnnotation(2000, DistributionType.NA));
		// join lineitem
		constraints.put(8, new RAAnnotation(2000, DistributionType.NA));
		// lineitem order
		constraints.put(9, new RAAnnotation(2000, DistributionType.NA));
		// part
		constraints.put(10, new RAAnnotation(2000, DistributionType.NA));
		// selection part
		constraints.put(11, new RAAnnotation(1, DistributionType.NA));
		constraints.put(12, new RAAnnotation(8, DistributionType.NA));
		// supplier
		constraints.put(13, new RAAnnotation(100, DistributionType.NA));
		constraints.put(14, new RAAnnotation(8, DistributionType.NA));
		constraints.put(15, new RAAnnotation(25, DistributionType.NA));
		constraints.put(16, new RAAnnotation(8, DistributionType.NA));
		return new RAQuery(SchemaTest.TPC_H_Q9, SQL_TPC_H_Q9, 8, constraints);
	}
}
