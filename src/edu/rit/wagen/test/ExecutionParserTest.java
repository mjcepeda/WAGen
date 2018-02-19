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

public class ExecutionParserTest {

	public static final String q1 = "\\select_{name='Amy'} customer;\n";
	public static final String q2 = "\\select_{name='Amy'} customer \\join_{c_id = o_cid} orders;\n";
	public static final String q4 = "customer \\join_{c_id = o_cid} orders;\n";
	//list of unsupported queries
	public static final String q3 = "\\select_{name='Amy'}(\\select_{name='Amy'} customer \\join_{c_id = o_cid} orders);\n";
	
	@Test
	public void test() {
		List<String> schema = Arrays.asList("CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80))", 
				"create table Orders (o_id int not null primary key, o_date date, o_cid int, FOREIGN KEY (o_cid) REFERENCES customer(c_id))");
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		constraints.put(1, new RAAnnotation(5, DistributionType.NA));
		constraints.put(2, new RAAnnotation(15, DistributionType.NA));
		constraints.put(4, new RAAnnotation(8, DistributionType.ZIFPS));
		List<RAQuery> queries = Arrays.asList(new RAQuery(q4, constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
//		planner.printQuery(q4);
	}
	
//	@Test
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
