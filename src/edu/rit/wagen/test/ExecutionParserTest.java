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

public class ExecutionParserTest {

	@Test
	public void test() {
		List<String> schema = Arrays.asList("CREATE TABLE CUSTOMER (c_id int NOT NULL PRIMARY KEY, c_acctbal varchar(50), name varchar(80))");
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		constraints.put(1, new RAAnnotation(50, DistributionType.NA));
		constraints.put(2, new RAAnnotation(3, DistributionType.NA));
		List<RAQuery> queries = Arrays.asList(new RAQuery("\\select_{name='Amy'} customer;\n", constraints));
		ExecutionPlanner planner = new ExecutionPlanner();
		planner.init(schema, queries);
//		planner.printQuery("\\select_{name='Amy'} customer;\n");
	}

}
