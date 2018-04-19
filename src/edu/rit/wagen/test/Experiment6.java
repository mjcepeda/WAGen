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
/**
 * Testing the scalability of my backtracking algorithm
 * @author Maria Cepeda
 *
 */
public class Experiment6 {

	public static final String QUERY_1 = "\\select_{a <= 20} r;\n";

	public static final String QUERY_2 = "\\select_{a > 30} r;\n";

	public static final String TABLE_R = "create table r (a int, b int);\n";
  
	@Test
	public void test() {
		List<String> schema = Arrays.asList(TABLE_R);
		Map<Integer, RAAnnotation> constraints = new HashMap<>();
		//plug here the number of nodes in the graph
		//if you want to test with 1000 nodes, you have to divide that number by 2
		//500 nodes in the left side and 500 nodes in the right side of the bipartite graph
		// table size = 4
		constraints.put(1, new RAAnnotation(4600, DistributionType.NA));
		// selection cardinality constraint = 2
		constraints.put(2, new RAAnnotation(2000, DistributionType.NA));
		List<RAQuery> queries = Arrays.asList(
				new RAQuery(QUERY_1, "select count(*) from <SCHEMA_NAME>.r where a <=20", 2000, constraints),
				new RAQuery(QUERY_2, "select count(*) from <SCHEMA_NAME>.r where a >30", 2000, constraints));
		ExecutionPlanner planner = new ExecutionPlanner(schema, 0, queries);
		planner.init();
		// planner.printQuery(QUERY_1);
		// planner.printQuery(QUERY_2);
	}

}
