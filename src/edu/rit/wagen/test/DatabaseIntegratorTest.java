package edu.rit.wagen.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.rit.wagen.dto.RAQuery;
import edu.rit.wagen.planner.ExecutionPlanner;
/**
 * Experiment to evaluate the efficiency of the integrator module to reduce the number of synthetic databases
 * @author Maria Cepeda
 *
 */
public class DatabaseIntegratorTest {

	@Test
	public void test() {
		List<RAQuery> l = new ArrayList();
		l.add(SchemaTest.getQuery1());
		l.add(SchemaTest.getQuery3());
		l.add(SchemaTest.getQuery5());
		l.add(SchemaTest.getQuery6());
		l.add(SchemaTest.getQuery8());
//		l.add(SchemaTest.getQuery8());
//		l.add(SchemaTest.getQuery6());
//		l.add(SchemaTest.getQuery5());
//		l.add(SchemaTest.getQuery3());
//		l.add(SchemaTest.getQuery1());
		ExecutionPlanner planner = new ExecutionPlanner(SchemaTest.schema, 50, l);
		planner.init();
//		DatabaseIntegrator integrator = new DatabaseIntegrator();
//		integrator.executingPlan(siSequence);
	}
	
//	@Test
	public void testQuery() {
		List<RAQuery> l = new ArrayList();
		l.add(SchemaTest.getQuery3());
		ExecutionPlanner planner = new ExecutionPlanner(SchemaTest.schema, 50, l);
		planner.init();
//		planner.printQuery(SchemaTest.TPC_H_Q9);
//		DatabaseIntegrator integrator = new DatabaseIntegrator();
//		integrator.executingPlan(siSequence);
	}
}
