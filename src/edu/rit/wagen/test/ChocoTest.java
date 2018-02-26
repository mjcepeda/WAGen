package edu.rit.wagen.test;

import org.chocosolver.solver.Model;
import org.chocosolver.solver.Solution;
import org.chocosolver.solver.constraints.real.IbexHandler;
import org.chocosolver.solver.variables.BoolVar;
import org.chocosolver.solver.variables.IntVar;
import org.junit.Test;

public class ChocoTest {

	@Test
	public void test() {
		Model m = new Model();
		
		System.out.println("I end with a number 4".matches("[a-z A-Z]+\\d"));
		
		System.out.println("4".matches("[a-z A-Z]+\\d"));
		
		//integer values
		IntVar v = m.intVar("salary", 0,100000);
		IntVar tax = m.intVar("tax", 0,100000);
		IntVar c = m.intVar("salary_other_department", 0, 1000000);
		IntVar d = m.intVar(80000);
		//boolean values
		BoolVar married = m.boolVar("married");
		BoolVar bc = m.boolVar(false);
		//testing arithmetic operation with integer attributes
		m.arithm(v, ">", c).post();
		m.arithm(c, ">=", d).post();
		m.arithm(v, "+", tax, "=", 120000).post();
		//testing boolean expressions
		m.addClausesBoolNot(married, bc);
		//testing arithmetic operation with real attributes
		IbexHandler hanlder =m.getIbexHandler();
//		RealPropagator rp = new RealPropagator(functions, vars)
//		hanlder.
		Solution solution = m.getSolver().findSolution();
		if (solution != null) {
			System.out.println(v.getValue());
			System.out.println(tax.getValue());
			System.out.println(solution.toString());
		}
	}

}
