package edu.rit.wagen.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;

import org.jgrapht.Graph;
import org.jgrapht.graph.SimpleGraph;

import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.utils.ConstraintSolver;
import edu.rit.wagen.utils.Utils;

/**
 * The Class GraphUtils.
 * 
 * @author Maria Cepeda
 */
public class GraphUtils {

	/**
	 * Generate constrained bipartite graph.
	 *
	 * @param listVertices0
	 *            the list vertices 0
	 * @param listVertices1
	 *            the list vertices 1
	 * @return the graph
	 * @throws Exception
	 *             the exception
	 */
	public static Graph<String, List<Predicate>> generateConstrainedBipartiteGraph(List<String> listVertices0,
			List<String> listVertices1) throws Exception {
		Graph<String, List<Predicate>> graph = new SimpleGraph<>(List.class);
		for (String v0 : listVertices0) {
			// add vertex to the left set
			graph.addVertex(v0);
			for (String v1 : listVertices1) {
				// add vertex to the right set
				graph.addVertex(v1);
				// get predicates from the vertex to the left
				List<Predicate> listPredicatesP0 = Utils.parsePredicate(v0);
				// get predicates from the vertex to the right
				List<Predicate> listPredicatesP1 = Utils.parsePredicate(v1);
				Stack<Predicate> stack = new Stack<>();
				// push predicates from the right vertex into the stack
				stack.addAll(listPredicatesP1);
				// replace symbols in the predicates from the right with the
				// symbols from the predicates to the left
				// we are trying to integrate these two tuples together, the
				// symbols must be the same
				while (!stack.isEmpty()) {
					Optional<Predicate> match = listPredicatesP0.stream()
							.filter(p -> p.attribute.equals(stack.peek().attribute)).findFirst();
					if (match.isPresent()) {
						// update symbol value
						stack.peek().symbol = match.get().symbol;
						stack.pop();
					}
				}
				// union all the predicates from both vertices
				List<Predicate> allPredicates = new ArrayList<>();
				allPredicates.addAll(listPredicatesP0);
				allPredicates.addAll(listPredicatesP1);
				try {
					// checking if predicates are satisfiable
					boolean isSat = ConstraintSolver.isSAT(allPredicates);
					if (isSat) {
						// adding edge to the graph
						graph.addEdge(v0, v1, allPredicates);
					}
				} catch (Exception e) {}
			}
		}
		return graph;
	}

	/**
	 * Find maximum satisfiable matching.
	 *
	 * @param graph
	 *            the graph
	 * @param vertices0
	 *            the vertices 0
	 * @param vertices1
	 *            the vertices 1
	 * @return the list
	 */
	public static List<Predicate> findMaximumSatisfiableMatching(Graph<String, List<Predicate>> graph,
			List<String> vertices0, List<String> vertices1) {
		List<Predicate> result = new ArrayList<>();
		// sort the left list of vertices by the number of neighbors
		vertices0.sort((v0, v1) -> compareByVertexDegree(v0, v1, graph));
		// finding a maximum satisfiable matching
		Map<String, List<Predicate>> partialSolution = new HashMap<>();
		result = findMatching(graph, vertices0, vertices1, partialSolution, 0, result);
		return result;
	}

	/**
	 * Find matching.
	 *
	 * @param graph
	 *            the graph
	 * @param vertices0
	 *            the vertices 0
	 * @param vertices1
	 *            the vertices 1
	 * @param partialSolution
	 *            the partial solution
	 * @param idxVertices0
	 *            the idx vertices 0
	 * @param solution
	 *            the solution
	 * @return the list
	 */
	private static List<Predicate> findMatching(Graph<String, List<Predicate>> graph, List<String> vertices0,
			List<String> vertices1, Map<String, List<Predicate>> partialSolution, int idxVertices0,
			List<Predicate> solution) {
		if (solution != null && solution.isEmpty()) {
			if (idxVertices0 == vertices0.size()) {
				// solution found, check it
				List<Predicate> predicates = partialSolution.entrySet().stream().flatMap(e -> e.getValue().stream())
						.collect(Collectors.toList());
				try {
					boolean isSAT = ConstraintSolver.isSAT(predicates);
					if (isSAT) {
						solution = predicates;
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} else {
				String v0 = vertices0.get(idxVertices0);
				// for every neighbor of the vertex in the right side
				for (List<Predicate> edge : graph.edgesOf(v0)) {
					String v1 = graph.getEdgeTarget(edge);
					//if the neighbor is free
					if (isFree(v1, partialSolution) && solution.isEmpty()) {
						// mark the vertex and include it into the partial solution
						partialSolution.put(v1, edge);
						// make recusion call
						solution = findMatching(graph, vertices0, vertices1, partialSolution, idxVertices0 + 1,
								solution);
						// delete solution
						partialSolution.remove(v1);
					}
				}
			}
		}
		return solution;
	}

	/**
	 * Checks if is free.
	 *
	 * @param vertex
	 *            the vertex
	 * @param partialSolution
	 *            the partial solution
	 * @return true, if is free
	 */
	private static boolean isFree(String vertex, Map<String, List<Predicate>> partialSolution) {
		return partialSolution.get(vertex) == null;
	}

	/**
	 * Compare by vertex degree.
	 *
	 * @param v0
	 *            the v 0
	 * @param v1
	 *            the v 1
	 * @param graph
	 *            the graph
	 * @return the int
	 */
	private static int compareByVertexDegree(String v0, String v1, Graph<String, List<Predicate>> graph) {
		return Integer.compare(graph.degreeOf(v0), graph.degreeOf(v1));
	}
}
