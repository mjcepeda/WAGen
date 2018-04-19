package edu.rit.wagen.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import edu.rit.wagen.dto.Predicate;
import edu.rit.wagen.utils.ConstraintSolver;

/**
 * The Class GraphUtils.
 * @author Maria Cepeda
 */
public class GraphUtils {

	/**
	 * Generate constrained bipartite graph 2.
	 *
	 * @param listVertices0 the list vertices 0
	 * @param listVertices1 the list vertices 1
	 * @return the graph
	 * @throws Exception the exception
	 */
	public static Graph<GraphNode, DefaultEdge/* List<Predicate> */> generateConstrainedBipartiteGraph(
			List<GraphNode> listVertices0, List<GraphNode> listVertices1) throws Exception {
		Graph<GraphNode, DefaultEdge/* List<Predicate> */> graph = new SimpleGraph<>(DefaultEdge.class);
		Map<String, Boolean> cacheMap = new HashMap<>();
		// add vertices from the right
		for (GraphNode v1 : listVertices1) {
			graph.addVertex(v1);
		}

		for (GraphNode v0 : listVertices0) {
			// System.out.println(new Date() + " inserting node " +
			// v0.predicates);
			boolean nodeConnected = Boolean.FALSE;
			// add vertex to the left set
			graph.addVertex(v0);
			// for (String key: mapPatternNodes.keySet()) {
			for (GraphNode v1 : listVertices1) {
				Stack<Predicate> stack = new Stack<>();
				// push predicates from the right vertex into the stack
				stack.addAll(v1.predicates);
				List<Predicate> predicatesUpdated = new ArrayList<>();
				// replace symbols in the predicates from the right with the
				// symbols from the predicates to the left
				// we are trying to integrate these two tuples together, the
				// symbols must be the same
				Predicate pAux = null;
				while (!stack.isEmpty()) {
					Optional<Predicate> match = v0.predicates.stream()
							.filter(p -> p.attribute.equals(stack.peek().attribute)).findFirst();
					if (match.isPresent()) {
						pAux = new Predicate(match.get().symbol, stack.peek().op, stack.peek().condition);
						predicatesUpdated.add(pAux);
						stack.pop();
					} else {
						String symbol = stack.peek().symbol.replaceAll("[0-9]", "");
						String sequential = stack.peek().symbol.replaceAll(symbol, "");
						pAux = new Predicate(symbol + sequential, stack.peek().op, stack.peek().condition);
						predicatesUpdated.add(pAux);
						stack.pop();
					}
				}
				// union all the predicates from both vertices
				List<Predicate> allPredicates = new ArrayList<>();
				allPredicates.addAll(v0.predicates);
				allPredicates.addAll(predicatesUpdated);
				// formula using the attributes name, not the symbol (pattern)
				String formula = allPredicates.stream().map(p -> p.getPattern()).collect(Collectors.joining(" and "));
				try {
					boolean isSat = Boolean.FALSE;
					if (cacheMap.containsKey(formula)) {
						isSat = cacheMap.get(formula);
					} else {
						// System.out.println(new Date() + " calling constraint
						// solver, formula " + formula);
						// checking if predicates are satisfiable
						isSat = ConstraintSolver.isSAT(allPredicates);
						// insert into the map
						cacheMap.put(formula, isSat);
					}
					if (isSat) {
						// adding edge to the graph
						// System.out.println("Adding edge" + allPredicates);
						graph.addEdge(v0, v1/* , allPredicates */);
						if (!nodeConnected) {
							nodeConnected = Boolean.TRUE;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					throw (e);
				}
			}
			if (!nodeConnected) {
				throw new Exception("Error creating constrained bipartite graph: Unconnected node");
			}
		}
		return graph;
	}

	/**
	 * Find maximum satisfiable matching 2.
	 *
	 * @param graph the graph
	 * @param vertices0 the vertices 0
	 * @param vertices1 the vertices 1
	 * @return the list
	 */
	public static List<Predicate> findMaximumSatisfiableMatching(
			Graph<GraphNode, DefaultEdge/* List<Predicate> */> graph, List<GraphNode> vertices0,
			List<GraphNode> vertices1) {
		List<Predicate> result = new ArrayList<>();
		// sort the left list of vertices by the number of neighbors
		vertices0.sort((v0, v1) -> compareByVertexDegree(v0, v1, graph));
		// finding a maximum satisfiable matching
		Map<GraphNode, DefaultEdge/* List<Predicate> */> partialSolution = new HashMap<>();
		result = findMatching(graph, vertices0, vertices1, partialSolution, 0, result);
		return result;
	}

	/**
	 * Find matching.
	 *
	 * @param graph the graph
	 * @param vertices0 the vertices 0
	 * @param vertices1 the vertices 1
	 * @param partialSolution the partial solution
	 * @param idxVertices0 the idx vertices 0
	 * @param solution the solution
	 * @return the list
	 */
	private static List<Predicate> findMatching(
			Graph<GraphNode, DefaultEdge/* List<Predicate> */> graph, List<GraphNode> vertices0,
			List<GraphNode> vertices1,
			Map<GraphNode, DefaultEdge/* List<Predicate> */> partialSolution, int idxVertices0,
			List<Predicate> solution) {
		if (solution != null && solution.isEmpty()) {
			if (idxVertices0 == vertices0.size()) {
				// solution found, check it
				// List<Predicate> predicates =
				// partialSolution.entrySet().stream().flatMap(e ->
				// e.getValue().stream())
				// .collect(Collectors.toList());
				List<Predicate> predicates = new ArrayList<>();
				for (DefaultEdge e : partialSolution.values()) {
					// left node
					GraphNode u = graph.getEdgeSource(e);
					// right node
					GraphNode v = graph.getEdgeTarget(e);
					// insert predicates from the left node into the list of
					// predicates
					predicates.addAll(u.predicates);
					Stack<Predicate> stack = new Stack<>();
					// push predicates from the right vertex into the stack
					stack.addAll(v.predicates);
					List<Predicate> predicatesUpdated = new ArrayList<>();
					// replace symbols in the predicates from the right with the
					// symbols from the predicates to the left
					// we are trying to integrate these two tuples together, the
					// symbols must be the same
					Predicate pAux = null;
					while (!stack.isEmpty()) {
						Optional<Predicate> match = u.predicates.stream()
								.filter(p -> p.attribute.equals(stack.peek().attribute)).findFirst();
						if (match.isPresent()) {
							pAux = new Predicate(match.get().symbol, stack.peek().op, stack.peek().condition);
							predicatesUpdated.add(pAux);
							stack.pop();
						} else {
							String symbol = stack.peek().symbol.replaceAll("[0-9]", "");
							String sequential = stack.peek().symbol.replaceAll(symbol, "");
							pAux = new Predicate(symbol + sequential, stack.peek().op, stack.peek().condition);
							predicatesUpdated.add(pAux);
							stack.pop();
						}
					}
					// add predicates from the right node with the symbol update
					predicates.addAll(predicatesUpdated);
				}
				System.out.println(predicates);
				try {
					boolean isSAT = ConstraintSolver.isSAT(predicates);
					if (isSAT) {
						// System.out.println(new Date() + " Solution found");
						solution = predicates;
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			} else {
				// System.out.println("Index " + idxVertices0);
				GraphNode v0 = vertices0.get(idxVertices0);
				// for every neighbor of the vertex in the right side
				for (DefaultEdge/* List<Predicate> */ edge : graph.edgesOf(v0)) {
					GraphNode v1 = graph.getEdgeTarget(edge);
					// if the neighbor is free and there is no solution
					if (isFree(v1, partialSolution) && solution.isEmpty()) {
						// mark the vertex and include it into the partial
						// solution
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
	 * @param vertex the vertex
	 * @param partialSolution the partial solution
	 * @return true, if is free
	 */
	private static boolean isFree(GraphNode vertex,
			Map<GraphNode, DefaultEdge/* List<Predicate> */> partialSolution) {
		return partialSolution.get(vertex) == null;
	}

	/**
	 * Compare by vertex degree.
	 *
	 * @param v0 the v 0
	 * @param v1 the v 1
	 * @param graph the graph
	 * @return the int
	 */
	private static int compareByVertexDegree(GraphNode v0, GraphNode v1,
			Graph<GraphNode, DefaultEdge/* List<Predicate> */> graph) {
		return Integer.compare(graph.degreeOf(v0), graph.degreeOf(v1));
	}
}
