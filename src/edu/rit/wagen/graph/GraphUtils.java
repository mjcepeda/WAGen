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

public class GraphUtils {

	// Creates a bipartite graph with the given numbers
	// of vertices and edges without isolated vertices
	public static Graph<String, List<Predicate>> generateConstrainedBipartiteGraph(List<String> listVertices0,
			List<String> listVertices1) throws Exception {
		List<String> connectedVertex = new ArrayList<>();
		Graph<String, List<Predicate>> graph = new SimpleGraph<>(List.class);
		for (String v0 : listVertices0) {
			graph.addVertex(v0);
			for (String v1 : listVertices1) {
				graph.addVertex(v1);
				List<Predicate> listPredicatesP0 = Utils.parsePredicate(v0);
				List<Predicate> listPredicatesP1 = Utils.parsePredicate(v1);
				Stack<Predicate> s = new Stack<>();
				s.addAll(listPredicatesP1);
				while (!s.isEmpty()) {
					Optional<Predicate> match = listPredicatesP0.stream()
							.filter(p -> p.attribute.equals(s.peek().attribute)).findFirst();
					if (match.isPresent()) {
						s.peek().symbol = match.get().symbol;
						s.pop();
					}
				}
				List<Predicate> allPredicates = new ArrayList<>();
				allPredicates.addAll(listPredicatesP0);
				allPredicates.addAll(listPredicatesP1);
				try {
					// TODO Solver has a feasible attribute
					ConstraintSolver.solvePredicates(allPredicates);
					// if there is no error, the formula is satifiable
					graph.addEdge(v0, v1, allPredicates);
					// connectedVertex.add(v0);
					// connectedVertex.add(v1);
				} catch (Exception e) {

				}
			}
			// TODO Controlar que no exista ningún vertice suelto
			// int totalVertices = listVertices0.size() + listVertices1.size();
			// if
			// (connectedVertex.stream().distinct().collect(Collectors.toList()).size()
			// < totalVertices) {
			// throw new Exception("Integration not possible");
			// }
		}
		return graph;
	}

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
				// for every neighbor of the vertex in the left side
				for (List<Predicate> edge : graph.edgesOf(v0)) {
					String v1 = graph.getEdgeTarget(edge);
					if (isFree(v1, partialSolution)) {
						// mark the vertex
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

	private static boolean isFree(String vertex, Map<String, List<Predicate>> partialSolution) {
		return partialSolution.get(vertex) == null;
	}

	private static int compareByVertexDegree(String v0, String v1, Graph<String, List<Predicate>> graph) {
		return Integer.compare(graph.degreeOf(v0), graph.degreeOf(v1));
	}

}
