package edu.rit.wagen.graph;

import java.util.List;

import edu.rit.wagen.dto.Predicate;

/**
 * The Class GraphNode.
 * @author Maria Cepeda
 */
public class GraphNode {

	/** The db. */
	public String db;
	
	/** The table. */
	public String table;
	
	/** The predicates. */
	public List<Predicate> predicates;

	/**
	 * Instantiates a new graph node.
	 *
	 * @param db the db
	 * @param table the table
	 * @param predicates the predicates
	 */
	public GraphNode(String db, String table, List<Predicate> predicates) {
		this.db = db;
		this.table = table;
		this.predicates = predicates;
	}
}
