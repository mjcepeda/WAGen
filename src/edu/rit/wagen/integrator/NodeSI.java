package edu.rit.wagen.integrator;

public class NodeSI {

	public static enum NodeType {
		INTERNAL, LEAF
	}

	private NodeSI leftNode;
	private NodeType type;
	private String rightSDB;
	private String leftSDB;
	
	public NodeSI(String left, String right) {
		this.type = NodeType.LEAF;
		leftSDB = left;
		rightSDB = right;
	}
	
	public NodeSI(NodeSI left, String right) {
		this.type = NodeType.INTERNAL;
		leftNode = left;
		rightSDB = right;
	}

	/**
	 * @return the leftNode
	 */
	public NodeSI getLeftNode() {
		return leftNode;
	}

	/**
	 * @return the type
	 */
	public NodeType getType() {
		return type;
	}

	/**
	 * @return the rightSDB
	 */
	public String getRightSDB() {
		return rightSDB;
	}

	/**
	 * @return the leftSDB
	 */
	public String getLeftSDB() {
		return leftSDB;
	}
}
