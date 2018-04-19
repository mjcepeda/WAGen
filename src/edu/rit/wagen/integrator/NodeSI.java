package edu.rit.wagen.integrator;

import java.util.Arrays;

import edu.rit.wagen.dto.IntegratedSDB;
import edu.rit.wagen.dto.RAQuery;

/**
 * The Class NodeSI.
 * @author Maria Cepeda
 */
public class NodeSI {

	/**
	 * The Enum NodeType.
	 */
	public static enum NodeType {

		/** The internal. */
		INTERNAL,
		/** The leaf. */
		LEAF
	}

	/** The left node. */
	private NodeSI leftNode;

	/** The type. */
	private NodeType type;

	/** The right SDB. */
	private IntegratedSDB rightSDB;

	/** The left SDB. */
	private IntegratedSDB leftSDB;

	/**
	 * Instantiates a new node SI.
	 *
	 * @param left
	 *            the left
	 * @param right
	 *            the right
	 */
	public NodeSI(RAQuery left, RAQuery right) {
		this.type = NodeType.LEAF;
		leftSDB = new IntegratedSDB(left.getSdbName(), Arrays.asList(left));
		rightSDB = new IntegratedSDB(right.getSdbName(), Arrays.asList(right));
	}

	/**
	 * Instantiates a new node SI.
	 *
	 * @param left
	 *            the left
	 * @param right
	 *            the right
	 */
	public NodeSI(NodeSI left, RAQuery right) {
		this.type = NodeType.INTERNAL;
		leftNode = left;
		rightSDB = new IntegratedSDB(right.getSdbName(), Arrays.asList(right));
	}

	/**
	 * Gets the left node.
	 *
	 * @return the left node
	 */
	public NodeSI getLeftNode() {
		return leftNode;
	}

	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public NodeType getType() {
		return type;
	}

	/**
	 * Gets the right SDB.
	 *
	 * @return the right SDB
	 */
	public IntegratedSDB getRightSDB() {
		return rightSDB;
	}

	/**
	 * Gets the left SDB.
	 *
	 * @return the left SDB
	 */
	public IntegratedSDB getLeftSDB() {
		return leftSDB;
	}
}
