package edu.rit.wagen.dto;

/**
 * The Class RAAnnotation.
 * 
 * @author Maria Cepeda
 */
public class RAAnnotation {

	/**
	 * The Enum DistributionType.
	 */
	public static enum DistributionType {
		/** The na. */
		NA,
		/** The uniform. */
		UNIFORM,
		/** The zifps. */
		ZIFPS
	}

	/** The cardinality. */
	private int cardinality;

	/** The dist type. */
	private DistributionType distType;

	/**
	 * Instantiates a new RA annotation.
	 *
	 * @param c
	 *            the c
	 * @param dis
	 *            the dis
	 */
	public RAAnnotation(int c, DistributionType dis) {
		this.cardinality = c;
		this.distType = dis;
	}

	/**
	 * Gets the cardinality.
	 *
	 * @return the cardinality
	 */
	public int getCardinality() {
		return cardinality;
	}

	/**
	 * Gets the dist type.
	 *
	 * @return the dist type
	 */
	public DistributionType getDistType() {
		return distType;
	}
}
