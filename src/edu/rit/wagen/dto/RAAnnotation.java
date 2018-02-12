package edu.rit.wagen.dto;

public class RAAnnotation {
	
	public static enum DistributionType {
		NA, UNIFORM, ZIFPS 
	}
	private int cardinality;
	private DistributionType distType;
	
	public RAAnnotation(int c, DistributionType dis) {
		this.cardinality = c;
		this.distType = dis;
	}

	/**
	 * @return the cardinality
	 */
	public int getCardinality() {
		return cardinality;
	}

	/**
	 * @return the distType
	 */
	public DistributionType getDistType() {
		return distType;
	}
}
