package org.apache.hadoop.types;

public class PhaseTime {

	private double minStartTime = Double.MAX_VALUE;
	private double maxStartTime = Double.MIN_VALUE;
	
	private double minFinishTime = Double.MAX_VALUE;
	private double maxFinishTime = Double.MIN_VALUE;
	
	public PhaseTime(double minStartTime, double maxStartTime, double minFinishTime, double maxFinishTime) {
		super();
		this.minStartTime = minStartTime;
		this.maxStartTime = maxStartTime;
		this.minFinishTime = minFinishTime;
		this.maxFinishTime = maxFinishTime;
	}
	
	public double getMinStartTime() {
		return minStartTime;
	}
	public void setMinStartTime(double minStartTime) {
		this.minStartTime = minStartTime;
	}
	public double getMaxStartTime() {
		return maxStartTime;
	}
	public void setMaxStartTime(double maxStartTime) {
		this.maxStartTime = maxStartTime;
	}
	public double getMinFinishTime() {
		return minFinishTime;
	}
	public void setMinFinishTime(double minFinishTime) {
		this.minFinishTime = minFinishTime;
	}
	public double getMaxFinishTime() {
		return maxFinishTime;
	}
	public void setMaxFinishTime(double maxFinishTime) {
		this.maxFinishTime = maxFinishTime;
	}
	
	public String toString(){
		return new String(minStartTime + ":" + maxStartTime + " - " + minFinishTime + ":" + maxFinishTime);
	}
	
}