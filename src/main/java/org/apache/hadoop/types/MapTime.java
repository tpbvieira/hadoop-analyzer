package org.apache.hadoop.types;

public class MapTime implements Comparable<MapTime>{

	private long startTime;
	private long finishTime;
	private long relativeStartTime;

	public MapTime(long startTime, long finishTime){
		this.startTime = startTime;
		this.finishTime = finishTime;
		relativeStartTime = 0;
	}

	public MapTime(long startTime, long finishTime, long relativeStartTime){
		this.startTime = startTime;
		this.finishTime = finishTime;
		this.relativeStartTime = relativeStartTime;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}

	public long getRelativeStartTime() {
		return relativeStartTime;
	}

	public void setRelativeStartTime(long relativeStartTime) {
		this.relativeStartTime = relativeStartTime;
	}

	@Override
	public int compareTo(MapTime compared) {

		if(this.getStartTime() == compared.getStartTime() && this.getFinishTime() == compared.getFinishTime())
			return 0;
		else if(this.getStartTime() > compared.getStartTime())
			return 1;
		else if(this.getStartTime() == compared.getStartTime() && this.getFinishTime() > compared.getFinishTime())
			return 1;
		else if(this.getStartTime() < compared.getStartTime())
			return -1;
		else if(this.getStartTime() == compared.getStartTime() && this.getFinishTime() < compared.getFinishTime())
			return 1;
		else
			throw new RuntimeException("Comparison Failed");
	}

}