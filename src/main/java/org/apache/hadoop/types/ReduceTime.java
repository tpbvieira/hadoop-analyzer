package org.apache.hadoop.types;

public class ReduceTime implements Comparable<ReduceTime>{

	private long startTime;	
	private long relativeStartTime;
	private long mapTime;
	private long shuffleTime;
	private long sortTime; 
	private long finishTime;


	public ReduceTime(long startTime, long shuffleTime, long sortTime, long finishTime){
		this.startTime = startTime;
		this.shuffleTime = shuffleTime;
		this.sortTime = sortTime;
		this.finishTime = finishTime;

		this.relativeStartTime = 0;
		this.mapTime = 0;
	}

	public ReduceTime(long startTime, long mapTime, long shuffleTime, long sortTime, long finishTime){
		this.startTime = startTime;
		this.mapTime = mapTime;
		this.shuffleTime = shuffleTime;
		this.sortTime = sortTime;
		this.finishTime = finishTime;

		this.relativeStartTime = 0;		
	}

	public ReduceTime(long startTime, long mapTime, long shuffleTime, long sortTime, long finishTime, long relativeStartTime){
		this.startTime = startTime;
		this.mapTime = mapTime;
		this.shuffleTime = shuffleTime;
		this.sortTime = sortTime;
		this.finishTime = finishTime;		
		this.relativeStartTime = relativeStartTime;		
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getRelativeStartTime() {
		return relativeStartTime;
	}

	public void setRelativeStartTime(long relativeStartTime) {
		this.relativeStartTime = relativeStartTime;
	}

	public long getMapTime() {
		return mapTime;
	}

	public void setMapTime(long mapTime) {
		this.mapTime = mapTime;
	}

	public long getShuffleTime() {
		return shuffleTime;
	}

	public void setShuffleTime(long shuffleTime) {
		this.shuffleTime = shuffleTime;
	}

	public long getSortTime() {
		return sortTime;
	}

	public void setSortTime(long sortTime) {
		this.sortTime = sortTime;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}

	@Override
	public int compareTo(ReduceTime compared) {

		if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() == compared.getMapTime()
				&& this.getShuffleTime() == compared.getShuffleTime()
				&& this.getSortTime() == compared.getSortTime()
				&& this.getFinishTime() == compared.getFinishTime())
			return 0;
		else if(this.getStartTime() > compared.getStartTime())
			return 1;
		else if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() > compared.getMapTime())
			return 1;
		else if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() == compared.getMapTime()
				&& this.getShuffleTime() > compared.getShuffleTime())
			return 1;
		else if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() == compared.getMapTime()
				&& this.getShuffleTime() == compared.getShuffleTime()
				&& this.getSortTime() > compared.getSortTime())
			return 1;
		else if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() == compared.getMapTime()
				&& this.getShuffleTime() == compared.getShuffleTime()
				&& this.getSortTime() == compared.getSortTime()
				&& this.getFinishTime() > compared.getFinishTime())
			return 1;
		else if(this.getStartTime() < compared.getStartTime())
			return -1;
		else if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() < compared.getMapTime())
			return -1;
		else if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() == compared.getMapTime()
				&& this.getShuffleTime() < compared.getShuffleTime())
			return -1;
		else if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() == compared.getMapTime()
				&& this.getShuffleTime() == compared.getShuffleTime()
				&& this.getSortTime() > compared.getSortTime())
			return -1;
		else if(this.getStartTime() == compared.getStartTime() 
				&& this.getMapTime() == compared.getMapTime()
				&& this.getShuffleTime() == compared.getShuffleTime()
				&& this.getSortTime() == compared.getSortTime()
				&& this.getFinishTime() > compared.getFinishTime())
			return -1;		
		else
			throw new RuntimeException("Comparison Failed");
	}

}