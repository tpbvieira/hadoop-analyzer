package org.apache.hadoop.types;

import java.util.Map;

public class GraphParameters {

	private int xMin;
	private int xMax;
	private int yMin;
	private int yMax;
	private int yMin2;
	private int yMax2;
	
	private Map<Integer,String> sources;
	
	private Map<Integer,String> labels;
	
	private Map<Integer,Integer> constants;
	
	public GraphParameters(int xMin, int xMax, int yMin, int yMax){
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;		
	}
	
	public GraphParameters(int xMin, int xMax, int yMin, int yMax, Map<Integer,Integer> constants){
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;		
		this.constants = constants;
	}
	
	public GraphParameters(int xMin, int xMax, int yMin, int yMax, int yMin2, int yMax2, Map<Integer,Integer> constants){
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;		
		this.yMin2 = yMin2;
		this.yMax2 = yMax2;		
		this.constants = constants;
	}
	
	public GraphParameters(int xMin, int xMax, int yMin, int yMax, Map<Integer,String> sources, Map<Integer,String> labels){
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;	
		this.sources = sources;
		this.labels = labels;
	}

	public GraphParameters(int xMin, int xMax, int yMin, int yMax, int yMin2, int yMax2){
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;		
		this.yMin2 = yMin2;
		this.yMax2 = yMax2;		
	}
	
	public GraphParameters(int xMin, int xMax, int yMin, int yMax, int yMin2, int yMax2, Map<Integer,String> sources, Map<Integer,String> labels){
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;		
		this.yMin2 = yMin2;
		this.yMax2 = yMax2;		
		this.sources = sources;
		this.labels = labels;
	}
	
	public int getxMin() {
		return xMin;
	}
	public void setxMin(int xMin) {
		this.xMin = xMin;
	}
	public int getxMax() {
		return xMax;
	}
	public void setxMax(int xMax) {
		this.xMax = xMax;
	}
	public int getyMin() {
		return yMin;
	}
	public void setyMin(int yMin) {
		this.yMin = yMin;
	}
	public int getyMax() {
		return yMax;
	}
	public void setyMax(int yMax) {
		this.yMax = yMax;
	}

	public int getyMin2() {
		return yMin2;
	}

	public void setyMin2(int yMin2) {
		this.yMin2 = yMin2;
	}

	public int getyMax2() {
		return yMax2;
	}

	public void setyMax2(int yMax2) {
		this.yMax2 = yMax2;
	}

	public Map<Integer,String> getSources() {
		return sources;
	}

	public void setSources(Map<Integer,String> sources) {
		this.sources = sources;
	}

	public Map<Integer,String> getLabels() {
		return labels;
	}

	public void setLabels(Map<Integer,String> labels) {
		this.labels = labels;
	}

	public Map<Integer,Integer> getConstants() {
		return constants;
	}

	public void setConstants(Map<Integer,Integer> constants) {
		this.constants = constants;
	}
	
}