package cs.ucla.edu.pagerank;

import java.util.ArrayList;

public class AdjacencyNode {

	private int nodeId;
	private double rankValue;
	private ArrayList<Integer> list;
	//private PagerankObject pagerankObject;
	
	public AdjacencyNode(int nodeId, double rankValue) {
		this.nodeId = nodeId;
		this.list = new ArrayList<Integer>();
		this.rankValue = rankValue;
	}
	
	public void addNeighbor(int e) {
		list.add(e);
	}
	
	public ArrayList<Integer> getAdjacencyList() {
		return list;
	}
	
	public int getNodeId() {
		return nodeId;
	}
	
	public double getRankValue() {
		return rankValue;
	}

	public void setRankValue(double rankValue) {
		this.rankValue = rankValue;
	}

	public String toString() {
		return nodeId + ":" + list.toString() + " rankValue: " + rankValue;
	}

}