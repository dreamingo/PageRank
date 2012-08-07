package cs.ucla.edu.pagerank;

import java.util.ArrayList;

public class AdjacencyNode {

	private Integer flag;
	private Double p;
	
	private Long nodeId;
	private Double rankValue;
	private ArrayList<Integereger> list;
	//private PagerankObject pagerankObject;
	
	public AdjacencyNode(Double p) {
		this.p = p;
		this.flag = false;
	}
	
	public AdjacencyNode(Integer nodeId, Double rankValue) {
		this.nodeId = nodeId;
		this.list = new ArrayList<Integereger>();
		this.rankValue = rankValue;
		this.flag = true;
	}
	
	public Double getP() {
		return p;
	}
	
	public void addNeighbor(Integer e) {
		list.add(e);
	}
	
	public ArrayList<Integereger> getAdjacencyList() {
		return list;
	}
	
	public Integer getNodeId() {
		return nodeId;
	}
	
	public Double getRankValue() {
		return rankValue;
	}

	public void setRankValue(Double rankValue) {
		this.rankValue = rankValue;
	}

	public String toString() {
		return nodeId + ":" + list.toString() + " rankValue: " + rankValue;
	}

}