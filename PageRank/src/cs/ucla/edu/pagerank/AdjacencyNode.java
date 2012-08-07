package cs.ucla.edu.pagerank;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class AdjacencyNode {

	private Boolean flag;
	private Double p;
	
	private Long nodeId;
	private Double rankValue;
	private ArrayList<Long> list;
	//private PagerankObject pagerankObject;
	
	private AdjacencyNode(Double p) {
		this.p = p;
		this.flag = false;
	}
	
	private AdjacencyNode(Long nodeId, Double rankValue) {
		this.nodeId = nodeId;
		this.list = new ArrayList<Long>();
		this.rankValue = rankValue;
		this.flag = true;
	}
	
	public static AdjacencyNode construct(LongWritable key, Text value) throws Exception {
		String[] array = value.toString().split("\t");
		if (array == null || array.length == 0) throw new Exception();
		
		Double rankValue = Double.parseDouble(array[0]);
		
		if ((array.length == 2) && (array[1].equals("0"))) {
			//Format:
			//0.3	0
			return new AdjacencyNode(rankValue);
		}
		else {
			//Format:
			//0.3	1	2	3
			//0.2
			AdjacencyNode node = new AdjacencyNode(key.get(), rankValue);
			for (int i=1; i < array.length; i++) {
				node.addNeighbor(Long.parseLong(array[i]));
			}
			return node;
		}
		
	}
	
	public Double getP() {
		return p;
	}
	
	public void addNeighbor(Long e) {
		list.add(e);
	}
	
	public ArrayList<Long> getAdjacencyList() {
		return list;
	}
	
	public Long getNodeId() {
		return nodeId;
	}
	
	public Double getRankValue() {
		return rankValue;
	}

	public void setRankValue(Double rankValue) {
		this.rankValue = rankValue;
	}

	public String toString() {
		if (!flag) {
			return rankValue + "\t" + 0;
		}
		else {
			String s = "";
			Iterator<Long> iter = list.iterator();
			if (iter.hasNext()) {
				s += "\t" + iter.next();
			}
			return rankValue + s;
		}
	}
	
	public Boolean getFlag() {
		return flag;
	}
	
	public static void main(String[] args) {
		AdjacencyNode node = new AdjacencyNode(100L, 0.2);
		//node.addNeighbor(2L);
		//node.addNeighbor(4L);
		
		System.out.println(node);
	}

}