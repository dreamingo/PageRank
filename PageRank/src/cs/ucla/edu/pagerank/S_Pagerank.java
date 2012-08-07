package cs.ucla.edu.pagerank;

import java.util.*;

public class S_Pagerank {
	
	public PagerankSimulator simulator = null;
	
	public void map(int nodeId, AdjacencyNode node) {
		double p = node.getRankValue() / node.getAdjacencyList().size();
		
		simulator.mapEmit(nodeId, node);
		
		Iterator<Integer> iter = node.getAdjacencyList().iterator();
		while (iter.hasNext()) {
			simulator.mapEmit(iter.next(), p);
		}
	}
	
	public void reduce(int nodeId, ArrayList<Object> list) {
		AdjacencyNode node = null;
		Iterator iter = list.iterator();
		
		double s = 0;
		while (iter.hasNext()) {
			Object p = iter.next();
			if (p instanceof AdjacencyNode) {
				node = (AdjacencyNode)p;
			}
			else {
				s += (double) p;
			}
		}
		
		node.setRankValue(s);
		simulator.reduceEmit(nodeId, node);
	}
	
}