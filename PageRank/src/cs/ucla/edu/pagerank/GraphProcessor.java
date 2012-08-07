package cs.ucla.edu.pagerank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class GraphProcessor {
	
	private File file;
	
	public GraphProcessor(String fileName) {
		file = new File(fileName);
	}
	
	public HashMap<Integer, AdjacencyNode> read() {
		try {
			
			FileReader reader = new FileReader(file);
			BufferedReader breader = new BufferedReader(reader);
			HashMap<Integer, AdjacencyNode> map = new HashMap<Integer, AdjacencyNode>();
			
			String s = breader.readLine();
			int sum = Integer.parseInt(s);
			
			for (int t=1; t<=sum; t++) {
				//System.out.println(s);
				
				s = breader.readLine();
				String[] stringArray = s.split(",");
				//System.out.println(stringArray[0]);
				AdjacencyNode node = new AdjacencyNode(t, 1.00 / sum);
				for (int i=0; i<stringArray.length; i++) {
					if (!stringArray[i].equals("")) node.addNeighbor(Integer.parseInt(stringArray[i]));
				}
				
				map.put(t, node);
			}
			
			System.out.println(map);
			return map;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
}
