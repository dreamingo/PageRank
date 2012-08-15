package cs.ucla.edu.pagerank;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Pagerank_Datalog {
	
	public static int count = 0;
	public static String taskname = "undefined_task";
	public static int iteration_times = 0;
	
	
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			AdjacencyNode node = null;
			try {
				node = AdjacencyNode.construct(key, value);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Double p = null;
			try {
				p = node
					.getRankValue() / 
					node
					.getAdjacencyList()
					.size();
				count++;
			} catch (Exception e) {
				System.out.println("Error: " + count + " !\n");
				e.printStackTrace();
				System.exit(1);
			}
			
			output.collect(key, value);
			//System.err.println("1: " + key + "\t" + value);
			
			
			Iterator<String> iter = node.getAdjacencyList().iterator();
			while (iter.hasNext()) {
				output.collect(new Text(iter.next()), new Text(p + "\t" + "*"));
				//System.err.println("2: " + key + "\t" + p);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			AdjacencyNode node = null;
			
			double s = 0;
			//boolean t = false;
			
			while (values.hasNext()) {
				AdjacencyNode temp = null;
				try {
					temp = AdjacencyNode.construct(key, values.next());
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				if (temp.getFlag()) {
					node = temp;
					//t = true;
					//System.err.println("3.1: " + key + "\t" + "OK");
				}
				else {
					s += temp.getP();
					//System.err.println("3.2: " + key + "\t" + s);
				}
			}
			
			//Main modifications done here
			//System.err.println("Flag State: " + t + " " + key);
			try {
				if (s > node.getRankValue()) {
					node.setRankValue(s);
				}
			}
			catch (Exception e) {
				//it means the node has no outsource nodes
				//due to the imperfect of dataset
				e.printStackTrace();
				node = new AdjacencyNode(key.toString(), s);
			}
			
			output.collect(key, new Text(node.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Pagerank_Datalog: [1] ITERATION_TIMES [2] TASKNAME !");
			return;
		}
		
		iteration_times = Integer.parseInt(args[0]);
		taskname = args[1];
		
		/* runs number starts from 0 */
		for (int runs = 0; runs < iteration_times; runs++) {
			runJob(runs);
        }
		
	}
	
	private static void runJob(int runs) throws Exception {
		JobConf conf = new JobConf(Pagerank_Datalog.class);
		conf.setJobName("pagerank_datalog");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setPartitionerClass(KeyPartitioner.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(taskname + "_Datalog_" + runs));
		FileOutputFormat.setOutputPath(conf, new Path(taskname + "_Datalog_" + (runs+1)));
		
		JobClient.runJob(conf);
	}
}