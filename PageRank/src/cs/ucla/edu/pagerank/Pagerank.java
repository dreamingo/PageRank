package cs.ucla.edu.pagerank;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Pagerank {
	
	public static int ITERATION_TIMES = 1;
	
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			AdjacencyNode node = null;
			try {
				node = AdjacencyNode.construct(key, value);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Double p = node.getRankValue() / node.getAdjacencyList().size();
			
			output.collect(key, value);
			System.err.println("1: " + key + "\t" + value);
			
			
			Iterator<String> iter = node.getAdjacencyList().iterator();
			while (iter.hasNext()) {
				output.collect(new Text(iter.next()), new Text(p + "\t" + 0));
				System.err.println("2: " + key + "\t" + p);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			AdjacencyNode node = null;
			double s = 0;
			while (values.hasNext()) {
				AdjacencyNode temp = null;
				try {
					temp = AdjacencyNode.construct(key, values.next());
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				if (temp.getFlag()) {
					node = temp;
					System.err.println("3.1: " + key + "\t" + "OK");
				}
				else {
					s += temp.getP();
					System.err.println("3.2: " + key + "\t" + s);
				}
			}
			
			node.setRankValue(s);
			output.collect(key, new Text(node.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		ITERATION_TIMES = Integer.parseInt(args[0]);
		for (int runs = 1; runs < ITERATION_TIMES; runs++) {
			runJob(runs, false);
        }
		runJob(ITERATION_TIMES, true);
	}
	
	private static void runJob(int runs, boolean last) throws Exception {
		JobConf conf = new JobConf(Pagerank.class);
		conf.setJobName("pagerank");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setPartitionerClass(KeyPartitioner.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("file" + runs));
		if (last) {
			FileOutputFormat.setOutputPath(conf, new Path("output"));
		}
		else {
			FileOutputFormat.setOutputPath(conf, new Path("file" + (runs+1)));
		}
		JobClient.runJob(conf);
	}
}