package cs.ucla.edu.pagerank;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Pagerank {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, AdjacencyNode, LongWritable, AdjacencyNode> {
		//private final static IntWritable one = new IntWritable(1);
		//private Text word = new Text();

		public void map(LongWritable key, AdjacencyNode value, OutputCollector<LongWritable, AdjacencyNode> output, Reporter reporter) throws IOException {
			Double p = value.getRankValue() / value.getAdjacencyList().size();
			
			output.collect(key, value);
			
			Iterator<Long> iter = node.getAdjacencyList().iterator();
			while (iter.hasNext()) {
				output.collect(iter.next(), new AdjacencyNode(p));
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, AdjacencyNode, LongWritable, AdjacencyNode> {
		public void reduce(LongWritable key, Iterator<AdjacencyNode> values, OutputCollector<LongWritable, AdjacencyNode> output, Reporter reporter) throws IOException {
			AdjacencyNode node = null;
			double s = 0;
			
			while (values.hasNext()) {
				AdjacencyNode p = values.next();
				if (p.getFlag() == true) {
					node = (AdjacencyNode)p;
				}
				else {
					s += p.getP();
				}
			}
			
			node.setRankValue(s);
			output.collect(key, node);
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Pagerank.class);
		conf.setJobName("pagerank");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(AdjacencyNode.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}