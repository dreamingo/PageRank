package cs.ucla.edu.pagerank;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Pagerank {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			
			AdjacencyNode node = null;
			try {
				node = AdjacencyNode.construct(key, value);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Double p = node.getRankValue() / node.getAdjacencyList().size();
			
			output.collect(key, value);
			Iterator<Long> iter = node.getAdjacencyList().iterator();
			while (iter.hasNext()) {
				output.collect(new LongWritable(iter.next()), new Text(p + "\t" + 0));
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			
			AdjacencyNode node = null;
			double s = 0;
			while (values.hasNext()) {
				AdjacencyNode temp = null;
				try {
					temp = AdjacencyNode.construct(key, values.next());
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				if (node.getFlag()) {
					node = temp;
				}
				else {
					s += temp.getP();
				}
			}
			
			node.setRankValue(s);
			output.collect(key, new Text(node.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Pagerank.class);
		conf.setJobName("pagerank");

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}