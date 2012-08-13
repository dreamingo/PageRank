package cs.ucla.edu.pagerank;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class GraphMapReduce {
		
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			output.collect(key, value);
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			StringBuffer sb = new StringBuffer();
			while (values.hasNext()) {
				sb.append("\t" + values.next().toString());
			}
			
			output.collect(key, new Text("1.00" + sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		runJob();
	}
	
	private static void runJob() throws Exception {
		JobConf conf = new JobConf(GraphMapReduce.class);
		conf.setJobName("graphMapReduce");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setPartitionerClass(KeyPartitioner.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("graph_input"));
		FileOutputFormat.setOutputPath(conf, new Path("graph_output"));
		
		JobClient.runJob(conf);
	}
}