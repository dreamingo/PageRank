package cs.ucla.edu.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class KeyPartitioner implements Partitioner<Text, Text>
{
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		return Integer.parseInt(key.toString()) % numPartitions;
	}

	@Override
	public void configure(JobConf conf) {
	}

}