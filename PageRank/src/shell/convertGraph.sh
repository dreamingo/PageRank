#!/bin/bash

# clean work
cd ~/hadoop-1.0.3/hadoop-1.0.3/

bin/hadoop fs -rmr .

# mapreduce
bin/hadoop fs -put graph_input graph_input

bin/hadoop jar pagerank.jar cs.ucla.edu.pagerank.GraphMapReduce

bin/hadoop fs -get graph_output tmp_output

# result
rm -r input

mkdir input

mv tmp_output/part-00000 input

rm -r tmp_output
