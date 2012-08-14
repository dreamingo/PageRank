#!/bin/bash

# clean work
cd ~/hadoop-1.0.3/hadoop-1.0.3/

bin/hadoop fs -rmr .

rm -r output

mkdir output

# mapreduce
bin/hadoop fs -put input file1

bin/hadoop jar pagerank.jar cs.ucla.edu.pagerank.Pagerank_Datalog 5

bin/hadoop fs -get . output


