#!/bin/bash

# clean work
cd ~/hadoop-1.0.3/hadoop-1.0.3/

cd graph_input

NUM=`ls | wc -l`

if [ $NUM -ne 1 ]
then
	echo "graph_input folder does not have exactly 1 file!"
	exit
fi

NAME=`ls`

cd ..

bin/hadoop fs -rmr .

# mapreduce
bin/hadoop fs -put graph_input graph_input

bin/hadoop jar pagerank.jar cs.ucla.edu.pagerank.GraphMapReduce

bin/hadoop fs -get graph_output tmp_output

# result
rm -r input

mkdir input

mv tmp_output/part-00000 input

mv input/part-00000 input/${NAME%.*}

rm -r tmp_output

