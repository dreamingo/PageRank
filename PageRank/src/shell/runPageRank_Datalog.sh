#!/bin/bash

if [ -z "$1" ]
then
	echo "Usage: ./runPageRank.sh [Iteration_Times]"
	exit
fi

# clean work
cd ~/hadoop-1.0.3/hadoop-1.0.3/

bin/hadoop fs -rmr .

rm -r output

mkdir output


cd input

NUM=`ls | wc -l`

if [ $NUM -ne 1 ]
then
        echo "input folder does not have exactly 1 file!"
        exit
fi

NAME=`ls`

cd ..

# mapreduce
bin/hadoop fs -put input "$NAME"_Datalog_0

bin/hadoop jar pagerank.jar cs.ucla.edu.pagerank.Pagerank_Datalog "$1" "$NAME" 2>&1 | tee backup/"$NAME"_Datalog_"$1".log

bin/hadoop fs -get . output

# backup

mv output/victor/* backup

