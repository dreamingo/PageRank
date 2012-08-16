#!/bin/bash

if [ -z "$1" ]
then
	echo "Usage: ./results.sh [foldername]"
	exit
fi

cd ~/hadoop-1.0.3/hadoop-1.0.3/backup/Wiki-Vote_Origin/"$1"

cat part-00000 | awk '{print $1"\t"$2}' | sort -r -g -k 2 | less
