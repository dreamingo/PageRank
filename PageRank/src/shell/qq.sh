#!/bin/bash

myvar=$(ps -A | grep QQ.exe | awk '{print $1}')
if [ -z "$myvar" ]
then
	echo "Start QQ Process:"
	/opt/longene/qq2012/qq2012.sh
else	
	echo "Kill QQ Process: ${myvar}"
	kill -KILL "${myvar}"
fi
