#!/bin/bash 

if [ $# -gt 10 ]; then 
	echo "Number of options are greater then required"
	exit 0 
fi

java -cp .:$HADOOP_FDW_CLASSPATH  HiveJdbcClient "$@"

exit 0
