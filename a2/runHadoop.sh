#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export CLASSPATH=`hadoop classpath`

TASK=Task4

echo --- Deleting
rm $TASK.jar
rm $TASK*.class

echo --- Compiling
$JAVA_HOME/bin/javac $TASK.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf $TASK.jar $TASK*.class

echo --- Running
INPUT=/tmp/smalldata.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
hdfs dfs -copyFromLocal sample_input/smalldata.txt /tmp
time yarn jar $TASK.jar $TASK -D mapreduce.map.java.opts=-Xmx4g $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
hdfs dfs -copyToLocal $OUTPUT /home2/p99patel/outputHadoop/
