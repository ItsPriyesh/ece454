#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SCALA_HOME=/usr
export CLASSPATH=".:/opt/spark-latest/jars/*"

TASK=Task1

echo --- Deleting
rm $TASK.jar
rm $TASK*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g $TASK.scala
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
time spark-submit --master yarn --class $TASK --driver-memory 4g --executor-memory 4g $TASK.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
hdfs dfs -copyToLocal $OUTPUT /home2/p99patel/outputSpark/

