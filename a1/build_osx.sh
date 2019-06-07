#!/bin/bash

#
# Wojciech Golab, 2016-2019
#
# brew install thrift@0.12

JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home

JAVA=$JAVA_HOME/bin/java
JAVA_CC=$JAVA_HOME/bin/javac
THRIFT_CC=thrift

echo --- Cleaning
rm -f *.jar
rm -f *.class
rm -fr gen-java

echo --- Compiling Thrift IDL
$THRIFT_CC --version &> /dev/null
ret=$?
if [ $ret -ne 0 ]; then
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "ERROR: The Thrift compiler does not work on this host."
    echo "       Please build on one of the eceubuntu or ecetesla hosts."
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    exit 1
fi
$THRIFT_CC --version
$THRIFT_CC --gen java:generated_annotations=suppress a1.thrift


echo --- Compiling Java
$JAVA_CC -version
$JAVA_CC gen-java/*.java -cp .:"lib/*"
$JAVA_CC *.java -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*"

echo --- Done, now run your code.
echo     Examples:
echo $JAVA '-cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" FENode 10123'
echo $JAVA '-cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" BENode localhost 10123 10124'
echo $JAVA '-cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" Client localhost 10123 hello'

