#!/bin/bash

#
# Wojciech Golab, 2016
#

#source settings.sh

#JAVA_CC=$JAVA_HOME/bin/javac
#THRIFT_CC=./thrift-0.12.0

export CLASSPATH=".:gen-java/:lib/*"

echo --- Cleaning
rm -f *.class
rm -fr gen-java

echo --- Compiling Thrift IDL
thrift --version &> /dev/null
ret=$?
if [ $ret -ne 0 ]; then
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "ERROR: The Thrift compiler does not work on this host."
    echo "       Please build on one of the eceubuntu or ecetesla hosts."
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    exit 1
fi
thrift --version
thrift --gen java:generated_annotations=suppress a3.thrift

echo --- Compiling Java
javac -version
javac gen-java/*.java
javac *.java

