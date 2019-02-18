#!/bin/bash

#percorsi relativi (a build.sbt) o assoluti (meglio)

JAR_FILE=/home/anto/Scrivania/GUI/Script/Local/HelpfulnessRank-assembly-0.1.jar
MAIN_CLASS=Main
DEMO=TRUE
FILE_INPUT=/home/anto/Scrivania/GUI/demo.csv
DIR_OUTPUT=/home/anto/Scrivania/GUI/result/
LAMBDA=20
ITER=10
PART=4
TIMEOUT=3000

if [ -z "$1" ]
then
    echo "insert type computation as params"
    exit
fi

TYPE_COMP=$1

echo "==========================="
echo "PARAMS:"
echo "==========================="
echo "> path jar    -> $JAR_FILE"
echo "> mainclass   -> $MAIN_CLASS"
echo "> tipoComp    -> $TYPE_COMP"
echo "> demo        -> $DEMO"
echo "> inputFile   -> $FILE_INPUT"
echo "> outputDir   -> $DIR_OUTPUT"
echo "> LAMBDA      -> $LAMBDA"
echo "> numIter     -> $ITER"
echo "> numPart     -> $PART"
echo "> timeout     -> $TIMEOUT ms"
echo "==========================="
echo "STARTING..."
echo "==========================="
/home/anto/Spark/bin/spark-submit --class $MAIN_CLASS $JAR_FILE $TYPE_COMP $DEMO $FILE_INPUT $DIR_OUTPUT $LAMBDA $ITER $PART $TIMEOUT

