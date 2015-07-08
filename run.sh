#!/bin/sh

rm -r output
rm -r ngram_classes/
hadoop fs -rmr project/output
sudo chmod 777 -R ngram_classes/
rm -r ngram_classes/
mkdir ngram_classes
javac -classpath hadoop-core.jar -d  ngram_classes NGram.java
jar -cvf NGram.jar -C ngram_classes/ .
hadoop jar NGram.jar edu.cmu.NGram project/input project/output
hadoop fs -get project/output output  

