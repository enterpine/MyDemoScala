#!/usr/bin/env bash

hdfs dfs -put -f /Users/jinsong/GitProject/MyDemo/data/input/GradeTable /input/

hdfs dfs -rm -r /output
hadoop jar /Users/jinsong/GitProject/MyDemo/target/MyDemo-1.0-SNAPSHOT.jar com.jinsong.mr.sort.SortBaseDriver \
-D mapreduce.job.reduces=5 /input/Sample.txt /input/Sample2.txt /output

hadoop jar /Users/jinsong/GitProject/MyDemo/target/MyDemo-1.0-SNAPSHOT.jar com.jinsong.mr.sort.SortDemoUsingHashPart \
-D mapreduce.job.reduces=5  /output/part-m-00000  /output/part-m-00001 /outputHashPart

hadoop jar /Users/jinsong/GitProject/MyDemo/target/MyDemo-1.0-SNAPSHOT.jar com.jinsong.mr.sort.SortByTempUsingTotalOrderPartition \
-D mapreduce.job.reduces=5  /output/part-m-00000  /output/part-m-00001 /outputTotalOrder

hdfs dfs -rm -r /outputTotalOrderOverPart
hadoop jar /Users/jinsong/GitProject/MyDemo/target/MyDemo-1.0-SNAPSHOT.jar com.jinsong.mr.sort.SortByTempGlobleOverPart \
-D mapreduce.job.reduces=8 /output/part-m-00000  /output/part-m-00001 /outputTotalOrderOverPart