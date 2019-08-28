#!/usr/bin/env bash

spark-submit --master yarn \
--name "demoYarn" \
--deploy-mode client \
--class com.jinsong.sparkcodes.RddOperate \
/Users/jinsong/GitProject/MyDemo/target/MyDemo-1.0-SNAPSHOT.jar

spark-submit --master local \
--name "demo" \
--class com.jinsong.sparkcodes.RddOperate \
/Users/jinsong/GitProject/MyDemo/target/MyDemo-1.0-SNAPSHOT.jar

spark-submit --master spark://songdeMacBook-Pro.local:7077 \
--name "demoStandalone" \
--deploy-mode cluster \
--class com.jinsong.sparkcodes.RddOperate \
/Users/jinsong/GitProject/MyDemo/target/MyDemo-1.0-SNAPSHOT.jar