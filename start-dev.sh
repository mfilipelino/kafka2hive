#!/bin/bash

PREFIX="/home/marcos.lino/hdfs2hive/"
PORT=$(( $RANDOM % 1000 + 40000 ))

spark-submit \
	--conf spark.yarn.queue=juvenal \
    --driver-class-path /etc/hive/conf \
    --conf spark.sql.hive.convertMetastoreParquet=false \
    --conf spark.executor.memory=2g \
    --conf spark.executor.extraJavaOptions="-XX:MaxPermSize=2048M" \
    --conf spark.driver.memory=2g \
    --conf spark.dynamicAllocation.initialExecutors=1 \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=2 \
	--conf spark.ui.port=${PORT} \
	--queue dev \
    ${PREFIX}/hdfs2hive.py


