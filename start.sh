#!/bin/bash

PREFIX="/home/batcher/projetos/batches"
PORT=$(( $RANDOM % 1000 + 40000 ))


export PYSPARK_PYTHON=python2.7
export PYSPARK_DRIVER_PYTHON=python2.7
export JAVA_HOME=/opt/oracle/jdk1.8.0_144
export SPARK_HOME=/opt/apache/spark-2.3.0-bin-hadoop2.6
port=$(( $RANDOM % 1000 + 40000 ))
${SPARK_HOME}/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.queue=juvenal \
    --conf spark.yarn.appMasterEnv.JAVA_HOME=/opt/oracle/jdk1.8.0_144 \
    --conf spark.executorEnv.JAVA_HOME=/opt/oracle/jdk1.8.0_144 \
    --conf spark.ui.port=${port} \
    --conf spark.executor.memory=3g \
    --conf spark.driver.memory=3g \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.maxExecutors=4 \
    --conf spark.dynamicAllocation.minExecutors=2 \
    --conf spark.dynamicAllocation.initialExecutors=2 \
    ${PREFIX}/hdfs2hive.py \
    juvenal-dev
