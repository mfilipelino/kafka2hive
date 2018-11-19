#!/bin/bash

export PYSPARK_PYTHON=python2.7
export PYSPARK_DRIVER_PYTHON=python2.7
export JAVA_HOME=/opt/oracle/jdk1.8.0_144
export SPARK_HOME=/opt/apache/spark-2.3.0-bin-hadoop2.6

PREFIX="/home/batcher/projetos/batches"
PORT=$(( $RANDOM % 1000 + 40000 ))

sudo su -l batcher -c "cd ${PREFIX} \
&&  ${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode client \
--conf spark.yarn.queue=juvenal \
--conf spark.yarn.appMasterEnv.JAVA_HOME=${JAVA_HOME} \
--conf spark.executorEnv.JAVA_HOME=${JAVA_HOME} \
--conf spark.executor.memory=2g \
--conf spark.driver.memory=3g \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.initialExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=4 \
--conf spark.ui.port=${port} \
--driver-class-path /etc/hive/conf \
${PREFIX}/hdfs2hive.py \
@option.env@"
