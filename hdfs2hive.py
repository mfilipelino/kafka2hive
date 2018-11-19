from sys import argv
from pyspark import SparkContext
from pyspark.sql import HiveContext
from py4j.protocol import Py4JJavaError
from datetime import timedelta

import json
from datetime import datetime

from settings import get_config


def mapJson(element):
    data = json.loads(element)

    id_ = data.get('id')
    type_ = data.get('type')
    scope = data.get('scope')
    event_type = data.get('event_type')
    event_time = data.get('event_time')
    action_type = data.get('action_type')
    partition_namespace = data.get('payload').get(scope).keys()[0]
    payload = json.dumps(data.get('payload').get(scope).get(partition_namespace))
    partition_datetime = event_time[:13]

    return json.dumps(
        dict(
            id=id_,
            type=type_,
            scope=scope,
            event_type=event_type,
            event_time=event_time,
            action_type=action_type,
            payload=payload,
            partition_namespace=partition_namespace.replace(':', '-'),
            partition_datetime=partition_datetime
        )
    )


def create_rdd_hdfs(spark_context, hdfs_directory_path):

    try:
        rdd = spark_context.textFile(hdfs_directory_path)
    except Py4JJavaError as exception_java_error:
        rdd = None
        if 'matches 0 files' in str(exception_java_error):
            print 'WARNING: Matches 0 files in directory: {}'.format(hdfs_directory_path)
        raise (exception_java_error)
    finally:
        return rdd


def create_hive_context(spark_context):
    hive_context = HiveContext(spark_context)
    hive_context.setConf('hive.exec.dynamic.partition.mode', 'nonstrict')
    hive_context.setConf('hive.exec.max.dynamic.partitions', '17520')
    return hive_context


def get_hdfs_directory(hdfs_base, format_regex):
    return hdfs_base + (datetime.now() + timedelta(hours=-1)).strftime(format_regex) + '*'


def save_table_hive(rdd, database_table, hive_context):
    data_frame = hive_context.jsonRDD(rdd, hive_context.table(database_table).schema)
    data_frame.repartition(1).write.partitionBy('partition_namespace', 'partition_datetime') \
        .saveAsTable(database_table, mode="append")


def run(configurations):

    spark_context = SparkContext(appName=configurations.app_name)
    hdfs_directory = get_hdfs_directory(configurations.hdfs_directory_base, format_regex='%Y-%m-%d_%H')
    rdd = create_rdd_hdfs(spark_context=spark_context,  hdfs_directory_path=hdfs_directory)
    rdd = rdd.map(mapJson)
    hive_context = create_hive_context(spark_context)
    save_table_hive(rdd, configurations.database_table, hive_context)


if __name__ == "__main__":

    if len(argv) > 1:
        configurations = get_config(argv[1])
    else:
        configurations = get_config(None)

    run(configurations)
