# config


class BaseConfig(object):
    # basic
    app_name = 'hdfs2hive-base'


class DevConfig(BaseConfig):
    app_name = 'hdfs2hive--dev'
    checkpoint_directory = '/user/marcos.lino/checkpoints/'
    hdfs_directory_base = '/user/marcos.lino/files/'
    format_datetime = '%Y-%m-%d'

    # hive
    database_table = 'teste.test_order'


def get_config(argv1):
    if argv1 == 'hdfs2hive':
        return JuvenalOrderConfig()
    elif argv1 == 'hdfs2hive-dev':
        return JuvenalDeliveryConfig()
    else:
        return DevConfig()
