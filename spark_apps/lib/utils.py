'''import configparser

from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read('spark.conf')
    print(config)

    for key, value in config.items('SPARK_APP_CONFIGS'):
        spark_conf.set(key, value)

    return spark_conf
'''

import configparser


def get_spark_app_config():
    config = configparser.ConfigParser()
    config.read('spark.conf')
    print(config)
