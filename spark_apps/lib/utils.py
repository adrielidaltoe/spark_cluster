import configparser
import os
from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()

    config_file_path = '/opt/spark/apps/lib/spark.conf'
    
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(
            f"Configuration file {config_file_path} not found.")
    
    config.read('/opt/spark/apps/spark.conf')
    print(config.items())

    if 'SPARK_APP_CONFIGS' not in config.sections():
        raise configparser.NoSectionError('SPARK_APP_CONFIGS')

    for key, value in config.items('SPARK_APP_CONFIGS'):
        spark_conf.set(key, value)

    return spark_conf


def load_survey_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)


def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()
