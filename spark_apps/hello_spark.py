from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":

    conf = get_spark_app_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    
    logger = Log4J(spark)

    logger.info('Starting program')

    logger.info('Finishing program')

    spark.stop()