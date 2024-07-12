from pyspark.sql import *

from lib.logger import Log4J

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName('Hello Spark') \
        .master('spark://spark-master:7077') \
        .getOrCreate()
    
    logger = Log4J(spark)

    logger.info('Starting program')

    logger.info('Finishing program')

    spark.stop()