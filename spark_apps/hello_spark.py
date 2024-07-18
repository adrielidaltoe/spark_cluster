from pyspark.sql import SparkSession

import sys
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":

    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: hello_spark.py <file_path>")
        sys.exit(-1)

    file_path = sys.argv[1]

    logger.info('Starting program')

    survey_raw_df = load_survey_df(spark, file_path)
    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    
    count_df.show()

    logger.info('Finishing program')

    spark.stop()