from pyspark.sql import *

import sys
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df

if __name__ == "__main__":

    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: hello_spark_sql.py <file_path>")
        sys.exit(-1)

    file_path = sys.argv[1]

    logger.info('Starting program')

    survey_raw_df = load_survey_df(spark, file_path)
    
    survey_raw_df.createOrReplaceTempView('survey_tbl')
    countDF = spark.sql('select Country, count(1) as count from survey_tbl where Age<40 group by Country')
    countDF.show()

    logger.info('Finishing program')

    spark.stop()