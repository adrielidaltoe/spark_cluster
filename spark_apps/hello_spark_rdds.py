from pyspark.sql import SparkSession

import sys
from lib.utils import get_spark_app_config
from lib.logger import Log4J
from collections import namedtuple


SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

if __name__ == "__main__":
    
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    
    sc = spark.sparkContext
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: /path/hello_spark_rdds.py <file_path>")
        sys.exit(-1)
    
    file_path = sys.argv[1]

    linesRDD = sc.textFile(file_path)
    
    # Extract the header line
    header = linesRDD.first()
    
    # Filter out the header line from the RDD
    linesRDD_no_header = linesRDD.filter(lambda line: line != header)

    partitionedRDD = linesRDD_no_header.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(','))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.Age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)
