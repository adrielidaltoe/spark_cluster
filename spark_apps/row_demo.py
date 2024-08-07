from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.utils import get_spark_app_config


def to_date_df(df, fmt, fld):
        return df.withColumn(fld, to_date(col(fld), fmt))

if __name__ == "__main__":
    
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    
    my_schema = StructType([
          StructField('id', StringType()),
          StructField('eventDate', StringType())
    ])

    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()

    new_df = to_date_df(my_df, 'M/d/y', 'eventDate')
    new_df.printSchema()
    new_df.show()

    spark.stop()