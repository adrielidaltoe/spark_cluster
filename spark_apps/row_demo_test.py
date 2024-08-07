from datetime import date
from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from row_demo import to_date_df
from lib.utils import get_spark_app_config


class RowDemoTestClass(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        conf = get_spark_app_config()

        cls.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        my_schema = StructType([
          StructField('id', StringType()),
          StructField('eventDate', StringType())
        ])

        my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        my_df = cls.spark.createDataFrame(my_rdd, my_schema)
    
    def test_data_type(self):
        rows = to_date_df(self.my_df, 'M/d/y', 'eventDate').collect()
        for row in rows:
            self.assertIsInstance(row['eventDate'], date)

    def test_date_value(self):
        rows = to_date_df(self.my_df, 'M/d/y', 'eventDate').collect()
        for row in rows:
            self.assertEqual(row['eventDate'], date(2020, 4, 5))
