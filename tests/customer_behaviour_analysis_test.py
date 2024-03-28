import datetime
import unittest
import warnings
from typing import Iterable, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.testing.utils import assertDataFrameEqual

from customer_behaviour_analysis import transactions_schema, monthly_aggregates


class CustomerBehaviourAnalysisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("customer-behaviour-analysis-test") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    monthly_aggregates_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("month", IntegerType()),
        StructField("purchase_frequency", IntegerType()),
        StructField("average_monthly_spending", DoubleType()),
    ])

    def test_monthly_aggregates_simple(self):
        transactions_data: Iterable[Any] = [
            ("T100", "C1", "P1", 150.0, datetime.date.fromisoformat("2024-01-10")),
            ("T101", "C2", "P2", 75.0, datetime.date.fromisoformat("2024-01-15")),
            ("T102", "C1", "P3", 200.0, datetime.date.fromisoformat("2024-02-05")),
        ]
        expected_data: Iterable[Any] = [
            ("C1", 1, 1, 150.0),
            ("C1", 2, 1, 200.0),
            ("C2", 1, 1, 75.0),
        ]
        transactions_df = self.spark.createDataFrame(transactions_data, schema=transactions_schema)
        expected_df = self.spark.createDataFrame(expected_data, schema=self.monthly_aggregates_schema)
        actual = monthly_aggregates(transactions_df)

        assertDataFrameEqual(actual, expected_df)

    def test_monthly_aggregates_complex(self):
        transactions_data: Iterable[Any] = [
            ("T100", "C1", "P1", 150.0, datetime.date.fromisoformat("2024-01-10")),
            ("T100", "C1", "P1", 250.0, datetime.date.fromisoformat("2024-01-11")),
            ("T101", "C2", "P2", 75.0, datetime.date.fromisoformat("2024-01-15")),
            ("T101", "C2", "P1", 125.0, datetime.date.fromisoformat("2024-02-16")),
            ("T102", "C1", "P3", 200.0, datetime.date.fromisoformat("2024-02-05")),
        ]
        expected_data: Iterable[Any] = [
            ("C1", 1, 2, 200.0),
            ("C1", 2, 1, 200.0),
            ("C2", 1, 1, 75.0),
            ("C2", 2, 1, 125.0),
        ]
        transactions_df = self.spark.createDataFrame(transactions_data, schema=transactions_schema)
        expected_df = self.spark.createDataFrame(expected_data, schema=self.monthly_aggregates_schema)
        actual = monthly_aggregates(transactions_df)

        assertDataFrameEqual(actual, expected_df)
