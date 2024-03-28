import datetime
import unittest
import warnings
from typing import Iterable, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.testing.utils import assertDataFrameEqual

from customer_behaviour_analysis import transaction_schema, monthly_aggregates, product_schema, preferred_categories, customer_schema, final_result, output_schema


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

    preferred_categories_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("preferred_category", StringType()),
        StructField("total_amount", DoubleType()),
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
        transactions_df = self.spark.createDataFrame(transactions_data, schema=transaction_schema)
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
        transactions_df = self.spark.createDataFrame(transactions_data, schema=transaction_schema)
        expected_df = self.spark.createDataFrame(expected_data, schema=self.monthly_aggregates_schema)
        actual = monthly_aggregates(transactions_df)

        assertDataFrameEqual(actual, expected_df)

    def test_preferred_categories(self):
        transactions_data: Iterable[Any] = [
            ("T100", "C1", "P1", 150.0, datetime.date.fromisoformat("2024-01-10")),
            ("T101", "C2", "P2", 75.0, datetime.date.fromisoformat("2024-01-15")),
            ("T102", "C1", "P3", 200.0, datetime.date.fromisoformat("2024-02-05")),
        ]
        products_data = [
            ("P1", "Laptop", "Electronics"),
            ("P2", "Headphones", "Electronics"),
            ("P3", "Coffee Maker", "Appliances"),
        ]
        expected_data = [
            ("C1", "Appliances", 200.0),
            ("C2", "Electronics", 75.0),
        ]
        transactions_df = self.spark.createDataFrame(transactions_data, schema=transaction_schema)
        products_df = self.spark.createDataFrame(products_data, schema=product_schema)
        expected_df = self.spark.createDataFrame(expected_data, schema=self.preferred_categories_schema)
        actual = preferred_categories(transactions_df, products_df)

        assertDataFrameEqual(actual, expected_df)

    def test_preferred_categories_tie_breaker(self):
        transactions_data = [
            ("T100", "C1", "P1", 150.0, datetime.date.fromisoformat("2024-01-10")),
            ("T100", "C1", "P2", 50.0, datetime.date.fromisoformat("2024-01-10")),
            ("T101", "C2", "P2", 75.0, datetime.date.fromisoformat("2024-01-15")),
            ("T102", "C1", "P3", 200.0, datetime.date.fromisoformat("2024-02-05")),
        ]
        products_data = [
            ("P1", "Laptop", "Electronics"),
            ("P2", "Headphones", "Electronics"),
            ("P3", "Coffee Maker", "Appliances"),
        ]
        expected_data = [
            ("C1", "Appliances", 200.0),
            ("C2", "Electronics", 75.0),
        ]
        transactions_df = self.spark.createDataFrame(transactions_data, schema=transaction_schema)
        products_df = self.spark.createDataFrame(products_data, schema=product_schema)
        expected_df = self.spark.createDataFrame(expected_data, schema=self.preferred_categories_schema)
        actual = preferred_categories(transactions_df, products_df)

        assertDataFrameEqual(actual, expected_df)

    def test_final_result(self):
        monthly_aggregates_data: Iterable[Any] = [
            ("C1", 1, 1, 150.0),
            ("C1", 2, 1, 200.0),
            ("C2", 1, 1, 75.0),
        ]
        monthly_aggregates_df = self.spark.createDataFrame(monthly_aggregates_data, schema=self.monthly_aggregates_schema)
        preferred_categories_data = [
            ("C1", "Appliances", 200.0),
            ("C2", "Electronics", 75.0),
        ]
        preferred_categories_df = self.spark.createDataFrame(preferred_categories_data, schema=self.preferred_categories_schema)
        customer_data = [
            ("C1", "John Doe", datetime.date.fromisoformat("2023-01-01")),
            ("C2", "Jane Smith", datetime.date.fromisoformat("2023-06-15")),
        ]
        customer_df = self.spark.createDataFrame(customer_data, schema=customer_schema)
        expected_data = [
            ("John Doe", datetime.date.fromisoformat("2023-01-01"), 175.0, 2, "Appliances"),
            ("Jane Smith", datetime.date.fromisoformat("2023-06-15"), 75.0, 1, "Electronics"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, schema=output_schema)
        actual = final_result(customer_df, monthly_aggregates_df, preferred_categories_df)

        assertDataFrameEqual(actual, expected_df)
