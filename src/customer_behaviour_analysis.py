from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

transactions_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("transaction_date", DateType()),
])

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
])

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("join_date", DateType()),
])

output_schema = StructType([
    StructField("customer_name", StringType()),
    StructField("join_date", DateType()),
    StructField("average_monthly_spending", DoubleType()),
    StructField("purchase_frequency", IntegerType()),
    StructField("preferred_category", StringType()),
])


def monthly_aggregates(transactions_df: DataFrame):
    grouping_expr = [col('customer_id'), month(col('transaction_date')).alias('month')]
    return transactions_df.groupBy(grouping_expr) \
        .agg(count('transaction_id').cast(IntegerType()).alias('purchase_frequency'), avg(col('amount')).alias('average_monthly_spending'))
