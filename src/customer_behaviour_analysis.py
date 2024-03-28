from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

transaction_schema = StructType([
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


def preferred_categories(transactions_df: DataFrame, products_df: DataFrame) -> DataFrame:
    grouping_expr = [col('customer_id'), col('category')]
    window_spec = Window.partitionBy('customer_id').orderBy(col('total_amount').desc(), col('category'))
    return transactions_df.join(products_df, "product_id") \
        .groupBy(grouping_expr) \
        .agg(sum('amount').alias('total_amount')) \
        .withColumn('rank', row_number().over(window_spec)) \
        .filter(col('rank') == 1) \
        .drop('rank') \
        .withColumnRenamed('category', 'preferred_category')


def final_result(customer_df: DataFrame, monthly_aggregates_df: DataFrame, preferred_categories_df: DataFrame) -> DataFrame:
    global_aggregates = monthly_aggregates_df \
        .groupBy('customer_id') \
        .agg(avg('average_monthly_spending').alias('average_monthly_spending'), sum('purchase_frequency').cast(IntegerType()).alias('purchase_frequency'))

    return customer_df.join(global_aggregates, 'customer_id') \
        .join(preferred_categories_df, 'customer_id') \
        .select(output_schema.names)

