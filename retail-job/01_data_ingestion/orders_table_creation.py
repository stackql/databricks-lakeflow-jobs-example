# Databricks notebook source
# MAGIC %md
# MAGIC # Orders Table Creation - Data Ingestion
# MAGIC This notebook creates dummy orders data and saves it to the bronze layer

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

# Create dummy orders data
orders_data = [
    (1001, "ORD-001", 101, "2024-01-15 10:30:00", 3, 299.99),
    (1002, "ORD-002", 102, "2024-01-16 14:22:00", 1, 89.50),
    (1003, "ORD-003", 103, "2024-01-17 09:15:00", 2, 156.75),
    (1004, "ORD-004", 104, "2024-01-18 16:45:00", 4, 423.20),
    (1005, "ORD-005", 105, "2024-01-19 11:30:00", 1, 67.99),
    (1006, "ORD-006", 106, "2024-01-20 13:20:00", 2, 198.45),
    (1007, "ORD-007", 107, "2024-01-21 08:45:00", 3, 345.67),
    (1008, "ORD-008", 108, "2024-01-22 15:10:00", 1, 78.90),
    (1009, "ORD-009", 109, "2024-01-23 12:35:00", 2, 234.56),
    (1010, "ORD-010", 110, "2024-01-24 17:20:00", 5, 567.89)
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_number", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_datetime", StringType(), True),
    StructField("number_of_line_items", IntegerType(), True),
    StructField("total_amount", DoubleType(), True)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)

# Convert string datetime to timestamp
orders_df = orders_df.withColumn("order_datetime", to_timestamp(col("order_datetime"), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# Write to bronze layer
orders_df.write.mode('overwrite').saveAsTable('orders_bronze')

# COMMAND ----------

# Display the created data
display(orders_df)

# COMMAND ----------

print(f"Created {orders_df.count()} orders records in orders_bronze table")