# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Table Creation - Data Ingestion
# MAGIC This notebook creates dummy sales data and saves it to the bronze layer

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

# Create dummy sales data
sales_data = [
    (2001, 101, "Laptop Pro 15", "2024-01-15", 1299.99, "Electronics"),
    (2002, 102, "Wireless Headphones", "2024-01-16", 89.50, "Electronics"),
    (2003, 103, "Office Chair", "2024-01-17", 156.75, "Furniture"),
    (2004, 104, "Standing Desk", "2024-01-18", 423.20, "Furniture"),
    (2005, 105, "Coffee Mug", "2024-01-19", 12.99, "Home"),
    (2006, 106, "Gaming Mouse", "2024-01-20", 65.45, "Electronics"),
    (2007, 107, "Monitor 27inch", "2024-01-21", 345.67, "Electronics"),
    (2008, 108, "Desk Lamp", "2024-01-22", 34.90, "Home"),
    (2009, 109, "Keyboard Mechanical", "2024-01-23", 123.56, "Electronics"),
    (2010, 110, "Ergonomic Cushion", "2024-01-24", 45.89, "Home"),
    (2011, 101, "USB-C Hub", "2024-01-25", 78.99, "Electronics"),
    (2012, 103, "Notebook Set", "2024-01-26", 23.45, "Office"),
    (2013, 105, "Water Bottle", "2024-01-27", 19.99, "Home"),
    (2014, 107, "Webcam HD", "2024-01-28", 89.99, "Electronics"),
    (2015, 109, "Phone Stand", "2024-01-29", 15.99, "Electronics")
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("category", StringType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)

# Convert string date to date type
sales_df = sales_df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# COMMAND ----------

# Write to bronze layer
sales_df.write.mode('overwrite').saveAsTable('sales_bronze')

# COMMAND ----------

# Display the created data
display(sales_df)

# COMMAND ----------

print(f"Created {sales_df.count()} sales records in sales_bronze table")