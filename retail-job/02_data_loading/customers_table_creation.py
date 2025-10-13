# Databricks notebook source
# MAGIC %md
# MAGIC # Customers Table Creation - Data Loading
# MAGIC This notebook creates dummy customer data with configurable catalog and schema

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *

# COMMAND ----------

# Get configuration from job parameters or use defaults
try:
    my_catalog = dbutils.widgets.get('catalog')
    my_schema = dbutils.widgets.get('schema')
except:
    # Default values for standalone testing
    my_catalog = "hive_metastore"
    my_schema = "default"

# Display the configuration
print(f"Using catalog: {my_catalog}")
print(f"Using schema: {my_schema}")

# COMMAND ----------

# Create dummy customer data with state information
customers_data = [
    (101, "Alice Johnson", "Premium", 25, "CA", "Los Angeles"),
    (102, "Bob Smith", "Standard", 12, "NY", "New York"),
    (103, "Carol Davis", "Premium", 18, "TX", "Houston"),
    (104, "David Wilson", "Gold", 42, "FL", "Miami"),
    (105, "Eva Brown", "Standard", 8, "WA", "Seattle"),
    (106, "Frank Miller", "Premium", 33, "IL", "Chicago"),
    (107, "Grace Lee", "Gold", 67, "CA", "San Francisco"),
    (108, "Henry Taylor", "Standard", 15, "TX", "Austin"),
    (109, "Ivy Chen", "Premium", 29, "NY", "Brooklyn"),
    (110, "Jack Robinson", "Gold", 51, "FL", "Orlando")
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("loyalty_segment", StringType(), True),
    StructField("units_purchased", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)

# COMMAND ----------

# Write to the configured catalog and schema
table_name = f'{my_catalog}.{my_schema}.customers_bronze'
customers_df.write.mode('overwrite').saveAsTable(table_name)

# Also write to default location for backward compatibility
customers_df.write.mode('overwrite').saveAsTable('customers_bronze')

# COMMAND ----------

# Display the created data
display(customers_df)

# COMMAND ----------

print(f"Created {customers_df.count()} customer records in {table_name} table")