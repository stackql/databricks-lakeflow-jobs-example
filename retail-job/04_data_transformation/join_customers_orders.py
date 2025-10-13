# Databricks notebook source
# MAGIC %md
# MAGIC # Join Customers and Orders - Data Transformation
# MAGIC This notebook creates a comprehensive orders dataset by joining customers with orders

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Read bronze layer tables
customers_df = spark.table('customers_bronze')
orders_df = spark.table('orders_bronze')

# COMMAND ----------

# Create customers_orders_silver table with comprehensive information
customers_orders_df = customers_df.join(
    orders_df,
    customers_df.customer_id == orders_df.customer_id,
    "inner"
).select(
    customers_df.customer_id,
    customers_df.customer_name,
    customers_df.loyalty_segment,
    customers_df.state,
    customers_df.city,
    orders_df.order_id,
    orders_df.order_number,
    # Convert timestamp to unix timestamp for the conditional logic later
    unix_timestamp(orders_df.order_datetime).alias("order_datetime"),
    orders_df.number_of_line_items,
    orders_df.total_amount
)

# COMMAND ----------

# Write the joined data to silver layer
customers_orders_df.write.mode('overwrite').saveAsTable('customers_orders_silver')

# COMMAND ----------

# Display sample of the created table
display(customers_orders_df.limit(10))

# COMMAND ----------

print(f"Created customers_orders_silver table with {customers_orders_df.count()} records")