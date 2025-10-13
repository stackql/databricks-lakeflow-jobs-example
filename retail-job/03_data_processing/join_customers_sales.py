# Databricks notebook source
# MAGIC %md
# MAGIC # Join Customers and Sales - Data Processing
# MAGIC This notebook joins customer and sales data, and checks for duplicates

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Read bronze layer tables
customers_df = spark.table('customers_bronze')
sales_df = spark.table('sales_bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers_sales_silver AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.customer_name,
# MAGIC   c.loyalty_segment,
# MAGIC   c.units_purchased,
# MAGIC   c.state,
# MAGIC   c.city,
# MAGIC   s.product_name,
# MAGIC   s.order_date,
# MAGIC   s.total_price,
# MAGIC   s.category
# MAGIC FROM
# MAGIC   customers_bronze c
# MAGIC JOIN
# MAGIC   sales_bronze s
# MAGIC ON
# MAGIC   c.customer_id = s.customer_id;

# COMMAND ----------

# Read the newly created silver table
df = spark.sql("""
    SELECT * FROM customers_sales_silver
""")

# Check for duplicate records
original_count = df.count()
deduplicated_count = df.dropDuplicates().count()
duplicate_exists = original_count > deduplicated_count

print(f"Original count: {original_count}")
print(f"Deduplicated count: {deduplicated_count}")
print(f"Duplicates exist: {duplicate_exists}")

# Set boolean flag in task values for conditional logic
dbutils.jobs.taskValues.set(key="has_duplicates", value=duplicate_exists)

# COMMAND ----------

# Display sample of joined data
display(df.limit(10))

# COMMAND ----------

print(f"Join completed. Created customers_sales_silver table with {df.count()} records")