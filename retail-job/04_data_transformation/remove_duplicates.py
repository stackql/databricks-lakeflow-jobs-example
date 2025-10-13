# Databricks notebook source
# MAGIC %md
# MAGIC # Remove Duplicates - Data Transformation
# MAGIC This notebook removes duplicates from the customers_sales_silver table (IF condition)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Read the customers_sales_silver table and remove duplicates
customers_sales_df = spark.sql('''
    SELECT DISTINCT * FROM customers_sales_silver
''')

# COMMAND ----------

# Overwrite the table with deduplicated data
customers_sales_df.write.mode('overwrite').saveAsTable('customers_sales_silver')

# COMMAND ----------

# Display sample of cleaned data
display(customers_sales_df.limit(10))

# COMMAND ----------

print(f"Removed duplicates. customers_sales_silver now has {customers_sales_df.count()} records")