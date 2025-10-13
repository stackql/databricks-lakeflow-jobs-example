# Databricks notebook source
# MAGIC %md
# MAGIC # Clean and Transform Data - Data Transformation
# MAGIC This notebook performs additional cleaning and transformation (ELSE condition)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Read customers_sales_silver table
customers_sales_df = spark.table('customers_sales_silver')

# COMMAND ----------

# Perform additional transformations
transformed_df = customers_sales_df.select(
    col("customer_id"),
    col("customer_name"),
    col("loyalty_segment"),
    col("units_purchased"),
    col("state"),
    col("city"),
    col("product_name"),
    col("order_date"),
    col("total_price"),
    col("category"),
    # Add computed columns
    when(col("total_price") > 100, "High Value").otherwise("Standard").alias("price_category"),
    when(col("loyalty_segment") == "Gold", col("total_price") * 0.9)
    .when(col("loyalty_segment") == "Premium", col("total_price") * 0.95)
    .otherwise(col("total_price")).alias("discounted_price"),
    # Add customer tier based on units purchased
    when(col("units_purchased") >= 50, "Platinum")
    .when(col("units_purchased") >= 20, "Gold")
    .when(col("units_purchased") >= 10, "Silver")
    .otherwise("Bronze").alias("customer_tier")
)

# COMMAND ----------

# Update the silver table with transformations
transformed_df.write.mode('overwrite').saveAsTable('customers_sales_silver_transformed')

# COMMAND ----------

# Display sample of transformed data
display(transformed_df.limit(10))

# COMMAND ----------

print(f"Transformed data saved to customers_sales_silver_transformed with {transformed_df.count()} records")