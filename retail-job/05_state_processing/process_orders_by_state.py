# Databricks notebook source
# MAGIC %md
# MAGIC # Process Orders by State - State Processing
# MAGIC This notebook processes customer orders data for a specific state (FOR EACH loop)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Get state parameter from job widget
try:
    state = dbutils.widgets.get("state")
    print(f"Processing orders for state: {state}")
except:
    # Default for testing
    state = "CA"
    print(f"Using default state: {state}")

# COMMAND ----------

# Read the customers_orders_silver table and filter by state
df = spark.sql(f"""
    SELECT *,
            TO_DATE(FROM_UNIXTIME(order_datetime)) AS order_date,
            CASE 
                WHEN number_of_line_items > 2 THEN true 
                ELSE false 
            END AS is_large_order,
            CASE 
                WHEN total_amount > 200 THEN 'High Value'
                WHEN total_amount > 100 THEN 'Medium Value'
                ELSE 'Low Value'
            END AS order_value_category
    FROM customers_orders_silver
    WHERE state = '{state}'
""")

# COMMAND ----------

# Add additional state-specific metrics
state_processed_df = df.withColumn("processed_timestamp", current_timestamp()) \
                      .withColumn("processing_state", lit(state)) \
                      .withColumn("order_month", month("order_date")) \
                      .withColumn("order_year", year("order_date"))

# COMMAND ----------

# Save to state-specific Delta table
table_name = f"customers_orders_{state.replace(' ', '_').lower()}_silver"
state_processed_df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# Display processed data
display(state_processed_df.limit(20))

# COMMAND ----------

# Generate summary statistics
summary_df = state_processed_df.groupBy("loyalty_segment", "order_value_category") \
                              .agg(
                                  count("*").alias("order_count"),
                                  sum("total_amount").alias("total_revenue"),
                                  avg("total_amount").alias("avg_order_value"),
                                  sum("number_of_line_items").alias("total_items")
                              )

print(f"=== Order Summary for {state} ===")
summary_df.show()

# COMMAND ----------

print(f"Successfully processed {state_processed_df.count()} orders for state {state}")
print(f"Results saved to table: {table_name}")