# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-4

# COMMAND ----------

#Getting state code from widget
state = dbutils.widgets.get("state")
print(f"Running for state: {state}")

# COMMAND ----------

# Reading and transforming data from master customers_orders_silver table
## Tranforming order data from unix timestamp to date and adding is_large_order column

df = spark.sql(f"""
    SELECT *,
            TO_DATE(FROM_UNIXTIME(order_datetime)) AS order_date,
            CASE WHEN number_of_line_items > 2 THEN true ELSE false END AS is_large_order
    FROM customers_orders_silver
    WHERE state = '{state}'
""")

# Save to Delta table, one per state specified in the loop:
table_name = f"customers_orders_{state.replace(' ', '_').lower()}_silver"
df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>