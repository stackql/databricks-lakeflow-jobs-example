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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers_sales_silver AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.customer_name,
# MAGIC   c.loyalty_segment,
# MAGIC   c.units_purchased,
# MAGIC   s.product_name,
# MAGIC   s.order_date,
# MAGIC   s.total_price
# MAGIC FROM
# MAGIC   customers_bronze c
# MAGIC JOIN
# MAGIC   sales_bronze s
# MAGIC ON
# MAGIC   c.customer_id = s.customer_id;
# MAGIC

# COMMAND ----------

df = spark.sql("""
    SELECT * FROM customers_sales_silver
""")

# Check for duplicate records
duplicate_exists = df.count() > df.dropDuplicates().count()

# Set boolean flag in task values
dbutils.jobs.taskValues.set(key="has_duplicates", value=duplicate_exists)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>