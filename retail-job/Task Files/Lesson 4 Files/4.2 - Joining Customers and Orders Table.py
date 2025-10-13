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
# MAGIC CREATE OR REPLACE TABLE customers_orders_silver AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.customer_name,
# MAGIC   c.state,
# MAGIC   c.city,
# MAGIC   o.order_number,
# MAGIC   o.order_datetime,
# MAGIC   o.number_of_line_items,
# MAGIC   o.promo_info
# MAGIC FROM
# MAGIC   customers_bronze c
# MAGIC JOIN
# MAGIC   orders_bronze o
# MAGIC ON
# MAGIC   c.customer_id = o.customer_id;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>