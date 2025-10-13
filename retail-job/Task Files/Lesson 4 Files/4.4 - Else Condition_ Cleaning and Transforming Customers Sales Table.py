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

# MAGIC %run ../../Includes/Classroom-Setup-6

# COMMAND ----------

### --- Data Cleaning ---

# Strip whitespace from column names
df = spark.sql("select * from customers_sales_silver")
df = df.toDF(*[col.strip() for col in df.columns])

# Standardize customer names to title case and strip whitespace
from pyspark.sql.functions import trim, initcap, col

df = df.withColumn("customer_name", initcap(trim(col("customer_name"))))

# Convert data types for IDs, numeric columns, and dates
from pyspark.sql.functions import to_date

df = df.withColumn("customer_id", col("customer_id").cast("long")) \
       .withColumn("units_purchased", col("units_purchased").cast("double")) \
       .withColumn("total_price", col("total_price").cast("double")) \
       .withColumn("order_date", to_date(col("order_date")))


# Drop rows with missing values after conversions
df = df.dropna(subset=["customer_id", "units_purchased", "total_price", "order_date"])

# COMMAND ----------

# --- Feature Engineering ---

from pyspark.sql.functions import year, month, date_format, round as spark_round, col

# Extract order year and order month for time-series analyses
df = df.withColumn("order_year", year(col("order_date"))) \
       .withColumn("order_month", date_format(col("order_date"), "MM"))

# Calculate average price per unit and round to 2 decimals
df = df.withColumn("avg_price_per_unit", spark_round(col("total_price") / col("units_purchased"), 2))

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("customers_sales_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>