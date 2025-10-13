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

## Store the values from the text input widgets into variables
my_catalog = dbutils.widgets.get('catalog')
my_schema = dbutils.widgets.get('schema')


## Set path to your volume
my_volume_path = f"/Volumes/{my_catalog}/{my_schema}/trigger_storage_location/"


## Display the variables
print(my_catalog)
print(my_schema)
print(my_volume_path)

# COMMAND ----------

## Load the customers.csv file from the volume
customers_df = (spark
             .read
             .format("csv")
             .option("header", "true")
             .load(my_volume_path)
)
customers_df.write.mode('overwrite').saveAsTable(f'{my_catalog}.{my_schema}.customers_bronze')

# COMMAND ----------

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>