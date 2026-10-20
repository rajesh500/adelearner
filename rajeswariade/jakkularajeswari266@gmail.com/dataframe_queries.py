# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
df = spark.read.table("rajeswariade.bronze.customer_sales")
df.show()

# COMMAND ----------

from pyspark.sql import functions as f 
from pyspark.sql import Window

# COMMAND ----------

df2 = df.withColumn("order_year", f.extract(f.lit("year"), "order_date"))
df2.show()

# COMMAND ----------

df3 = df2.groupBy("customer_id", "order_year").agg(f.sum("amount")).withColumnRenamed("sum(amount)", "total_amount")
df3.show()

# COMMAND ----------

df4 = df3.withColumn("current_year", f.lead("total_amount", 1).over(Window.partitionBy("customer_id").orderBy("customer_id", "order_year"))).filter(f.col("current_year").isNotNull())
df4.show()

# COMMAND ----------

df5 = df4.withColumn("higher", f.when(f.col("current_year")>f.col("total_amount"), f.lit(1)).otherwise(f.lit(0)))
df5.filter(df5.higher ==1).show()