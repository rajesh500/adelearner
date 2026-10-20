# Databricks notebook source
from pyspark.sql import functions as F

num_rows = 10_000_000

df = (
    spark.range(0, num_rows, numPartitions=100)
    .withColumn(
        "customer_id",
        (F.col("id") % 2_000_000)
    )
    .withColumn(
        "amount",
        F.rand() * 1000
    )
    .withColumn(
        "random_value",
        F.rand()
    )
    .withColumn(
        "payload",
        F.concat(
            F.lit("transaction_"),
            F.col("id").cast("string"),
            F.lit("_ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        )
    )
)

display(df.limit(10))


df.select(F.spark_partition_id().alias("partition_id")) \
  .groupBy("partition_id") \
  .count() \
  .orderBy("partition_id") \
  .show(200, False)


sorted_df = df.orderBy("random_value")


(
    sorted_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("spill_test")
)