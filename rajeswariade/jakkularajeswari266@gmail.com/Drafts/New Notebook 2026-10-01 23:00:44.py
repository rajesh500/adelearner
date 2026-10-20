# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

EH_NAMESPACE = "eventhubdemo"
EH_NAME = "wiki-events"

connection_string = dbutils.secrets.get(
    scope="eventhub-demo",
    key="ehdbsecret101"
)

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers":
        f"{EH_NAMESPACE}.servicebus.windows.net:9093",

    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",

    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain."
        "PlainLoginModule required "
        'username="$ConnectionString" '
        f'password="{connection_string}";'
    )
}

# COMMAND ----------

test_df = (
    spark.read
    .format("kafka")
    .options(**kafka_options)
    .option("subscribe", EH_NAME)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

# COMMAND ----------

display(test_df)

# COMMAND ----------

CHECKPOINT_ROOT = f"/Volumes/rajeswariade/bronze/my_managed_volume/checkpoint/wiki/"

# COMMAND ----------

raw = (
    spark.readStream.format("kafka")
    .options(**kafka_options)
    .option("subscribe", EH_NAME)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
    .select(
        F.col("value").cast("string").alias("event_json"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("eventhub_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )
)

# COMMAND ----------

raw.show()

# COMMAND ----------

bronze_query = (
    raw.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze")
    .trigger(availableNow=True)
    .toTable("rajeswariade.bronze.wiki")
)