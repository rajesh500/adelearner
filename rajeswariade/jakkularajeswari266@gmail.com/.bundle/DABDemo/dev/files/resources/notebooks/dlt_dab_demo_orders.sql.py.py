# Databricks notebook source
# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC -- Create streaming table
# MAGIC CREATE OR REFRESH STREAMING TABLE st_orders
# MAGIC AS
# MAGIC SELECT * FROM STREAM(samples.tpch.orders)
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- Create MV
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW agg_orders
# MAGIC AS
# MAGIC SELECT
# MAGIC count(o_orderkey) as cnt_orders,
# MAGIC o_orderstatus
# MAGIC FROM LIVE.st_orders
# MAGIC group by o_orderstatus