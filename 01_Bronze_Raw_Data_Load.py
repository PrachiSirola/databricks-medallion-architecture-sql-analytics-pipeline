# Databricks notebook source
spark.conf.set("spark.sql.ansi.enabled", "false")


# COMMAND ----------

mc_2023_raw = spark.table("mechanical_counters_2023")
mc_2024_raw = spark.table("mechanical_counters_2024")
mc_2025_raw = spark.table("mechanical_counters_2025")


# COMMAND ----------

def to_all_string(df):
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])

from pyspark.sql import functions as F

mc_2023 = to_all_string(mc_2023_raw)
mc_2024 = to_all_string(mc_2024_raw)
mc_2025 = to_all_string(mc_2025_raw)


# COMMAND ----------

mc_bronze = (
    mc_2023
    .unionByName(mc_2024)
    .unionByName(mc_2025)
)


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS bronze_mechanical_counters")

mc_bronze.write.mode("overwrite").saveAsTable("bronze_mechanical_counters")


# COMMAND ----------

spark.table("bronze_mechanical_counters").count()
spark.table("bronze_mechanical_counters").show(5)
