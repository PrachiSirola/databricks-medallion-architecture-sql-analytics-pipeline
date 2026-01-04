# Databricks notebook source
spark.conf.set("spark.sql.ansi.enabled", "false")


# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("bronze_mechanical_counters")


# COMMAND ----------

silver_raw = bronze.select(
    F.col("serial_no"),
    F.col("ID_CASINO"),
    F.to_date("date").alias("date"),
    F.col("total_in").cast("string").alias("total_in_str"),
    F.col("total_out").cast("string").alias("total_out_str")
)


# COMMAND ----------

silver_clean_str = silver_raw.select(
    "serial_no",
    "ID_CASINO",
    "date",
    F.regexp_replace("total_in_str", "[^0-9]", "").alias("total_in_str"),
    F.regexp_replace("total_out_str", "[^0-9]", "").alias("total_out_str")
)


# COMMAND ----------

silver_numbers = silver_clean_str.select(
    "serial_no",
    "ID_CASINO",
    "date",
    F.expr("try_cast(total_in_str as BIGINT)").alias("total_in"),
    F.expr("try_cast(total_out_str as BIGINT)").alias("total_out")
)


# COMMAND ----------

silver_valid = silver_numbers.filter(
    F.col("serial_no").isNotNull() &
    F.col("ID_CASINO").isNotNull() &
    F.col("date").isNotNull() &
    F.col("total_in").isNotNull() &
    F.col("total_out").isNotNull()
)


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver_mechanical_counters")

silver_valid.write.mode("overwrite").saveAsTable("silver_mechanical_counters")


# COMMAND ----------

spark.table("silver_mechanical_counters").count()
spark.table("silver_mechanical_counters").show(5)


# COMMAND ----------

from pyspark.sql.functions import col

spark.table("silver_mechanical_counters").filter(
    col("serial_no").isNull() |
    col("ID_CASINO").isNull() |
    col("date").isNull() |
    col("total_in").isNull() |
    col("total_out").isNull()
).count()
