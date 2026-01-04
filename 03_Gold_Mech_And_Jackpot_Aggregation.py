# Databricks notebook source
spark.conf.set("spark.sql.ansi.enabled", "false")


# COMMAND ----------

silver = spark.table("silver_mechanical_counters")


# COMMAND ----------

from pyspark.sql.functions import col

silver_filt = silver.filter(col("date") >= "2023-12-01")


# COMMAND ----------

from pyspark.sql import functions as F

mc_min_max = (
    silver_filt
    .groupBy("serial_no", "ID_CASINO")
    .agg(
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date")
    )
)


# COMMAND ----------

mc_min_max.show(5)


# COMMAND ----------

mc_init = (
    silver_filt.alias("s")
    .join(
        mc_min_max.alias("m"),
        (col("s.serial_no") == col("m.serial_no")) &
        (col("s.ID_CASINO") == col("m.ID_CASINO")) &
        (col("s.date") == col("m.min_date")),
        "inner"
    )
    .select(
        col("s.serial_no"),
        col("s.ID_CASINO"),
        col("s.total_in").alias("IN_INIT"),
        col("s.total_out").alias("OUT_INIT")
    )
)


# COMMAND ----------

mc_fin = (
    silver_filt.alias("s")
    .join(
        mc_min_max.alias("m"),
        (col("s.serial_no") == col("m.serial_no")) &
        (col("s.ID_CASINO") == col("m.ID_CASINO")) &
        (col("s.date") == col("m.max_date")),
        "inner"
    )
    .select(
        col("s.serial_no"),
        col("s.ID_CASINO"),
        col("s.total_in").alias("IN_FIN"),
        col("s.total_out").alias("OUT_FIN")
    )
)


# COMMAND ----------

mc_diff = (
    mc_init
    .join(mc_fin, ["serial_no", "ID_CASINO"], "inner")
    .withColumn("IN_DIFF", col("IN_FIN") - col("IN_INIT"))
    .withColumn("OUT_DIFF", col("OUT_FIN") - col("OUT_INIT"))
)


# COMMAND ----------

mc_diff.show(5)
mc_diff.count()


# COMMAND ----------

mc_diff_fixed = (
    mc_diff
    .withColumn(
        "IN_DIFF",
        F.when(col("IN_DIFF") < 0, 0).otherwise(col("IN_DIFF"))
    )
    .withColumn(
        "OUT_DIFF",
        F.when(col("OUT_DIFF") < 0, 0).otherwise(col("OUT_DIFF"))
    )
)


# COMMAND ----------

mc_diff_fixed.filter(col("IN_DIFF") < 0).count()
mc_diff_fixed.filter(col("OUT_DIFF") < 0).count()


# COMMAND ----------

gold_mech = (
    mc_diff_fixed
    .groupBy("ID_CASINO")
    .agg(
        F.sum("IN_DIFF").alias("INTRARI"),
        F.sum("OUT_DIFF").alias("IESIRI"),
        F.countDistinct("serial_no").alias("NR_SLOTURI")
    )
)


# COMMAND ----------

gold_mech.show(5)
gold_mech.count()


# COMMAND ----------

jackpot = spark.table("jackpot_history")


# COMMAND ----------

jackpot_total = (
    jackpot
    .filter(col("data") >= "2023-12-01")
    .groupBy("ID_CASINO")
    .agg(F.sum("value").alias("TOTAL_JACKPOT"))
)


# COMMAND ----------

gold_final = (
    gold_mech
    .join(jackpot_total, "ID_CASINO", "left")
    .fillna({"TOTAL_JACKPOT": 0})
)


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold_mech_and_jackpot")

gold_final.write.mode("overwrite").saveAsTable("gold_mech_and_jackpot")


# COMMAND ----------

spark.table("gold_mech_and_jackpot").show(10, truncate=False)
spark.table("gold_mech_and_jackpot").count()


# COMMAND ----------

gold_df = spark.table("gold_mech_and_jackpot")

gold_df.printSchema()
gold_df.count()


# COMMAND ----------

from pyspark.sql import functions as F

gold_final = (
    gold_df
    .fillna({
        "INTRARI": 0,
        "IESIRI": 0,
        "NR_SLOTURI": 0,
        "TOTAL_JACKPOT": 0
    })
)


# COMMAND ----------

gold_final.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in gold_final.columns
]).show()
