# Databricks notebook source
storage_account = "stcarbondatalake02"



# COMMAND ----------

df = spark.read.csv(
    "abfss://raw@stcarbondatalake02.dfs.core.windows.net/energy_input/",
    header=True,
    inferSchema=True
)

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df = spark.read.csv(
    "abfss://raw@stcarbondatalake02.dfs.core.windows.net/energy_input/",
    header=True,
    inferSchema=True
).withColumn(
    "load_timestamp", current_timestamp()
).withColumn(
    "source_file", input_file_name()
)

# COMMAND ----------

from pyspark.sql.functions import when, col

df_dq = df.withColumn(
    "dq_status",
    when(col("energy_kwh").isNull(), "invalid")
    .when(col("gas_kwh").isNull(), "invalid")
    .when(col("temperature_c") < -30, "invalid")
    .when(col("occupancy").isNull(), "invalid")
    .otherwise("valid")
)

# COMMAND ----------

df_dq = df_dq.withColumn(
    "dq_reason",
    when(col("energy_kwh").isNull(), "missing_energy")
    .when(col("gas_kwh").isNull(), "missing_gas")
    .when(col("temperature_c") < -30, "invalid_temperature")
    .when(col("energy_kwh") > 1000, "energy_outlier")
    .when(col("occupancy").isNull(), "missing_occupancy")
    .otherwise("clean")
)

# COMMAND ----------

df_dq = df_dq.withColumn(
    "dq_reason",
    when(col("energy_kwh").isNull(), "missing_energy")
    .when(col("gas_kwh").isNull(), "missing_gas")
    .when(col("temperature_c") < -30, "invalid_temperature")
    .when(col("energy_kwh") > 1000, "energy_outlier")
    .when(col("occupancy").isNull(), "missing_occupancy")
    .otherwise("clean")
)

# COMMAND ----------

df_valid = df_dq.filter(col("dq_status") == "valid")
df_invalid = df_dq.filter(col("dq_status") == "invalid")

# COMMAND ----------

print("valid:", df_valid.count())
print("invalid:", df_invalid.count())

# COMMAND ----------

df_valid.write.mode("overwrite").parquet(
    "abfss://silver@stcarbondatalake02.dfs.core.windows.net/energy_clean/"
)

# COMMAND ----------

df_invalid.write.mode("overwrite").parquet(
    "abfss://quarantine@stcarbondatalake02.dfs.core.windows.net/energy_rejected/"
)

# COMMAND ----------

silver_check = spark.read.parquet(
    "abfss://silver@stcarbondatalake02.dfs.core.windows.net/energy_clean/"
)

quarantine_check = spark.read.parquet(
    "abfss://quarantine@stcarbondatalake02.dfs.core.windows.net/energy_rejected/"
)

print("silver:", silver_check.count())
print("quarantine:", quarantine_check.count())

# COMMAND ----------

from pyspark.sql.functions import sum, avg

df_gold = df_valid.groupBy("building_id", "reading_date") \
    .agg(
        sum("energy_kwh").alias("total_energy_kwh"),
        sum("gas_kwh").alias("total_gas_kwh"),
        avg("temperature_c").alias("avg_temperature_c"),
        avg("occupancy").alias("avg_occupancy")
    )

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_gold.write \
    .mode("overwrite") \
    .partitionBy("reading_date") \
    .parquet("abfss://gold@stcarbondatalake02.dfs.core.windows.net/site_daily_summary/")

# COMMAND ----------

gold_check = spark.read.parquet(
    "abfss://gold@stcarbondatalake02.dfs.core.windows.net/site_daily_summary/"
)

gold_check.show()
print("gold count:", gold_check.count())

# COMMAND ----------

from pyspark.sql.functions import mean, stddev

stats = df_valid.groupBy("building_id").agg(
    mean("energy_kwh").alias("mean_energy"),
    stddev("energy_kwh").alias("std_energy")
)

# COMMAND ----------

df_anomaly = df_valid.join(stats, on="building_id", how="left")

# COMMAND ----------

from pyspark.sql.functions import abs

df_anomaly = df_anomaly.withColumn(
    "z_score",
    (col("energy_kwh") - col("mean_energy")) / col("std_energy")
)

# COMMAND ----------

df_anomaly = df_anomaly.withColumn(
    "anomaly_flag",
    when(abs(col("z_score")) > 2, 1).otherwise(0)
)

# COMMAND ----------

from pyspark.sql.functions import expr

median_df = df_valid.groupBy("building_id").agg(
    expr("percentile_approx(energy_kwh, 0.5)").alias("median_energy")
)

# COMMAND ----------

df_anomaly = df_valid.join(median_df, on="building_id", how="left")

# COMMAND ----------

from pyspark.sql.functions import abs

df_anomaly = df_anomaly.withColumn(
    "abs_dev",
    abs(col("energy_kwh") - col("median_energy"))
)

# COMMAND ----------

mad_df = df_anomaly.groupBy("building_id").agg(
    expr("percentile_approx(abs_dev, 0.5)").alias("mad")
)

# COMMAND ----------

df_anomaly = df_anomaly.join(mad_df, on="building_id", how="left")

# COMMAND ----------

df_anomaly = df_anomaly.withColumn(
    "robust_z",
    (col("energy_kwh") - col("median_energy")) / col("mad")
)

# COMMAND ----------

df_anomaly = df_anomaly.withColumn(
    "anomaly_flag",
    when(abs(col("robust_z")) > 3, 1).otherwise(0)
)

# COMMAND ----------

from pyspark.sql.functions import when

df_anomaly = df_anomaly.withColumn(
    "robust_z",
    when(col("mad") == 0, 0)
    .otherwise((col("energy_kwh") - col("median_energy")) / col("mad"))
)

# COMMAND ----------

from pyspark.sql.functions import when, col

df_anomaly = df_valid.withColumn(
    "anomaly_flag",
    when(col("energy_kwh") > 1000, 1).otherwise(0)
)

df_anomaly.select("building_id", "energy_kwh", "anomaly_flag").show()

# COMMAND ----------

df_anomaly.write.mode("overwrite").parquet(
    "abfss://gold@stcarbondatalake02.dfs.core.windows.net/anomaly_detection/"
)

# COMMAND ----------

anomaly_check = spark.read.parquet(
    "abfss://gold@stcarbondatalake02.dfs.core.windows.net/anomaly_detection/"
)

anomaly_check.show()

# COMMAND ----------

df.select("site_id", "building_id", "source_file", "load_timestamp").show(truncate=False)

# COMMAND ----------

df_valid.select("building_id", "source_file", "load_timestamp").show(truncate=False)

# COMMAND ----------

df_invalid.select("building_id", "source_file", "load_timestamp").show(truncate=False)