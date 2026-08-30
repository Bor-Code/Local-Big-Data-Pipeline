from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, count, avg, sum

spark = SparkSession.builder \
    .appName("NYCTaxiDataCleaning") \
    .getOrCreate()

df = spark.read.parquet("/opt/spark/data/raw/yellow_tripdata_2024-01.parquet")

df_filtered = df.filter(
    (col("passenger_count") > 0) & 
    (col("trip_distance") > 0.0) & 
    (col("total_amount") > 0.0)
)

df_daily_summary = df_filtered.withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
    .groupBy("pickup_date") \
    .agg(
        count("*").alias("total_trips"),
        avg("trip_distance").alias("avg_distance")
    ) \
    .orderBy("pickup_date")

df_hourly_revenue = df_filtered.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .groupBy("pickup_hour") \
    .agg(
        sum("total_amount").alias("total_revenue")
    ) \
    .orderBy("pickup_hour")

df_filtered.show(5)
df_daily_summary.show(5)
df_hourly_revenue.show(5)

spark.stop()