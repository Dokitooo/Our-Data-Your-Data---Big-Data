"""
YOUR DATA? OUR DATA!
BUCS IT3C - Big Data Analysis
Laboratory Activity 3 - Refactored for DataFrame API

FENIS, Austin B.
LOREJO, Jerome M.
MARFIL, John Marvin G.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, desc, to_date

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Our_Data_Refactored") \
    .getOrCreate()

# 2. Load Dataset
df = spark.read.csv("customers-1000.csv", header=True, inferSchema=True)

# ---------------------------------------------------------
# STRATEGY 1: Repartitioning & Active Subscriber Filtering
# Logic: Subscriptions are active if Date >= 2021-03-06 (5 years before today).
# ---------------------------------------------------------
# We cast to date type first to ensure accurate comparison
df_active = df.withColumn("Subscription Date", to_date(col("Subscription Date"))) \
    .repartition(10) \
    .filter(col("Subscription Date") >= "2021-03-06")

print(f"Total Active Subscribers: {df_active.count()}")
print(f"Number of partitions (Strategy 1): {df_active.rdd.getNumPartitions()}")

# ---------------------------------------------------------
# STRATEGY 2: Hash Partitioning (by Country)
# Logic: Partitioning by Country to optimize searches for specific regions.
# ---------------------------------------------------------
num_countries = df_active.select("Country").distinct().count()
df_partitioned_country = df_active.repartition(num_countries, "Country")

print(f"Number of partitions by Country: {df_partitioned_country.rdd.getNumPartitions()}")

# ---------------------------------------------------------
# TRANSFORMATION PIPELINE: Philippines Focus
# ---------------------------------------------------------
# Step 1: Filter specifically for Philippines
# Step 2: Extract Year from Subscription Date
# Step 3: Group by Year to see subscription trends in the PH
# Step 4: Sort by Year descending
ph_summary = df_partitioned_country \
    .filter(col("Country") == "Philippines") \
    .withColumn("Sub_Year", year(col("Subscription Date"))) \
    .groupBy("Country", "Sub_Year") \
    .agg(count("Customer Id").alias("Total_PH_Subscribers")) \
    .orderBy(desc("Sub_Year"))

# Show the results for the Philippines
ph_summary.show()

# Save the partitioned data to disk
# This organizes the files by Country for faster querying later
ph_summary.write.partitionBy("Country").mode("overwrite").parquet("active_subs_ph_summary")

print("Processing Complete. Data saved to parquet.")

spark.stop()