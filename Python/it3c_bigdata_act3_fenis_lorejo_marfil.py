"""
YOUR DATA? OUR DATA!
BUCS IT3C - Big Data Analysis
Laboratory Activity 3

FENIS, Austin B.
LOREJO, Jerome M.
MARFIL, John Marvin G.
"""

import csv
from io import StringIO
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Our_Data").getOrCreate()
sc = spark.sparkContext

yourData = sc.textFile("customers-1000.csv")
header = yourData.first()
ourData = yourData.filter(lambda x: x != header)

"""
[1ST PARTITIONING STRATEGY]
::  'Repartitioning"
::  Since this 1000-row dataset involves customer subscription
and assuming that subscriptions expire after 5 years, we filtered
the dataset to remove customers whose subscription dates were older
than March 6, 2021, five years before March 6, 2026.
"""

# Partitioning the dataset into 10 then directly filtering active subscribers
ourData = ourData\
  .repartition(10)\
  .map(lambda x: list(csv.reader(StringIO(x), skipinitialspace=True))[0])\
  .filter(lambda x: len(x) > 10 and x[10].strip() >= "2021-03-06")

for row in ourData.collect():
    print(row)

print("\n\nNo. of partitions:", ourData.getNumPartitions())
print("Active subscribers: " + str(ourData.count()), "\n----------\n\n\n\n")


"""
[2ND PARTITIONING STRATEGY]
::  'Hash Partitioning"
::  The already partitioned dataset is once again partitioned
according to the countries of each customer, with the number of
countries as the number of partitioning (not counting their duplicates).
Upon repartitioning, all customers based in the Philippines are then
located and counted, thus finding the number of active subscribers
from the Philippines.
"""

# Counting the number of countries.
num_countries = ourData\
  .map(lambda x: x[6])\
  .distinct()\
  .count()

# Repartitioning the dataset by country.
ourData = ourData\
  .map(lambda x: (x[6], x))\
  .sortByKey(numPartitions = num_countries)

# Counting the active subscribers from the Philippines
subs_phil = ourData.filter(lambda x: x[0] == "Philippines").collect()

for country, row in subs_phil:
    print(row)

print("\n\nNo. of partitions:", ourData.getNumPartitions())
print("Active subscribers from the Philippines: ", len(subs_phil), "\n----------\n")

spark.stop()
