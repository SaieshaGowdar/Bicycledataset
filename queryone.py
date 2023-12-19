#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, to_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bike Ride Yearly Analysis") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv("s3://mybucketnewcloud/clean_trip_data.csv/part-00000-1d11b2e3-8730-43c7-aa7c-2bb5aa56e43e-c000.csv", header=True, inferSchema=True)

# Convert 'started_at' to timestamp and extract year
df = df.withColumn("started_at", to_timestamp("started_at", 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn("year", year("started_at"))

# Analysis for the year 2020
df_2020 = df.filter(df['year'] == 2020)
total_rides_2020 = df_2020.count()
avg_duration_2020 = df_2020.select(avg("ride_seconds")).first()[0]

# Analysis for the year 2021
df_2021 = df.filter(df['year'] == 2021)
total_rides_2021 = df_2021.count()
avg_duration_2021 = df_2021.select(avg("ride_seconds")).first()[0]

# Analysis for the year 2022
df_2022 = df.filter(df['year'] == 2022)
total_rides_2022 = df_2022.count()
avg_duration_2022 = df_2022.select(avg("ride_seconds")).first()[0]

# Display the results
print(f"Total number of rides in 2020: {total_rides_2020}")
print(f"Average duration of rides in 2020 (in seconds): {avg_duration_2020}")
print(f"Total number of rides in 2021: {total_rides_2021}")
print(f"Average duration of rides in 2021 (in seconds): {avg_duration_2021}")
print(f"Total number of rides in 2022: {total_rides_2022}")
print(f"Average duration of rides in 2022 (in seconds): {avg_duration_2022}")

# Stop the Spark session
spark.stop()

