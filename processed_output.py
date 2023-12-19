#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, col, count, avg, max, to_timestamp, hour, month, year, dayofweek

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bike Ride Comprehensive Analysis") \
    .getOrCreate()

# Load the dataset
# Replace 'path_to_your_file.csv' with the actual path to your dataset
df = spark.read.csv("s3://mybucketnewcloud/clean_trip_data.csv/part-00000-1d11b2e3-8730-43c7-aa7c-2bb5aa56e43e-c000.csv", header=True, inferSchema=True)

# Define the output directory
# Replace 'path_to_your_output_directory' with the desired output directory
output_dir = "s3://mybucketnewcloud/clean_trip_data.csv/processed_output/"

# Convert 'started_at' and 'ended_at' to timestamp
df = df.withColumn("started_at", to_timestamp("started_at", 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn("ended_at", to_timestamp("ended_at", 'yyyy-MM-dd HH:mm:ss'))

# Extract hour, month, year, and day from 'started_at'
df = df.withColumn("start_hour", hour("started_at"))
df = df.withColumn("start_month", month("started_at"))
df = df.withColumn("year", year("started_at"))
df = df.withColumn("start_day", dayofweek("started_at"))

# Analysis 1: Standard Deviation of Ride Durations by User Type and Year
std_dev_analysis = df.groupBy("year", "member_casual") \
                     .agg(stddev(col("ride_seconds")).alias("std_dev_duration")) \
                     .orderBy("year", "member_casual")
std_dev_analysis.write.csv(f"{output_dir}/std_dev_analysis.csv", header=True, mode='overwrite')

# Analysis 2: Count the Number of Rides
total_rides = df.count()

# Analysis 3: Average Duration of Rides (in seconds)
average_duration = df.select(avg("ride_seconds")).first()[0]

# Analysis 4: Number of Rides per Start Station
rides_per_station = df.groupBy("start_station_name").agg(count("ride_id").alias("number_of_rides"))
rides_per_station.write.csv(f"{output_dir}/rides_per_station.csv", header=True, mode='overwrite')

# Analysis 5: Rides per User Type
rides_per_user_type = df.groupBy("member_casual").agg(count("ride_id").alias("number_of_rides"))
rides_per_user_type.write.csv(f"{output_dir}/rides_per_user_type.csv", header=True, mode='overwrite')

# Analysis 6: Longest Ride (in seconds)
longest_ride = df.select(max("ride_seconds")).first()[0]

# Analysis 7: Rides per Year
rides_per_year = df.groupBy("year").agg(count("ride_id").alias("number_of_rides"))
rides_per_year.write.csv(f"{output_dir}/rides_per_year.csv", header=True, mode='overwrite')

# Analysis 8: Average Duration per Start Station (in seconds)
average_duration_per_station = df.groupBy("start_station_name").agg(avg("ride_seconds").alias("average_duration"))
average_duration_per_station.write.csv(f"{output_dir}/average_duration_per_station.csv", header=True, mode='overwrite')

# Print some of the results that are not DataFrames
print(f"Total number of rides: {total_rides}")
print(f"Average duration of rides (in seconds): {average_duration}")
print(f"Longest ride duration (in seconds): {longest_ride}")

# Stop the Spark session
spark.stop()

