#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, month, year, dayofweek, expr

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Data Cleaning on EMR") \
    .getOrCreate()

# Read the CSV file from S3
df = spark.read.csv("s3://mybucketnewcloud/all_trips_v2.csv", header=True, inferSchema=True)

# Data Inspection (optional)
df.printSchema()

# Dropping duplicates
df = df.dropDuplicates()

# Convert 'started_at' and 'ended_at' to timestamp
df = df.withColumn("started_at", to_timestamp("started_at", 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn("ended_at", to_timestamp("ended_at", 'yyyy-MM-dd HH:mm:ss'))

# Extract hour, month, year, and day from 'started_at'
df = df.withColumn("start_hour", hour("started_at"))
df = df.withColumn("start_month", month("started_at"))
df = df.withColumn("year", year("started_at"))
df = df.withColumn("start_day", dayofweek("started_at"))

# Converting day number to string for readability using expr
df = df.withColumn("start_day", 
                   expr("CASE start_day WHEN 1 THEN 'Sunday' " +
                                    "WHEN 2 THEN 'Monday' " +
                                    "WHEN 3 THEN 'Tuesday' " +
                                    "WHEN 4 THEN 'Wednesday' " +
                                    "WHEN 5 THEN 'Thursday' " +
                                    "WHEN 6 THEN 'Friday' " +
                                    "WHEN 7 THEN 'Saturday' END"))

# Removing older year data
df = df.filter(~df['year'].isin([2013, 2014, 2015]))

# Save the cleaned data back to S3
output_path = "s3://mybucketnewcloud/cleaned_all_trips/"
df.write.csv(output_path, header=True, mode='overwrite')

# Stop the SparkSession
spark.stop()

