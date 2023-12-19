#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Combine CSV Files") \
    .getOrCreate()

# Read multiple CSV files from a directory into a single DataFrame
# Replace 's3://yourbucket/path/to/csvfiles' with the path to your CSV files
df = spark.read.csv('s3://mybucketnewcloud/cleaned_all_trips/*.csv', header=True, inferSchema=True)

# Perform any transformations or data cleaning here if necessary

# Write the DataFrame back to a single CSV file
# coalesce(1) is used to ensure the output is a single file
# Replace 's3://yourbucket/path/to/output/combined.csv' with your desired output path
df.coalesce(1).write.csv('s3://mybucketnewcloud/clean_trip_data.csv', header=True, mode='overwrite')

# Stop the SparkSession
spark.stop()

