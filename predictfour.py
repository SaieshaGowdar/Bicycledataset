#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, month, year, dayofweek
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bike Ride Prediction Model") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv("s3://mybucketnewcloud/clean_trip_data.csv/part-00000-1d11b2e3-8730-43c7-aa7c-2bb5aa56e43e-c000.csv", header=True, inferSchema=True)

# Convert 'started_at' to timestamp and extract features
df = df.withColumn("started_at", to_timestamp("started_at", 'yyyy-MM-dd HH:mm:ss'))
df = df.withColumn("start_hour", hour("started_at"))
df = df.withColumn("start_month", month("started_at"))
df = df.withColumn("start_day", dayofweek("started_at"))

# Select relevant columns
df = df.select("member_casual", "start_hour", "start_month", "start_day")

# StringIndexer for the label column
label_indexer = StringIndexer(inputCol="member_casual", outputCol="label").fit(df)

# VectorAssembler to combine feature columns into a single vector column
assembler = VectorAssembler(inputCols=["start_hour", "start_month", "start_day"], outputCol="features")

# Define the Random Forest model with increased maxBins
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10, maxBins=900)

# Pipeline for the tasks
pipeline = Pipeline(stages=[label_indexer, assembler, rf])

# Split the data into training and test sets
(trainingData, testData) = df.randomSplit([0.7, 0.3])

# Train the model
model = pipeline.fit(trainingData)

# Make predictions
predictions = model.transform(testData)

# Select example rows to display
predictions.select("prediction", "label", "features").show(5)

# Evaluate the model
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Accuracy = %g" % (accuracy))

# Stop the Spark session
spark.stop()

