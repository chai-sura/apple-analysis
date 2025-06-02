# Databricks notebook source
# MAGIC %run "./transformer"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class WorkFlow:
    def __init__(self):
        pass

    def runner(self):

        #Extract all required data from different source
        inputDF = AirpodsafteriPhoneExtractor().extract()

        #Transformation 
        #Customers who bought Airpods after iPhone
        firstTransformedDF = FirstTransformer().transform(
            inputDF
        )

        #Load
        AirpodsafteriPhoneLoader(firstTransformedDF).sink()





workflow = WorkFlow().runner()

# COMMAND ----------

spark.stop()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("newbigdatashow").getOrCreate()

# COMMAND ----------

input_df = spark.read.format("csv").option("header", True).load("dbfs:/FileStore/newproject/Transaction_Updated.csv")

input_df.show()