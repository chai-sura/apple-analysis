# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast

# COMMAND ----------

class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDF):
        pass

class FirstTransformer(Transformer):

    def transform(self, inputDF):
        """
        customers who have bought Airpods after buying Iphone
        """

        transactionInputDF = inputDF.get("transactionInputDF")

        print("transactionInputDF in transformation")

        transactionInputDF.show()


        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transactionInputDF.withColumn("next_product", lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iphone")

        transformedDF.orderBy("customer_id", "transaction_date", "product_name").show()


        filteredDF = transformedDF.filter(
            (col("product_name") == "iPhone") & (col("next_product") == "AirPods"))
        

        filteredDF.orderBy("customer_id", "transaction_date", "product_name").show()

        customerInputDF = inputDF.get("customerInputDF")

        customerInputDF.show()

        joinDF = customerInputDF.join(broadcast(filteredDF), "customer_id")

        print("Joined DF")
        display(joinDF.select(
            "customer_id", "customer_name", "location"
        
        ))

        return joinDF
