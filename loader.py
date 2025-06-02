# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):
        pass

class AirpodsafteriPhoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF,
            path = "dbfs:/FileStore/newproject",
            method = "overwrite"
        ).load_dataframe()
