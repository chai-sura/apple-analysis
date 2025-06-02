# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract class
    """
    def __init__(self):
        pass

    def extract(self):
        pass


class AirpodsafteriPhoneExtractor(Extractor):
    
    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """
        transactionInputDF = get_data_source(
            data_type = "csv",
            file_path = "dbfs:/FileStore/newproject/Transaction_Updated.csv"
        ).get_dataframe()

        transactionInputDF.orderBy("customer_id", "transaction_date").show()

        customerInputDF = get_data_source(
            data_type = "delta",
            file_path = "default.customer_delta_table"
        ).get_dataframe()

        inputDF = {
            "transactionInputDF": transactionInputDF,
            "customerInputDF": customerInputDF

        }

        return inputDF

    
    
