# Databricks notebook source
class DataSink:
    """
    Abstract Class
    """

    def __init__(self, df, path, method, params = None):
        self.df = df
        self.path = path
        self.method = method
        self.params = params

    def load_dataframe(self):
        """
        Abstract method, FUnction will be defined in sub classes
        """

        raise ValueError("Not Implemented")

class LoadtoDBFS(DataSink):

    def load_dataframe(self):

        self.df.write.mode(self.method).save(self.path)


class LoadtoDBFSwithPartition(DataSink):

    def load_dataframe(self):

        partitionByColumn = self.params.get("partitionByColumn")
        self.df.write.mode(self.method).partitionBy(*partitionByColumn).save(self.path)

class LoadtoDeltaTable(DataSink):

    def load_dataframe(self):

        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)

def get_sink_source(sink_type, df, path, method, params = None):
    if sink_type == "dbfs":
        return LoadtoDBFS(df, path, method, params)
    elif sink_type == "dbfs_with_partition":
        return LoadtoDBFSwithPartition(df, path, method, params)
    elif sink_type == "delta":
        return LoadtoDeltaTable(df, path, method, params)
    else:
        return ValueError(f"Not implemented for sink_type: {sink_type}")

