"""
Process job flow for dataA Dim Customer.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from spark_process_common.process import BaseProcess, MISSING_DESC, MISSING_STRING_ID

class ProcessDimLocationdataA(BaseProcess):

    def transform(self, sources: dict) -> DataFrame:
        """
        Dim Location records and attributes from dataA Sources
        """

        spark = self.get_spark()
        
        rlt = spark.read.orc(sources['rptm_location_txt']['path'])       
        rlt = rlt.select(rlt.location_id, rlt.short_desc)
        rlt = rlt.withColumn('iptmeta_source_system', F.lit('dataA'))
        rlt = rlt.withColumnRenamed("short_desc", "description")

        df = (
            rlt.select('location_id','iptmeta_source_system','description')
        )

        return df