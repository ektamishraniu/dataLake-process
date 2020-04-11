"""
Process job flow for dataA Dim Customer.
"""
from pyspark.sql import DataFrame
from pyspark.sql import Window as W
from pyspark.sql import functions as F
from spark_process_common.process import BaseProcess

class ProcessDimLocation(BaseProcess):

    def transform_distinct(self, df_as4: DataFrame, df_dataA: DataFrame) -> DataFrame:

        df = df_dataA.union(df_as4)

        # There are cases when the same location_id is referenced in both source systems
        # Since we don't have another way to determine which specific system to choose
        # conflicting records from, we choose something that is deterministic.
        window = W.partitionBy('location_id').orderBy(df['iptmeta_source_system'])
        df = df.withColumn("rank", F.rank().over(window))
        df_rank = df.filter("rank = 1").distinct()

        return df_rank   

    def transform(self, config: dict) -> DataFrame:
        
        from dim_location_dataB import ProcessDimLocationdataB
        from dim_location_dataA import ProcessDimLocationdataA

        spark = self.get_spark()
        sources = config['sources']

        dim_location_dataA = ProcessDimLocationdataA(spark=spark, config=config)
        df_dataA = dim_location_dataA.transform(sources=sources)

        dim_location_dataB = ProcessDimLocationdataB(spark=spark, config=config)
        df_as4 = dim_location_dataB.transform(sources=sources)
        df = self.transform_distinct(df_as4, df_dataA)
        
        return df

if __name__ == '__main__':
    SPARK_CONFIG = {
        "spark.sql.hive.convertMetastoreOrc": "true",
        "spark.sql.files.ignoreMissingFiles": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.hive.verifyPartitionPath": "false",
        "spark.sql.orc.filterPushdown": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "hive.exec.dynamic.partition": "true"         
    }

    with ProcessDimLocation(spark_config=SPARK_CONFIG) as process:
        process.execute()
