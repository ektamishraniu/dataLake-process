"""
Process job flow for dataA Dim Customer.
"""
from pyspark.sql import DataFrame
from spark_process_common.process import BaseProcess

class ProcessDimCustomer(BaseProcess):

    def transform(self, config: dict) -> DataFrame:

        from dim_customer_dataA import ProcessDimCustomerdataA
        from dim_customer_dataB import ProcessDimCustomerdataB

        spark = self.get_spark()
        sources = config['sources']

        dim_customer_dataA = ProcessDimCustomerdataA(spark=spark, config=config)
        df_dataA = dim_customer_dataA.transform(sources=sources)

        dim_customer_as4 = ProcessDimCustomerdataB(spark=spark, config=config)
        df_as4 = dim_customer_as4.transform(sources=sources)

        df = df_dataA.union(df_as4)

        df = df.distinct()
        
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

    with ProcessDimCustomer(spark_config=SPARK_CONFIG) as process:
        process.execute()
