"""
Process job flow for dataA Dim Customer.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from spark_process_common.process import BaseProcess, MISSING_DESC, MISSING_STRING_ID

class ProcessDimLocationdataB(BaseProcess):

    def transform_as4_invoice(self, sources: dict) -> DataFrame:
        """
        Dim Location records and attributes from dataB Invoice Plant data
        """

        spark = self.get_spark()
        
        inv_loc_df = spark.read.orc(sources['dataB_invoice_extract']['path'])

        df = (
            inv_loc_df.select(col('plant').alias('location_id'),
                                  'invoice_date',
                                  'invoice',
                                   col('plant_name').alias('description'))
        )

        window = Window.partitionBy('location_id').orderBy(df['invoice_date'].desc(), df['invoice'].desc())
        df = df.withColumn("rank", F.rank().over(window))
        df_rank = df.filter("rank = 1").distinct()

        df_final = (df_rank.select('location_id', 'invoice_date', 'invoice', 'description'))   

        return df_final   

    def transform_as4_ship_from(self, sources: dict) -> DataFrame:
        """
        Dim Location records and attributes from dataB Invoice Plant data
        """

        spark = self.get_spark()
        
        inv_loc_df = spark.read.orc(sources['dataB_invoice_extract']['path'])
        inv_loc_df = inv_loc_df.withColumn(
            'location_id',
            F.when(inv_loc_df.ship_to_outs_cvtr == 'Y', inv_loc_df.ship_to).otherwise(inv_loc_df.bus_unit)
        )
        inv_loc_df = inv_loc_df.withColumn(
            'description',
            F.when(inv_loc_df.ship_to_outs_cvtr == 'Y', inv_loc_df.ship_to_name).otherwise(inv_loc_df.bus_unit_name)
        )

        df = inv_loc_df.select(
            'location_id',
            'invoice_date',
            'invoice',
            'description'
        )

        window = Window.partitionBy('location_id').orderBy(df['invoice_date'].desc(), df['invoice'].desc())
        df = df.withColumn("rank", F.rank().over(window))
        df_rank = df.filter("rank = 1").distinct()

        df_final = (df_rank.select('location_id', 'invoice_date', 'invoice', 'description'))   

        return df_final

    def transform(self, sources) -> DataFrame:
        """
        Combine the invoice & ship_from dataframes to get a unique set of locations
        """
        df_as4_invoice = self.transform_as4_invoice(sources=sources)
        df_as4_ship_from = self.transform_as4_ship_from(sources=sources)
        df = df_as4_invoice.union(df_as4_ship_from)
        df = df.withColumn('iptmeta_source_system', F.lit('dataB'))

        window = Window.partitionBy('location_id').orderBy(df['invoice_date'].desc(), df['invoice'].desc())
        df = df.withColumn("rank", F.rank().over(window))
        df_rank = df.filter("rank = 1").distinct()

        df_final = df_rank.select('location_id', 'iptmeta_source_system', 'description')                                                                          

        return df_final
