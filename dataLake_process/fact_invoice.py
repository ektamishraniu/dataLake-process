"""
Process job flow for emcomp Profitability Fact Invoice.
"""
import math as M
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType
from pyspark.sql import functions as F
from spark_process_common.process import BaseProcess


class ProcessFactInvoice(BaseProcess):
    is_not_close_udf = F.udf(lambda a, b: not M.isclose(a, b, abs_tol=0.01), BooleanType())

    def after_agg(self, after_df) -> DataFrame:
        return after_df.groupBy(after_df.billing_system, after_df.invoice_number).agg(
            F.sum(F.coalesce(after_df.actual_tons, F.lit(0.0))).alias('actual_tons'),
            F.sum(F.when(F.coalesce(after_df.gross_price, F.lit(0.0)) == 0.0, F.lit(0))
                  .otherwise(F.lit(1))).alias('gross_price_nonzero')
        )

    def validate_before_after(self, before_agg, after_agg) -> DataFrame:
        is_not_close_udf = F.udf(lambda a, b: not M.isclose(a, b, abs_tol=0.01), BooleanType())
        return (before_agg.join(after_agg,
                                [before_agg.billing_system_before == after_agg.billing_system,
                                 before_agg.invoice_number_before == after_agg.invoice_number], 'left_outer')
                .filter(is_not_close_udf(before_agg.actual_tons_before,
                                         F.coalesce(after_agg.actual_tons, F.lit(float('NaN')))) |
                        is_not_close_udf(before_agg.gross_price_nonzero_before,
                                         F.coalesce(after_agg.gross_price_nonzero, F.lit(float('NaN'))))))

    def validate_mydataB(self, before_df: DataFrame, after_df: DataFrame) -> DataFrame:
        before_agg = (before_df.withColumn('billing_system_before', F.lit('mydataB'))
                      .groupBy('billing_system_before', before_df.invoice)).agg(
            F.sum(F.coalesce(before_df.lbs / 2000.0, F.lit(0.0))).alias('actual_tons_before'),
            F.sum(F.when((before_df.gen_ledg == 41000) &
                         (F.coalesce(before_df.extended_amount, F.lit(0.0)) != F.lit(0.0)), F.lit(1))
                  .otherwise(F.lit(0))).alias('gross_price_nonzero_before')
        )
        before_agg = before_agg.withColumnRenamed('invoice', 'invoice_number_before')

        after_agg = self.after_agg(after_df)

        return self.validate_before_after(before_agg, after_agg)

    def validate_mydataA(self, before_df: DataFrame, after_df: DataFrame) -> DataFrame:
        before_agg = (before_df.groupBy(before_df.system_id, before_df.invoice_id)).agg(
            F.sum(F.coalesce(before_df.weight_qty, F.lit(0.0))).alias('actual_tons_before'),
            F.sum(F.when((before_df.rev_acct_id.isin('R0100', 'R0500', 'R0700', 'R0105')) &
                         (F.coalesce(before_df.invoice_line_value, F.lit(0.0)) != F.lit(0.0)), F.lit(1))
                  .otherwise(F.lit(0))).alias('gross_price_nonzero_before')
        )
        before_agg = (before_agg
                      .withColumnRenamed('system_id', 'billing_system_before')
                      .withColumnRenamed('invoice_id', 'invoice_number_before'))

        after_agg = self.after_agg(after_df)

        return self.validate_before_after(before_agg, after_agg)

    def post_processing(self, sources: dict, df) -> DataFrame:
        excinv = self.read_source(source=sources['exclude_invoices'])

        df_post = df.join(excinv, [excinv.invoice_number == df.invoice_number,
                                   excinv.billing_system == df.billing_system],
                          'left_anti')

        return df_post

    def transform(self, config: dict) -> DataFrame:
        """
        Fact Invoice records and attributes from all Sources
        """
        from fact_invoice_mydataA import ProcessFactInvoicemydataA
        from fact_invoice_mydataB import ProcessFactInvoicemydataB
        from fact_invoice_cpsacr import ProcessFactInvoiceCpsacr
        
        spark = self.get_spark()
        sources = config['sources']

        fact_invoice_mydataA = ProcessFactInvoicemydataA(spark=spark, config=config)
        df_mydataA = fact_invoice_mydataA.transform(sources=sources).persist(StorageLevel.DISK_ONLY)

        val_mydataA = self.validate_mydataA(fact_invoice_mydataA.invoice_dataframe(sources['rptt_invoice']), df_mydataA)

        fact_invoice_mydataB = ProcessFactInvoicemydataB(spark=spark, config=config)
        df_mydataB = fact_invoice_mydataB.transform(sources=sources).persist(StorageLevel.DISK_ONLY)

        val_mydataB = self.validate_mydataB(fact_invoice_mydataB.invoice_dataframe(sources['mydataB_urbcrb_invoice']), df_mydataB)

        fact_invoice_cpsacr = ProcessFactInvoiceCpsacr(spark=spark, config=config)
        df_cpsacr = fact_invoice_cpsacr.transform(sources=sources)
        
        # Union with other sources here
        df = df_mydataA.union(df_mydataB)
        df = df.union(df_cpsacr)

        df_val = val_mydataA.union(val_mydataB).coalesce(1)
        val_config = {'target_path': config['validate_target_path'],
                      'write_options': config['write_options'],
                      'target_format': 'csv'}
        self.save(df_val, val_config)

        df = self.post_processing(sources, df)

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

    with ProcessFactInvoice(spark_config=SPARK_CONFIG) as process:
        process.execute()
