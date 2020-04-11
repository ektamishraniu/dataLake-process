"""
Process job flow for dataA Dim Customer.
"""
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from spark_process_common.process import BaseProcess, MISSING_DESC, MISSING_STRING_ID

JDE_BILL_TO = 'jde_bill_to'

JDE_SHIP_TO = 'jde_ship_to'


class ProcessDimCustomerdataB(BaseProcess):

    def transform_as4_invoice(self, sources: dict, customer_id_from) -> DataFrame:
        """
        Dim Customer records and attributes from dataB Sources
        """
        spark = self.get_spark()
        inv = spark.read.orc(sources['dataB_invoice_extract']['path'])
        mcy = spark.read.orc(sources['glb_mstr_country']['path'])
        mt = spark.read.orc(sources['glb_mstr_geo_territory']['path'])
        msr = spark.read.orc(sources['glb_mstr_geo_sub_region']['path'])
        mr = spark.read.orc(sources['glb_mstr_geo_region']['path'])
        ma = spark.read.orc(sources['glb_mstr_geo_area']['path'])
        ms = spark.read.orc(sources['glb_mstr_geo_state']['path'])
        rptpar = spark.read.orc(sources['rpt_par']['path'])

        is_bill_to = JDE_BILL_TO == customer_id_from
        df = inv.withColumn("system_id", F.lit('dataB'))
        df = df.withColumn('customer_id', df[customer_id_from])
        df = df.withColumn("customer_key", F.concat_ws('_', df.system_id, F.coalesce(df.customer_id,F.lit(MISSING_STRING_ID))))
        df = df.withColumn("customer_description",
                           F.coalesce(F.upper(
                               df.bill_to_name if is_bill_to else df.ship_to_name
                           ), F.lit(MISSING_DESC)))
        df = df.withColumn("country_id", F.coalesce(F.upper(df.ship_to_country), F.lit(MISSING_STRING_ID)))
        df = df.withColumn("county", F.lit(MISSING_DESC))

        df = (
            df.join(mcy, [mcy.country_id == df.country_id], 'left_outer')
            .join(mt, [mt.geo_territory_id == mcy.geo_territory_id], 'left_outer')
            .join(msr, [msr.geo_sub_region_id == mt.geo_sub_region_id], 'left_outer')
            .join(mr, [mr.geo_region_id == msr.geo_region_id], 'left_outer')
            .join(ma, [ma.geo_area_id == mr.geo_area_id], 'left_outer')
            .join(ms, [ms.state_id == df.ship_to_ship_to, ms.country_id == df.country_id], 'left_outer')
            .join(rptpar, [rptpar.sold_to_customer == df.customer_id], 'left_outer')
            .select(
                df.system_id, df.customer_id, df.customer_key, df.customer_description, df.country_id, df.invoice_date,
                df.invoice, df.end_cust_desc, mcy.country_desc,
                mt.geo_territory_id, mt.geo_territory_desc, msr.geo_sub_region_id, msr.geo_sub_region_desc,
                mr.geo_region_id, mr.geo_region_desc, ma.geo_area_id, ma.geo_area_desc, ms.state_id,
                ms.state_desc, df.county, df.ship_to_city, df.ship_to_zip, df.customer_description, df.line,
                rptpar.commercial_parent
            )
        )

        df = (
            df.withColumn("country_description", F.coalesce(F.upper(df.country_desc), F.lit(MISSING_DESC)))
            .withColumn("territory_id", F.coalesce(F.upper(df.geo_territory_id), F.lit(MISSING_STRING_ID)))
            .withColumn("territory_description", F.coalesce(F.upper(df.geo_territory_desc), F.lit(MISSING_DESC)))
            .withColumn("sub_region_id", F.coalesce(F.upper(df.geo_sub_region_id), F.lit(MISSING_STRING_ID)))
            .withColumn("sub_region_description", F.coalesce(F.upper(df.geo_sub_region_desc), F.lit(MISSING_DESC)))
            .withColumn("region_id", F.coalesce(F.upper(df.geo_region_id), F.lit(MISSING_STRING_ID)))
            .withColumn("region_description", F.coalesce(F.upper(df.geo_region_desc), F.lit(MISSING_DESC)))
            .withColumn("area_id", F.coalesce(F.upper(df.geo_area_id), F.lit(MISSING_STRING_ID)))
            .withColumn("area_description", F.coalesce(F.upper(df.geo_area_desc), F.lit(MISSING_DESC)))
            .withColumn("state_id", F.coalesce(F.upper(df.state_id), F.lit(MISSING_STRING_ID)))
            .withColumn("state_description", F.coalesce(F.upper(df.state_desc), F.lit(MISSING_DESC)))
            .withColumn("county", F.coalesce(F.upper(df.county), F.lit(MISSING_DESC)))
        )

        df = df.withColumn("city", F.coalesce(F.upper(df.ship_to_city), F.lit(MISSING_DESC)))
        df = df.withColumn("postal_code", F.coalesce(df.ship_to_zip, F.lit(MISSING_DESC)))
        df = df.withColumn("commercial_parent",
                           F.coalesce(df.commercial_parent,
                                      F.coalesce(df.customer_description, F.lit(MISSING_DESC))))
        df = df.withColumn("brand_owner", F.coalesce(df.end_cust_desc, df.commercial_parent, F.lit(MISSING_DESC)))
        # This column exists to allow preference of bill to when the invoice has identical customer id/brand owner
        df = df.withColumn('bill_to_ind', F.lit(1 if is_bill_to else 0))
        df = (
            df.select(
                            'system_id', 'customer_id', 'brand_owner', 'customer_key',
                            'customer_description', 'invoice_date', 'invoice',
                            'country_id', 'country_description', 'territory_id', 'territory_description',
                            'sub_region_id', 'sub_region_description', 'region_id', 'region_description',
                            'area_id', 'area_description', 'state_id', 'state_description',
                            'county', 'city', 'postal_code', 'commercial_parent', 'line', 'bill_to_ind')
        )

        return df

    @staticmethod
    def combine_as4_invoice(df_as4_invoice_ship_to: DataFrame, df_as4_invoice_bill_to: DataFrame) -> DataFrame:
        """
        Combine the ship_to & bill_to dataframes to get a unique set of customers
        """

        df = df_as4_invoice_ship_to.union(df_as4_invoice_bill_to)
        df = df.withColumn('iptmeta_source_system', F.lit('dataB'))
        df = df.withColumnRenamed('system_id', 'billing_system')

        window = (
            Window.partitionBy('customer_id','brand_owner')
            .orderBy(
                df['bill_to_ind'].desc(),  # only use ship to data if bill to is not present
                df['invoice_date'].desc(),
                df['invoice'].desc(),
                df['line'],
                df['customer_description']
            )
        )
        df = df.withColumn("rank", F.rank().over(window))
        df = df.filter("rank = 1")

        # Distinct is required when attributes are the same for ship to and bill to on the most recent invoice
        df = (
            df.select(
                'billing_system', 'customer_id', 'brand_owner', 'iptmeta_source_system', 'customer_key',
                'customer_description', 'country_id', 'country_description', 'territory_id', 'territory_description',
                'sub_region_id', 'sub_region_description', 'region_id', 'region_description',
                'area_id', 'area_description', 'state_id', 'state_description',
                'county', 'city', 'postal_code', 'commercial_parent')
        ).distinct()

        return df

    def transform(self, sources: dict) -> DataFrame:

        df_as4_invoice_ship_to = self.transform_as4_invoice(sources, JDE_SHIP_TO)
        df_as4_invoice_bill_to = self.transform_as4_invoice(sources, JDE_BILL_TO)
        df = self.combine_as4_invoice(df_as4_invoice_ship_to, df_as4_invoice_bill_to)

        return df
