"""
Process job flow for dataA Dim Customer.
"""
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from spark_process_common.process import BaseProcess, MISSING_DESC, MISSING_STRING_ID

class ProcessDimCustomerdataA(BaseProcess):

    def transform(self, sources: dict) -> DataFrame:
        """
        Dim Customer records and attributes from dataA Sources
        """
        spark = self.get_spark()        
        mc = spark.read.orc(sources['mstr_customer_vw']['path'])
        mcy = spark.read.orc(sources['glb_mstr_country']['path'])
        mt = spark.read.orc(sources['glb_mstr_geo_territory']['path'])
        msr = spark.read.orc(sources['glb_mstr_geo_sub_region']['path'])
        mr = spark.read.orc(sources['glb_mstr_geo_region']['path'])
        ma = spark.read.orc(sources['glb_mstr_geo_area']['path'])
        ms = spark.read.orc(sources['glb_mstr_geo_state']['path'])

        df = (
            mc.join(mcy, [mcy.country_id == mc.country_id], 'left_outer')
            .join(mt, [mt.geo_territory_id == mcy.geo_territory_id], 'left_outer')
            .join(msr, [msr.geo_sub_region_id == mt.geo_sub_region_id], 'left_outer')
            .join(mr, [mr.geo_region_id == msr.geo_region_id], 'left_outer')
            .join(ma, [ma.geo_area_id == mr.geo_area_id], 'left_outer')
            .join(ms, [ms.state_id == mc.state_id, ms.country_id == mc.country_id], 'left_outer')
            .select(
                mc.system_id, mc.customer_id, mc.customer_desc, mc.country_id, mcy.country_desc,
                mt.geo_territory_id, mt.geo_territory_desc, msr.geo_sub_region_id, msr.geo_sub_region_desc,
                mr.geo_region_id, mr.geo_region_desc, ma.geo_area_id, ma.geo_area_desc, ms.state_id,
                ms.state_desc, mc.county, mc.city, mc.postal_code, mc.top_10_desc    
            )
        )

        df = df.withColumn('iptmeta_source_system', F.lit('dataA'))
        df = df.withColumn("customer_key", F.concat_ws('_', df.system_id, df.customer_id))
        df = df.withColumn("customer_description", F.coalesce(F.upper(df.customer_desc), F.lit(MISSING_DESC)))

        df = (
            df.withColumn("country_id", F.coalesce(F.upper(df.country_id), F.lit(MISSING_STRING_ID)))
            .withColumn("country_description", F.coalesce(F.upper(df.country_desc), F.lit(MISSING_DESC)))
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

        df = df.withColumn("city", F.coalesce(F.upper(df.city), F.lit(MISSING_DESC)))
        df = df.withColumn("postal_code", F.coalesce(df.postal_code, F.lit(MISSING_DESC)))
        df = df.withColumn("commercial_parent", F.coalesce(df.top_10_desc, F.lit(MISSING_DESC)))
        df = df.withColumn("brand_owner", F.coalesce(df.customer_desc, df.top_10_desc, F.lit(MISSING_DESC)))

        df = df.withColumnRenamed('system_id', 'billing_system')
        
        df = df.select(
            df.billing_system, df.customer_id, df.brand_owner, df.iptmeta_source_system
            , df.customer_key, df.customer_description
            , df.country_id, df.country_description, df.territory_id, df.territory_description
            , df.sub_region_id, df.sub_region_description, df.region_id, df.region_description
            , df.area_id, df.area_description, df.state_id, df.state_description
            , df.county, df.city, df.postal_code, df.commercial_parent
        )

        return df