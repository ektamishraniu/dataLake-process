"""
Process job flow for Mill Profitability Fact Invoice from dataB Data Sources.
"""
import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StringType
from spark_process_common.process import BaseProcess, MISSING_DESC, MISSING_STRING_ID, NOT_APPLICABLE_CODE, NOT_APPLICABLE_DESC, MISSING_NUMBER
from spark_process_common.transforms import hash_columns
from transforms import prime_enrich, dataB_sale_form, fixCurExchangeToAvg, dataB_adjust_currency_fields

class ProcessFactInvoicedataB(BaseProcess):
    """
    Mill Profitability Fact Invoice from dataB Data Sources.
    """

    @staticmethod
    def addColPrefix(df: DataFrame, mystr="notgiven") -> DataFrame:
        '''
        Simple function to prefix all columns in a supplied DataFrame
        with a string.
        '''

        for mcol in df.columns:
            df = df.withColumnRenamed(mcol, mystr + "_" + mcol)
        return df

    @staticmethod
    def remove_all_whitespace(col):
        return F.regexp_replace(col, "\\s+", "")

    @staticmethod
    def dataB_material_id(df: DataFrame) -> DataFrame:
        '''
        Calculates the material id with a specific rule for the data
        frame supplied.
        '''

        data_frame = df.withColumn('material_id',
                    F.concat(F.col("grade"), F.lit("_"), F.col("caliper"), F.lit("_"), 
                    F.format_number(F.col("width"),3), F.lit("_"), F.format_number(F.col("length"),3)))
        return data_frame

    @staticmethod
    def dataB_filter_plant(df: DataFrame, edf: DataFrame) -> DataFrame:
        '''
        Filter out plants based on the contents of the Exclude Plants
        worksheet from the Recycle Product Taxonomy Excel file.
        '''

        data_frame = df.join(edf, df.plant==edf.plant, 'left_anti')
        return data_frame

    @staticmethod
    def dataB_filter_billtoname_jdebillto(df: DataFrame, edf: DataFrame) -> DataFrame:
        '''
        Filter out invoice records where the jdebillto id is listed in the Recycle Product
        Taxonomy file in the Exclude JDEBillTo sheet.
        '''

        data_frame = df.join(edf, df.jde_bill_to==edf.jdebillto, 'left_anti')
        return data_frame

    @staticmethod
    def dataB_filter_plantvals(df: DataFrame) -> DataFrame:
        '''
        Limit the data set to only include the specified plants.
        This may seem to conflict with the filter for the plants in dataB_filter_plant,
        but the above list could at one point exclude one of these and that dynamic load
        will provide the flexibility needed by the business.
        '''

        data_frame = df.filter(F.col('plant').isin([3250, 3270, 3221, 3211, 3330, 3212, 3340]))
        return data_frame

    @staticmethod
    def dataB_filter_report_date(df: DataFrame) -> DataFrame:
        '''
        Filter the invoice records by the report month and year
        ensuring nothing is older than 2016-10-01
        '''

        data_frame = df.filter(((df.rept_year < 2016)
                               | ((df.rept_year == 2016) & (df.rept_month < 10)))
                               == False)
        return data_frame

    @staticmethod
    def dataB_filter_caliper(df: DataFrame) -> DataFrame:
        '''
        calipers (after being multiplied by 1000) should not be >= 200
        '''
        max_caliper = "200"
        data_frame = df.filter(df.caliper < max_caliper)
        return data_frame    

    @staticmethod
    def dataB_claims(df: DataFrame) -> DataFrame:
        # Identify the claims value based on the gen_ledg field value
        data_frame = df.withColumn("claims", F.coalesce(F.when(df.gen_ledg.isin([46000, 46400]),
                                                               df.adj_extended_amount).otherwise(0),
                                                        F.lit(MISSING_NUMBER)))
        return data_frame

    @staticmethod
    def dataB_rebates(df: DataFrame) -> DataFrame:
        data_frame = df.withColumn('rebates', F.coalesce(-1 * F.col("rmr_rebate_amount"),
                                                         F.lit(MISSING_NUMBER)))
        return data_frame

    @staticmethod
    def dataB_discounts(df: DataFrame) -> DataFrame:
        data_frame = df.withColumn('discounts', F.coalesce(-1 * df.adj_allowed_disc,
                                                           F.lit(MISSING_NUMBER)))
        return data_frame

    @staticmethod
    def dataB_freight_upcharge(df: DataFrame) -> DataFrame:
        data_frame = df.withColumn("freight_upcharge",
                                   F.coalesce(
                                       F.when(df.charge_desc1.isin({
                                           "ADDED FREIGHT",
                                           "FREIGHT ALLOWANCE",
                                           "FREIGHT CHARGE",
                                           "FREIGHT SURCHARGE"}),
                                           df.adj_extended_amount).otherwise(0.0),
                                       F.lit(MISSING_NUMBER)))
        return data_frame

    @staticmethod
    def dataB_product_sold_flag(df: DataFrame) -> DataFrame:
        data_frame = df.withColumn("product_sold_flag", F.when( 
                                  ((df.lbs.isNull()) | (df.lbs==0)), F.lit('N')).otherwise(F.lit('Y')))
        return data_frame

    @staticmethod
    def dataB_gross_price(df: DataFrame) -> DataFrame:
        data_frame = df.withColumn("gross_price", F.coalesce(F.when((df.gen_ledg == 41000),
                                                                    df.adj_extended_amount).otherwise(0),
                                                             F.lit(MISSING_NUMBER)))
        return data_frame

    @staticmethod
    def dataB_invoiced_currency(df: DataFrame) -> DataFrame:
        df = df.withColumn('currency_code_len', F.length((F.regexp_replace(df.currency_code, "\\s+", "") ) ) )
        data_frame = df.withColumn("invoiced_currency", 
                        F.when(((df.currency_code_len == 0) | (df.currency_code == None)), F.lit('USD')) \
                          .otherwise(df.currency_code)).drop('currency_code_len')
        return data_frame

    @staticmethod
    def dataB_sale_type(df: DataFrame) -> DataFrame:
        title_case_udf = F.udf(lambda x: x.title(), StringType())
        data_frame = df.withColumn("sale_type", F
                                   .when((df.bill_to == 9934650000), F.lit('IntraBU'))
                                   .otherwise(F.coalesce(title_case_udf(df.channel), F.lit(MISSING_DESC))))
        return data_frame

    @staticmethod
    def dataB_msf(df: DataFrame) -> DataFrame:
        df = df.withColumn("msf", 
		        F.when(((df.sq_ft.isNotNull()) & (df.sq_ft > 0)), ( df.sq_ft / 1000.0) ).
	            otherwise( 
			    F.when(( (df.lbs.isNotNull()) & (df.dmat_nominal_basis_weight.isNotNull() | (df.dmat_nominal_basis_weight!=0)) ), 
				          df.lbs/df.dmat_nominal_basis_weight).otherwise(F.lit(MISSING_NUMBER)) 
				         )
				)
        df = df.withColumn('msf', F.coalesce( df.msf, F.lit(MISSING_NUMBER) ) )
        return df

    @staticmethod
    def dataB_ship_from_loc_number(df: DataFrame) -> DataFrame:
        data_frame = df.withColumn("ship_from_loc_number", 
                                F.when((df.ship_to_outs_cvtr == "Y"), df.ship_to) \
                                .otherwise(df.bus_unit))
        return data_frame

    @staticmethod
    def dataB_process_std_freight_rates(df_rates: DataFrame, df_plant: DataFrame) -> DataFrame:
        '''
        This function takes the Freight Rates file and creates a dataframe of estimates
        that are limited to the plants specified in the plant_mapping data source.  The
        estimates include source and destination, where the sources are the plants that
        the invoice orignated from.  This dataframe is used to estimate the Freight Invoice
        value when the invoice cannot be properly matched to OTM.
        df_rates - Rate Validation data source
        df_plant - Plant Mapping data source
        '''

        df = df_rates.join(df_plant, [(df_rates.ocity == df_plant.city) & (df_rates.ostate == df_plant.state)], "inner") \
            .select(df_rates.direction, df_rates.ocity, df_rates.ostate, df_rates.dcity, df_rates.dstate, df_rates.base_rate, df_plant.plant) \
            .groupBy(df_rates.direction, df_rates.ocity, df_rates.ostate, df_rates.dcity, df_rates.dstate, df_plant.plant) \
            .agg((F.avg('base_rate') / 20).alias('estimate_freight_rate_per_ton')) \
            .where(df_rates.direction == 'Outbound')
        return df

    @staticmethod
    def dataB_process_std_freight_rates_slr(df_slr: DataFrame) -> DataFrame:
        '''
        This function takes the Supplemental Lane Rates data frame and preps it to be
        a supplement to the Standard Freight Rates data frame processed in
        dataB_process_std_freight_rates.  This should cover the lanes that are missing
        from the official Freight Rate file.
        df_slr - Supplemental Lane Rates data source
        '''

        df = df_slr.select(df_slr.plant, df_slr.ocity, df_slr.ostate, df_slr.dcity, df_slr.dstate, df_slr.total_cost_per_load) \
            .withColumn('estimate_freight_rate_per_ton', (df_slr.total_cost_per_load / 20))
        return df

    @staticmethod
    def dataB_process_otm(df_s: DataFrame, df_cost: DataFrame, df_sr: DataFrame,
                          df_sstat: DataFrame, df_ss: DataFrame, df_ss_remark: DataFrame,
                          df_inv: DataFrame) -> DataFrame:
        """
        This function takes OTM source data frames and processes Freght Costs for the
        invoices in the dataB database that correspond to the Recycle Mills.
        :param df_s: Shipment table dataframe
        :param df_cost: Shipment_Cost table dataframe
        :param df_sr: Shipment_Refnum table dataframe
        :param df_sstat: Shipment_Status table dataframe
        :param df_ss: Shipment_Stop table dataframe
        :param df_ss_remark: Shipment_Stop_Remark table dataframe
        :param df_inv: dataB Recycle Invoices dataframe to match with OTM data
        :return: df: OTM processed dataframe
        """

        # Filter the Shipment_Refnum dataframe to only include records
        # with domain_name of SSCC and shipment_refnum_qual_gid's that equal MB or SSCC.MB.
        # Including a distinct because of duplicate values in the source.
        df_sr = df_sr.where(df_sr.domain_name == 'SSCC')\
            .where(df_sr.shipment_refnum_qual_gid.isin(['MB', 'SSCC.MB']))\
            .distinct()

        # Join on the shipment_stop_remark table to include sub-BOL's
        df_sr_sub_bol = df_sr.join(df_ss_remark, [df_sr.shipment_gid == df_ss_remark.shipment_gid], 'inner')\
            .select(df_sr.shipment_gid,
                    df_ss_remark.remark_text,
                    df_ss_remark.insert_date)
        df_sr_sub_bol = df_sr_sub_bol.withColumnRenamed('remark_text', 'shipment_refnum_value')
        df_sr = df_sr.select(df_sr.shipment_gid,
                             df_sr.shipment_refnum_value,
                             df_sr.insert_date)
        df_sr = df_sr.union(df_sr_sub_bol)

        # Filter down the Shipment_Refnum dataset to only include the records
        # that match with invoices so that the later joins and calculations
        # are more focused.  Since the bol's are unique in OTM, we only 
        # need unique bol numbers here to filter down.
        df_inv = df_inv.where(df_inv.lbs >= 0)\
            .groupBy(df_inv.bol_number)\
            .agg((F.sum(df_inv.lbs)/2000).alias("total_bol_tons_from_inv"))

        df_sr = df_sr.groupBy(df_sr.shipment_gid,
                              df_sr.shipment_refnum_value)\
            .agg(F.max(df_sr.insert_date).alias("max_insert_date"))
        df_sr = df_sr.orderBy(df_sr.shipment_gid,
                              df_sr.shipment_refnum_value,
                              df_sr.max_insert_date.desc())\
            .dropDuplicates(["shipment_gid",
                             "shipment_refnum_value"])

        df_sr = df_inv.join(df_sr, [df_inv.bol_number == df_sr.shipment_refnum_value], 'inner')\
            .select(df_sr.shipment_gid,
                    df_sr.shipment_refnum_value,
                    df_inv.total_bol_tons_from_inv)

        df_sr_join_back = df_sr.select(df_sr.shipment_gid,
                                       df_sr.shipment_refnum_value)\
            .withColumnRenamed('shipment_gid', 'df_sr_join_back_shipment_gid')

        df_sr = df_sr.select(df_sr.shipment_gid,
                             df_sr.total_bol_tons_from_inv)\
            .groupBy(df_sr.shipment_gid)\
            .agg(F.sum(df_inv.total_bol_tons_from_inv).alias("total_shipment_gid_tons_from_inv"))

        df_sr = df_sr.join(df_sr_join_back, [df_sr.shipment_gid == df_sr_join_back.df_sr_join_back_shipment_gid], 'inner')\
            .select(df_sr.shipment_gid,
                    df_sr.total_shipment_gid_tons_from_inv,
                    df_sr_join_back.shipment_refnum_value)\
            .withColumnRenamed('shipment_gid', 'sr_shipment_gid')

        df_sstat = df_sstat.groupBy(df_sstat.domain_name,
                                    df_sstat.shipment_gid, 
                                    df_sstat.status_type_gid,
                                    df_sstat.status_value_gid)\
            .agg(F.max(df_sstat.insert_date).alias("max_insert_date"))
        df_sstat = df_sstat.orderBy(df_sstat.shipment_gid,
                                    df_sstat.status_type_gid,
                                    df_sstat.status_value_gid,
                                    df_sstat.max_insert_date.desc())\
            .dropDuplicates(["shipment_gid",
                             "status_type_gid",
                             "status_value_gid"])

        # Filter the Shipment Refnum dataframe to only include records where the Shipment Status
        # matches what we are looking for with domain_name of SSCC and specific status_value_id's
        # At least three of the status_value_gid's need to match in order to put the confidence
        # level high enough to signify a match.
        df_sr = df_sr.join(df_sstat, [df_sr.sr_shipment_gid == df_sstat.shipment_gid], 'inner')\
            .where((df_sstat.domain_name == 'SSCC') &
                   (df_sstat.status_value_gid.isin({
                       'SSCC.BOL_ACTUALS_ENTERED_TRANSMISSION',
                       'SSCC.BOL DELETED_NO',
                       'SSCC.SECURE RESOURCES_ACCEPTED',
                       'SSCC.SECURE RESOURCES_PICKUP NOTIFICATION'})))\
            .groupBy(df_sr.sr_shipment_gid,
                     df_sr.shipment_refnum_value,
                     df_sr.total_shipment_gid_tons_from_inv).count()\
            .where(F.col('count') > 2) \
            .select(df_sr.sr_shipment_gid,
                    df_sr.shipment_refnum_value,
                    df_sr.total_shipment_gid_tons_from_inv)

        df_ss = df_ss.groupBy(df_ss.domain_name,
                              df_ss.shipment_gid,
                              df_ss.stop_num,
                              df_ss.dist_from_prev_stop_base)\
            .agg(F.max(df_ss.insert_date).alias("max_insert_date"))
        df_ss = df_ss.select(df_ss.domain_name,
                             df_ss.shipment_gid,
                             df_ss.stop_num,
                             df_ss.dist_from_prev_stop_base,
                             df_ss.max_insert_date)\
            .orderBy(df_ss.shipment_gid,
                     df_ss.stop_num,
                     df_ss.dist_from_prev_stop_base,
                     df_ss.max_insert_date.desc())\
            .dropDuplicates(["shipment_gid",
                             "stop_num"])

        # Filter the Shipment_Stop dataframe to only include records
        # with domain_name of SSCC and then add up the dist_from_prev_stop_base values
        # to determine the mileage
        df_ss = df_ss.where(df_ss.domain_name == 'SSCC')\
            .groupBy(df_ss.shipment_gid)\
            .agg(F.sum('dist_from_prev_stop_base').alias('mileage'))
        df_ss = df_ss.withColumnRenamed('shipment_gid', 'ss_shipment_gid')

        df_cost = df_cost.groupBy(df_cost.cost_type,
                                  df_cost.cost_base,
                                  df_cost.accessorial_code_gid,
                                  df_cost.is_weighted,
                                  df_cost.domain_name,
                                  df_cost.shipment_gid)\
            .agg(F.max(df_cost.insert_date).alias("max_insert_date"))

        # Drop the extra entries based on the max insert date
        df_cost = df_cost.orderBy(df_cost.shipment_gid,
                                  df_cost.max_insert_date.desc())\
            .dropDuplicates(["shipment_gid", "cost_base"])\
            .select(df_cost.shipment_gid,
                    df_cost.accessorial_code_gid,
                    df_cost.cost_type,
                    df_cost.cost_base,
                    df_cost.is_weighted,
                    df_cost.domain_name)

        # Filter the Shipment_Cost dataframe to only include records
        # with domain_name of SSCC
        df_cost = df_cost.where(df_cost.domain_name == 'SSCC')

        # Create a dataframe from Shipment_Cost that includes the detention costs
        df_cost_det = df_cost.where(df_cost.cost_type == 'A')\
            .where(df_cost.accessorial_code_gid.isin({
                'SSCC.DETENTION',
                'SSCC.DETENTION_DESTINATION',
                'SSCC.DTL LOADING',
                'SSCC.STORAGE'}))\
            .groupBy(df_cost.shipment_gid)\
            .agg(F.sum('cost_base').alias('det_cost_base_sum'))\
            .select(df_cost.shipment_gid, 'det_cost_base_sum')
        df_cost_det = df_cost_det.withColumnRenamed('shipment_gid', 'det_shipment_gid')
        df_cost_det = df_cost_det\
            .withColumn('det_cost_base_sum',
                        F.when(df_cost_det.det_cost_base_sum.isNotNull(), df_cost_det.det_cost_base_sum)
                        .otherwise(0))

        # Create a dataframe from Shipment_Cost that includes the accessorial costs
        df_cost_acc = df_cost.where(df_cost.cost_type.isin({'A', 'S', 'O'}))\
            .where(df_cost.accessorial_code_gid.isin({
                'SSCC.DETENTION',
                'SSCC.DETENTION_DESTINATION',
                'SSCC.DTL LOADING',
                'SSCC.STORAGE'}) == False)\
            .where(F.split(df_cost.accessorial_code_gid, '.').getItem(1).contains('FSC') == False)\
            .where(df_cost.is_weighted == 'N')\
            .groupBy(df_cost.shipment_gid)\
            .agg(F.sum('cost_base').alias('acc_cost_base_sum'))\
            .select(df_cost.shipment_gid, 'acc_cost_base_sum')
        df_cost_acc = df_cost_acc.withColumnRenamed('shipment_gid', 'acc_shipment_gid')
        df_cost_acc = df_cost_acc\
            .withColumn('acc_cost_base_sum',
                        F.when(df_cost_acc.acc_cost_base_sum.isNotNull(), df_cost_acc.acc_cost_base_sum)
                        .otherwise(0))

        # Create a dataframe from Shipment_Cost that includes the accessorial costs
        df_cost_fsrchg = df_cost.where(df_cost.cost_type == 'A')\
            .where(F.split(df_cost.accessorial_code_gid, '.').getItem(1).contains('FSC'))\
            .groupBy(df_cost.shipment_gid)\
            .agg(F.sum('cost_base').alias('fsrchg_cost_base_sum'))\
            .select(df_cost.shipment_gid, 'fsrchg_cost_base_sum')
        df_cost_fsrchg = df_cost_fsrchg.withColumnRenamed('shipment_gid', 'fsrchg_shipment_gid')
        df_cost_fsrchg = df_cost_fsrchg\
            .withColumn('fsrchg_cost_base_sum',
                        F.when(df_cost_fsrchg.fsrchg_cost_base_sum.isNotNull(), df_cost_fsrchg.fsrchg_cost_base_sum)
                        .otherwise(0))

        # Create a dataframe from Shipment_Cost that includes the base rate costs
        # for LTL based shipments.  This will be later joined to the Shipment
        # table to apply a Freight cost value if the transport mode is LTL
        df_cost_ltl_base = df_cost.where(df_cost.cost_type.isin({'B', 'D'}))\
            .groupBy(df_cost.shipment_gid)\
            .agg(F.sum('cost_base').alias('ltl_base_cost_base_sum'))\
            .select(df_cost.shipment_gid, 'ltl_base_cost_base_sum')
        df_cost_ltl_base = df_cost_ltl_base.withColumnRenamed('shipment_gid', 'ltl_base_shipment_gid')
        df_cost_ltl_base = df_cost_ltl_base\
            .withColumn('ltl_base_cost_base_sum',
                        F.when(df_cost_ltl_base.ltl_base_cost_base_sum.isNotNull(),
                               df_cost_ltl_base.ltl_base_cost_base_sum)
                        .otherwise(0))

        # Create a dataframe from Shipment_Cost that includes the base rate costs
        # for non-LTL based shipments.  This will be later joined to the Shipment
        # table to apply a Freight cost value if the transport mode is not LTL
        df_cost_nonltl_base = df_cost.where(df_cost.cost_type == 'B')\
            .groupBy(df_cost.shipment_gid)\
            .agg(F.sum('cost_base').alias('nonltl_base_cost_base_sum')) \
            .select(df_cost.shipment_gid, 'nonltl_base_cost_base_sum')
        df_cost_nonltl_base = df_cost_nonltl_base.withColumnRenamed('shipment_gid', 'nonltl_base_shipment_gid')
        df_cost_nonltl_base = df_cost_nonltl_base\
            .withColumn('nonltl_base_cost_base_sum',
                        F.when(df_cost_nonltl_base.nonltl_base_cost_base_sum.isNotNull(),
                               df_cost_nonltl_base.nonltl_base_cost_base_sum)
                        .otherwise(0))

        df_s = df_s.groupBy(df_s.shipment_gid,
                            df_s.transport_mode_gid,
                            df_s.total_weight_base) \
            .agg(F.max(df_s.insert_date).alias("max_insert_date"))
        df_s = df_s.orderBy(df_s.shipment_gid, df_s.max_insert_date.desc())\
            .dropDuplicates(["shipment_gid"])

        # This join filters down the ref_nums to only shipments with good statuses that are 
        # relevant to our invoices.
        df = df_sr.join(df_s, [df_sr.sr_shipment_gid == df_s.shipment_gid], 'left_outer')
        df = df.join(df_ss, [df_ss.ss_shipment_gid == df.sr_shipment_gid], 'left_outer') 
        df = df.join(df_cost_det, [df_cost_det.det_shipment_gid == df.sr_shipment_gid], 'left_outer') 
        df = df.join(df_cost_acc, [df_cost_acc.acc_shipment_gid == df.sr_shipment_gid], 'left_outer') 
        df = df.join(df_cost_fsrchg, [df_cost_fsrchg.fsrchg_shipment_gid == df.sr_shipment_gid], 'left_outer') 
        df = df.join(df_cost_ltl_base, [df_cost_ltl_base.ltl_base_shipment_gid == df.sr_shipment_gid], 'left_outer') 
        df = df.join(df_cost_nonltl_base, [df_cost_nonltl_base.nonltl_base_shipment_gid == df.sr_shipment_gid], 'left_outer')
        df = df.select(df.shipment_gid,
                       df.transport_mode_gid,
                       df.total_shipment_gid_tons_from_inv,
                       df.shipment_refnum_value,
                       df.mileage,
                       df.det_cost_base_sum,
                       df.acc_cost_base_sum,
                       df.fsrchg_cost_base_sum,
                       df.ltl_base_cost_base_sum,
                       df.nonltl_base_cost_base_sum)
        df = df.withColumn('det_cost_base_sum',
                           F.when(df.det_cost_base_sum.isNotNull(), df.det_cost_base_sum)
                           .otherwise(0))\
            .withColumn('acc_cost_base_sum',
                        F.when(df.acc_cost_base_sum.isNotNull(), df.acc_cost_base_sum)
                        .otherwise(0))\
            .withColumn('fsrchg_cost_base_sum',
                        F.when(df.fsrchg_cost_base_sum.isNotNull(), df.fsrchg_cost_base_sum)
                        .otherwise(0))\
            .withColumn('ltl_base_cost_base_sum',
                        F.when(df.ltl_base_cost_base_sum.isNotNull(), df.ltl_base_cost_base_sum)
                        .otherwise(0))\
            .withColumn('nonltl_base_cost_base_sum',
                        F.when(df.nonltl_base_cost_base_sum.isNotNull(), df.nonltl_base_cost_base_sum)
                        .otherwise(0))

        # Calculate the individual costs based on all of the joined tables.
        df = df.withColumn('tons',
                           F.when(df.total_shipment_gid_tons_from_inv.isNull(), 0)
                           .otherwise(df.total_shipment_gid_tons_from_inv).cast(T.DecimalType(38, 18)))\
            .withColumn('detention',
                        F.when(df.det_cost_base_sum.isNull(), 0)
                        .otherwise(df.det_cost_base_sum).cast(T.DecimalType(38, 18)))\
            .withColumn('accessorials',
                        F.when(df.acc_cost_base_sum.isNull(), 0)
                        .otherwise(df.acc_cost_base_sum).cast(T.DecimalType(38, 18)))\
            .withColumn('fuel_surcharge',
                        F.when(df.fsrchg_cost_base_sum.isNull(), 0)
                        .otherwise(df.fsrchg_cost_base_sum).cast(T.DecimalType(38, 18)))\
            .withColumn('base_rate',
                        F.when((df.ltl_base_cost_base_sum.isNull() & df.nonltl_base_cost_base_sum.isNull()), 0)
                        .when(F.trim(F.upper(df.transport_mode_gid)) == 'LTL', df.ltl_base_cost_base_sum)
                        .otherwise(df.nonltl_base_cost_base_sum).cast(T.DecimalType(38, 18)))
        # Calculate the freight rate per ton
        df = df.withColumn('freight_rate_per_ton',
                           F.when(df.tons > 0,
                                  (df.detention + df.accessorials + df.fuel_surcharge + df.base_rate) / df.tons)
                           .otherwise(0))\
            .withColumnRenamed('shipment_refnum_value', 'bol_number_join')

        # It is possible for multiple shipment_gid to match to the same bol_number and have the same cost.
        # This distinct removes those cases so as not to introduce duplicates when joining with the invoice table.
        df = (df.select(df.freight_rate_per_ton,
                        df.bol_number_join)
              .distinct())

        return df

    def transform(self, sources: dict) -> DataFrame:
        """
        Fact Invoice records and attributes from dataA Sources
        """
        
        rpttax = self.read_source(source=sources['rpt_tax'])
        inv = self.invoice_dataframe(sources['dataB_urbcrb_invoice'])
        cer = self.read_source(source=sources['currency_exchange_master'])
        cer = fixCurExchangeToAvg(self, cer)
        rsc = self.read_source(source=sources['recycle_standard_cost'])
        rmr = self.read_source(source=sources['recycle_mill_rebates'])
        exclplnt = self.read_source(source=sources['rpt_exclplnt'])
        exclbillto = self.read_source(source=sources['rpt_exclbillto'])

        salesmanexc = self.read_source(source=sources['rpt_salesmanexc'])
        salesmanexc = self.addColPrefix(salesmanexc, "salesmanexc")

        otm_shipment = self.read_source(source=sources['shipment'])
        otm_shipment_cost = self.read_source(source=sources['shipment_cost'])
        otm_shipment_refnum = self.read_source(source=sources['shipment_refnum'])
        otm_shipment_status = self.read_source(source=sources['shipment_status'])
        otm_shipment_stop = self.read_source(source=sources['shipment_stop'])
        otm_shipment_stop_remark = self.read_source(source=sources['shipment_stop_remark'])

        freight_rate_val = self.read_source(source=sources['rate_validation'])
        freight_rate_plant_mapping = self.read_source(source=sources['plant_mapping'])
        freight_rate_slr = self.read_source(source=sources['supplemental_lane_rates'])

        dmat = self.read_source(source=sources['dim_material'])
        dmat = self.addColPrefix(dmat, "dmat")

        df_otm_freight = self.dataB_process_otm(
            otm_shipment,
            otm_shipment_cost,
            otm_shipment_refnum,
            otm_shipment_status,
            otm_shipment_stop,
            otm_shipment_stop_remark,
            inv)

        df_freight_rate_estimates = self.dataB_process_std_freight_rates(
                freight_rate_val,
                freight_rate_plant_mapping)

        df_freight_rate_estimates_slr = self.dataB_process_std_freight_rates_slr(
                freight_rate_slr)

        rmr = self.addColPrefix(rmr, "rmr")
        rsc = self.addColPrefix(rsc, "rsc")
        cer = self.addColPrefix(cer, "cer")
        rpttax = self.addColPrefix(rpttax, "rpttax")
        df_freight_rate_estimates = self.addColPrefix(df_freight_rate_estimates, "fre")
        df_freight_rate_estimates_slr = self.addColPrefix(df_freight_rate_estimates_slr, "fre_slr")

        df = inv.select('invoice_date', 'allowed_disc', 'bill_to', 'bus_unit', 'bus_unit_name', 'channel', 'sq_ft', 'line',
                        'caliper', 'charge_desc1', 'currency_code', 'curr_conv', 'extended_amount', 'bill_to_name',
                        'gen_ledg', 'grade', 'grade_desc', 'invoice', 'iptmeta_corrupt_record', 
                        'iptmeta_extract_dttm', 'jde_bill_to', 'jde_ship_to', 'lbs', 'qty', 'length',  'order_format',
                        'plant', 'plant_name', 'salesman', 'salesman_name', 'substrate', 'ship_to', 'ship_to_outs_cvtr', 'width',
                        'price_uom_desc', 'bol_number', 'ship_to_city', 'ship_to_ship_to','qty_uom_desc','end_cust_desc',
                        'form', 'trans_mode', 'rept_month', 'rept_year')

        df = df.join(salesmanexc, [df.salesman == salesmanexc.salesmanexc_salesman], 'left_outer')
        df = df.withColumn('sales_representative', F.coalesce(df.salesmanexc_salesman_name_override, df.salesman_name, F.lit(MISSING_DESC)))
        df = df.withColumnRenamed('salesman', 'sales_rep_id')

        df = df.withColumn("billing_system", F.lit('dataB'))
        # The caliper needs to be calculated before creating the material id
        # because the caliper is used to build the material id.
        df = df.withColumn("caliper", F.col("caliper")*1000.0)
        # remove records with calipers greater >= 200
        df = self.dataB_filter_caliper(df)
        df = self.dataB_material_id(df)
        df = dataB_sale_form(df)

        df = df.withColumn("inv_date", F.col('invoice_date').cast(T.DateType()) )
        df = df.withColumn("inv_month", F.month( F.col('inv_date') ) )
        df = df.withColumn("inv_year", F.year( F.col('inv_date') ) )
        df = df.withColumn("invoice_period", F.date_format(F.col("invoice_date"), "MMyyyy") ) #Change format to MMyyyy
        df = df.withColumn("invoice_period", (df.invoice_period.cast(T.StringType()))[0:6] ) #invoice_period lenght_max 6

        rpttax = rpttax.withColumn("rpttax_plant", F.col('rpttax_plant').cast(T.IntegerType()) )
        rpttax = rpttax.withColumnRenamed("rpttax_grade", "rpttax_grade_code")

        df = df.join(rsc, [df.plant == rsc.rsc_plant,
                            df.grade == rsc.rsc_grade_code,
                            df.caliper == rsc.rsc_caliper,
                            df.sale_form == rsc.rsc_ship_form], 'left_outer')
        df = df.join(rpttax, [df.grade == rpttax.rpttax_grade_code,  df.plant == rpttax.rpttax_plant], 'left_outer')
        df = df.join(rmr, [df.plant == rmr.rmr_plant_id,  df.invoice == rmr.rmr_invoice_id, df.line == rmr.rmr_invoice_line_number], 'left_outer')
        df = df.join(cer, [cer.cer_currency_code_from == df.currency_code, 
                           cer.cer_cur_year == df.inv_year,
                           cer.cer_cur_month == df.inv_month
                        ],'left_outer'
                    )
        # Join for OTM to use estimates only
        df = df.join(df_otm_freight, [df.bol_number == df_otm_freight.bol_number_join], 'left_outer')
        # This code joins the freight rates but includes OTM checks so only
        # records without OTM matches are give values
        df = df.join(df_freight_rate_estimates, [df.plant == df_freight_rate_estimates.fre_plant, 
                                                    F.lower(F.trim(df.ship_to_city)) == F.lower(F.trim(df_freight_rate_estimates.fre_dcity)),
                                                    F.lower(F.trim(df.ship_to_ship_to)) == F.lower(F.trim(df_freight_rate_estimates.fre_dstate)),
                                                    df_otm_freight.freight_rate_per_ton.isNull()], 'left_outer')
        # This code joins the freight rates supplemental lanes but includes OTM checks
        # so only records that don't match OTM and the Freight Rates files are given
        # values
        df = df.join(df_freight_rate_estimates_slr, [df.plant == df_freight_rate_estimates_slr.fre_slr_plant, 
                                                    F.lower(F.trim(df.ship_to_city)) == F.lower(F.trim(df_freight_rate_estimates_slr.fre_slr_dcity)),
                                                    F.lower(F.trim(df.ship_to_ship_to)) == F.lower(F.trim(df_freight_rate_estimates_slr.fre_slr_dstate)),
                                                    df_otm_freight.freight_rate_per_ton.isNull(), 
                                                    df_freight_rate_estimates.fre_estimate_freight_rate_per_ton.isNull()], 'left_outer')
        # Select that includes OTM calculations
        df = df.select(
                df.allowed_disc, df.bill_to, df.bus_unit, df.bus_unit_name, df.charge_desc1, df.caliper, df.channel, 
                df.width, df.length, df.currency_code, df.curr_conv, df.lbs, df.qty, df.material_id, df.sales_rep_id, df.sales_representative, df.billing_system,
                df.gen_ledg, df.extended_amount, df.grade, df.line, df.invoice, df.invoice_date, df.invoice_period,
                df.plant, df.plant_name, df.bill_to_name, df.ship_to_city, df.ship_to_ship_to, df.end_cust_desc, df.rept_month, df.rept_year,
                df.jde_ship_to, df.jde_bill_to, df.ship_to, df.ship_to_outs_cvtr, df.sq_ft, df.bol_number, df.trans_mode,
                rsc.rsc_msf, rmr.rmr_rebate_amount,
                rpttax.rpttax_end_market, rpttax.rpttax_grade_code, rpttax.rpttax_plant, rpttax.rpttax_product_family, 
                rpttax.rpttax_product_group, rpttax.rpttax_product_name, rpttax.rpttax_substrate,
                cer.cer_conversion_rate_multiplier, df.price_uom_desc, df.qty_uom_desc, df_otm_freight.freight_rate_per_ton,
                df_freight_rate_estimates.fre_estimate_freight_rate_per_ton,
                df_freight_rate_estimates_slr.fre_slr_estimate_freight_rate_per_ton
        )

        df = self.dataB_filter_plant(df, exclplnt)
        df = self.dataB_filter_billtoname_jdebillto(df, exclbillto)

        df = df.withColumn('actual_tons', F.coalesce( (F.col('lbs') / 2000.0), F.lit(MISSING_NUMBER) ) )
        df = df.withColumn('fx_conversion_to_usd', F.coalesce(
            F.when((df.currency_code == 'USD') | (df.currency_code == '') | df.currency_code.isNull(), 1)
            .otherwise(df.cer_conversion_rate_multiplier.cast(T.DoubleType())), F.lit(MISSING_NUMBER)))
        df = dataB_adjust_currency_fields(df)
        df = self.dataB_claims(df)
        df = self.dataB_discounts(df)

        # The following code block includes the OTM calculations for actual rates

        # Determine the approach for calculating the Freight Invoice value
        # and fill out the flag.  Includes CPU filtering.
        df = df.withColumn("freight_invoice_calc", 
                F.when(F.lower(F.trim(df.trans_mode)) == F.lit('cpu'), F.lit('actual_cpu'))
                .when(df.freight_rate_per_ton.isNotNull(), F.lit('actual'))
                .when(df.freight_rate_per_ton.isNull() & 
                    df.fre_estimate_freight_rate_per_ton.isNotNull(), F.lit('estimate'))
                .when(df.freight_rate_per_ton.isNull() & 
                    df.fre_estimate_freight_rate_per_ton.isNull() &
                    df.fre_slr_estimate_freight_rate_per_ton.isNotNull(), F.lit('estimate_slr'))
                .otherwise(F.lit(NOT_APPLICABLE_CODE)))

        # Using the flag fill in the freight_invoice value.
        df = df.withColumn("freight_invoice", 
                F.when(df.freight_invoice_calc == F.lit('actual_cpu'), F.lit(0))
                .when(df.freight_invoice_calc == F.lit('actual'), df.freight_rate_per_ton * df.actual_tons)
                .when(df.freight_invoice_calc == F.lit('estimate'), df.fre_estimate_freight_rate_per_ton * df.actual_tons)
                .when(df.freight_invoice_calc == F.lit('estimate_slr'), df.fre_slr_estimate_freight_rate_per_ton * df.actual_tons)
                .otherwise(F.lit(MISSING_NUMBER)))

        df = self.dataB_freight_upcharge(df)
        df = self.dataB_gross_price(df)
        df = df.withColumn('report_month', F.lpad(df.rept_month, 2, '0'))
        df = df.withColumnRenamed('rept_year', 'report_year')
        df = df.withColumn("other_deductions", F.lit(0) )
        df = self.dataB_rebates(df)
        df = df.withColumn("service_allowances", F.lit(0) )
        df = df.withColumn('net_price', F.coalesce((F.col('gross_price')
                                                    + F.col('discounts')
                                                    + F.col('rebates')
                                                    + F.col('claims')
                                                    + F.col('freight_upcharge')
                                                    + F.col('other_deductions')
                                                    + F.col('service_allowances')),
                                                   F.lit(MISSING_NUMBER)))

        df = df.withColumn('cp_channel', F.lit(0))
        df = df.withColumn('cp_mode', F.lit(0) ) 
        df = df.withColumn('cp_sales_region', F.lit(0))

        df = df.withColumnRenamed('invoice',  'invoice_number')
        df = df.withColumnRenamed('line', 'invoice_line_number')
        df = df.withColumnRenamed('plant_name', 'invoice_location')

        df = self.dataB_invoiced_currency(df)
        df = self.dataB_sale_type(df)
        df = self.dataB_ship_from_loc_number(df)

        df = df.withColumn("invoice_dim_location_id", F.expr(hash_columns(['plant'])))
        df = df.withColumn("ship_from_dim_location_id",F.expr(hash_columns(['ship_from_loc_number'])))
        df = df.withColumnRenamed('rpttax_end_market', 'end_market')
        df = prime_enrich(df, quality_class_column=None)
        df = df.withColumn('sales_order_number', F.lit('0'))
        df = df.withColumn("ship1_dim_material_id", F.expr(hash_columns(['billing_system', 'material_id', 'end_market'])))
        df = df.withColumn('ship_dim_customer_id', F.expr(hash_columns(['billing_system', 'jde_ship_to', 'end_cust_desc'])))
        df = df.withColumn('sold_dim_customer_id', F.expr(hash_columns(['billing_system', 'jde_bill_to', 'end_cust_desc'])))
        df = df.withColumn('brand_dim_customer_id', F.lit(MISSING_STRING_ID))

                           # Joining the processed material dimension to retrieve the calculated nominal_basis_weight
        # value to be used to calculate nominal_tons.
        df = df.join(dmat,[df.ship1_dim_material_id == dmat.dmat_dim_material_id],'left_outer')
        df = self.dataB_msf(df)
        df = df.withColumn('nominal_tons', F.coalesce(((df.dmat_nominal_basis_weight * df.msf) / 2000.0), F.lit(MISSING_NUMBER)))
        
        df = df.withColumn('subset', F.coalesce( F.when(df.rpttax_product_group.isNotNull(), df.rpttax_product_group), F.lit(MISSING_DESC) ) )
        df = df.withColumn('commercial_print_channel', F.coalesce(F.when(df.channel.isNotNull(), df.channel), F.lit(MISSING_DESC) ) )
        df = df.withColumn('invoice_location_number', F.coalesce(F.when(df.plant.isNotNull(), df.plant), F.lit(MISSING_NUMBER) ) )

        df = df.withColumn("invoice_source_type", F.lit(NOT_APPLICABLE_CODE))
        df = df.withColumn("invoice_line_code", F.lit(NOT_APPLICABLE_CODE))
        df = df.withColumn('iptmeta_source_system', F.lit('dataB'))
        df = self.dataB_product_sold_flag(df)
        df = df.withColumn("commercial_print_mode", F.lit(NOT_APPLICABLE_DESC))
        df = df.withColumn("commercial_print_region", F.lit(NOT_APPLICABLE_DESC))
        df = df.withColumnRenamed("qty","invoice_volume")
        df = df.withColumnRenamed("qty_uom_desc","invoice_uom_id")
        df = df.withColumn('standard_cost', F.coalesce(F.when(df.rsc_msf.isNotNull(), df.rsc_msf) * df.msf, F.lit(MISSING_NUMBER)))
        df = df.withColumn('standard_gross_margin', F.coalesce((df.net_price - (df.standard_cost + df.freight_invoice)), F.lit(MISSING_NUMBER)))
        df = df.withColumn('invoice_line_desc_1', F.lit(NOT_APPLICABLE_CODE))

        df = df.select(
            df.billing_system, 
            df.invoice_number, 
            df.invoice_line_number, 
            df.invoice_period, 
            df.invoice_source_type,
            df.invoice_line_code, 
            df.iptmeta_source_system, 
            df.product_sold_flag,
            df.commercial_print_channel, 
            df.commercial_print_mode,
            df.fx_conversion_to_usd, 
            df.grade, 
            df.invoice_date, 
            df.ship_from_dim_location_id,
            df.invoiced_currency,
            df.ship1_dim_material_id,
            df.prime,
            df.sales_order_number,
            df.sale_type, 
            df.sales_representative, 
            df.ship_dim_customer_id,
            df.sold_dim_customer_id,
            df.brand_dim_customer_id,
            df.subset, 
            df.actual_tons, 
            df.claims, 
            df.discounts, 
            df.freight_invoice,
            df.freight_invoice_calc,
            df.freight_upcharge, 
            df.gross_price, 
            df.msf, 
            df.net_price, 
            df.nominal_tons, 
            df.other_deductions,
            df.rebates, 
            df.service_allowances, 
            df.standard_cost, 
            df.standard_gross_margin,
            df.invoice_dim_location_id,
            df.commercial_print_region,
            df.invoice_volume,
            df.invoice_uom_id,
            df.bol_number,
            df.report_month,
            df.report_year,
            df.sales_rep_id,
            df.invoice_line_desc_1
        ).distinct()

        return df

    def invoice_dataframe(self, invoice_source):
        inv = self.read_source(source=invoice_source)
        convert_date = F.udf(
            lambda xdate: datetime.datetime.strptime(xdate, '%Y%m%d') if len(xdate) == 8 else datetime.datetime(1, 1, 1,
                                                                                                                0, 0),
            T.DateType())
        inv = inv.withColumnRenamed('invoice_date', 'invoice_date_original')
        inv = inv.withColumn('invoice_date', convert_date(
            inv.invoice_date_original.cast(T.StringType())))  # Invoice_date must be 10 or more
        # Filter the invoice dataframe to simplify later processing
        inv = self.dataB_filter_plantvals(inv)
        inv = self.dataB_filter_report_date(inv)
        return inv
