"""
Process job flow for Mill Profitability Fact Invoice.
"""
from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window as W
from spark_process_common.process import BaseProcess, MISSING_DESC, MISSING_STRING_ID, NOT_APPLICABLE_CODE, NOT_APPLICABLE_DESC, MISSING_NUMBER
from spark_process_common.transforms import hash_columns
from transforms import dataA_sales_rep_override, prime_enrich, fixCurExchangeToAvg, lstrip_0

class ProcessFactInvoicedataA(BaseProcess):


    # this id get's inserted into dim_material on transfers into redshift using the DML scripts. There is a business rule that
    # states when an invoice has weight_qty/actual_tons == 0, then that invoice record should be associated 
    # to a "non-product" material_id. This record 
    NON_PRODUCT_RECORD_MATERIAL_ID = '~0000000000~'

    def transform(self, sources: dict) -> DataFrame:
        ri = self.invoice_dataframe(sources['rptt_invoice'])

        rst = self.read_source(source=sources['rptm_sbu_subset_txt'])

        cmf = self.read_source(source=sources['customer_mapping'])
        cmf = cmf.withColumnRenamed('sales_rep', 'sales_rep_override')
        cmf = cmf.withColumnRenamed('sales_rep_id', 'sales_rep_id_override')
        cmf = cmf.withColumnRenamed('end_market', 'cmf_end_market')

        mmf = self.read_source(source=sources['material_mapping'])
        srtr = self.read_source(source=sources['sales_rep_to_region'])

        rsrt = self.read_source(source=sources['rptm_sales_rep_txt'])
        rsrt = rsrt.withColumnRenamed('med_desc', 'sales_rep_original')

        edataA = self.read_source(source=sources['exclusion_dataA'])
        edataA = edataA.withColumnRenamed('sold_customer_id', 'edataA_sold_customer_id')

        # Source contains system_id/material_id pairs that need excluded
        excmat = self.read_source(source=sources['exclude_mat'])

        cerd = self.read_source(source=sources['currency_exchange_rates'])
        cerd = fixCurExchangeToAvg(self, cerd)
        cers = cerd.select('currency_code_from','cur_year','cur_month','conversion_rate_multiplier')
        cers = cers.withColumnRenamed('currency_code_from', 'std_currency_code_from')
        cers = cers.withColumnRenamed('cur_year',  'std_cur_year')
        cers = cers.withColumnRenamed('cur_month', 'std_cur_month')
        cers = cers.withColumnRenamed('conversion_rate_multiplier', 'std_conversion_rate_multiplier')

        dcust_sold = self.read_source(source=sources['dim_customer'])
        dcust_sold = dcust_sold.withColumnRenamed('dim_customer_id', 'sold_dim_customer_id')

        dcust_ship = self.read_source(source=sources['dim_customer'])
        dcust_ship = dcust_ship.withColumnRenamed('dim_customer_id', 'ship_dim_customer_id')

        dcust_brand = self.read_source(source=sources['dim_customer'])
        dcust_brand = dcust_brand.withColumnRenamed('dim_customer_id', 'brand_dim_customer_id')

        dloc_ship = self.read_source(source=sources['dim_location'])
        dloc_ship = dloc_ship.withColumnRenamed('dim_location_id', 'ship_from_dim_location_id')

        dloc_inv = self.read_source(source=sources['dim_location'])
        dloc_inv = dloc_inv.withColumnRenamed('dim_location_id', 'invoice_dim_location_id')

        dmat = self.read_source(source=sources['dim_material'])
        dmat = dmat.withColumnRenamed('dim_material_id', 'ship1_dim_material_id')

        df = (
            ri.join(excmat, [excmat.material_id==ri.ship1_material_id_int, excmat.system==ri.system_id] , 'left_anti')
            .join(rst, [rst.sbu_subset_id == ri.sbu_subset_id], 'left_outer')
            .join(mmf, [mmf.material == ri.mmf_material], 'left_outer')
            .join(dmat, [dmat.billing_system == ri.system_id,
                         dmat.material_id == ri.ship_mat1_id,
                         dmat.end_market_or_prime == F.when(
                             ri.prime_flag == 1, 'Prime').otherwise('Non-Prime')], 'left_outer')
            .join(cmf, [F.upper(F.trim(cmf.sold_to_ship_to)) == ri.commercial_print_customer_key, F.upper(F.trim(cmf.cmf_end_market)) == F.upper(dmat.end_market)], 'left_outer')
            .join(srtr, [srtr.sales_rep_id == cmf.sales_rep_id_override], 'left_outer')
            .join(
                cerd, 
                [
                    cerd.currency_code_from == ri.currency_id,
                    cerd.cur_year == ri.inv_year,
                    cerd.cur_month == ri.inv_month
                ],
                 'left_outer'
            )
            .join(
                cers, 
                [
                    cers.std_currency_code_from == ri.std_cost_currency_id,
                    cers.std_cur_year == ri.inv_year,
                    cers.std_cur_month == ri.inv_month
                ],
                 'left_outer'
            )            
            .join(dcust_sold,
                  [dcust_sold.billing_system == ri.system_id,
                   dcust_sold.customer_id == ri.sold_customer_id],
                  'left_outer')
            .join(dcust_ship,
                  [dcust_ship.billing_system == ri.system_id,
                   dcust_ship.customer_id == ri.ship_customer_id],
                  'left_outer')
            .join(dcust_brand,
                  [dcust_brand.billing_system == ri.system_id,
                   dcust_brand.customer_id == ri.brand_owner],
                  'left_outer')
            .join(dloc_ship, [dloc_ship.location_id == ri.ship_location_id], 'left_outer')
            .join(dloc_inv, [dloc_inv.location_id == ri.mfg_location_id], 'left_outer')
            .join(edataA, [edataA.edataA_sold_customer_id == ri.sold_customer_id_lstrip_0, ri.system_id == 'S3', ri.rev_acct_id == 'R6000'], 'left_anti')
            .join(rsrt, [rsrt.sales_rep_id == ri.ri_sales_rep_id], 'left_outer')
            .select(
                ri.system_id, ri.invoice_id, ri.line_number, ri.month, ri.source_type, ri.rev_acct_id,
                ri.weight_qty, ri.currency_id, ri.std_cost_currency_id, ri.inv_date, ri.quality_class, ri.sale_type, ri.invoice_line_value,
                ri.line_qty, ri.invoice_uom_id, ri.inv_line_std_cost, ri.period, ri.year, ri.sales_order,
                ri.ri_sales_rep_id, ri.line_desc1, rst.med_desc, mmf.cp_subset, cmf.channel, cmf.drop_ship_into_stock,
                cmf.sales_rep_override, cmf.cmf_end_market, cmf.sales_rep_id_override,
                cerd.conversion_rate_multiplier, cers.std_conversion_rate_multiplier,
                dmat.ship1_dim_material_id, dmat.product_code, dmat.force_product_code,
                dmat.nominal_basis_weight, dmat.material_id, dmat.end_market,
                dloc_ship.ship_from_dim_location_id,
                dloc_inv.invoice_dim_location_id,
                dcust_ship.ship_dim_customer_id,
                dcust_sold.sold_dim_customer_id,
                dcust_brand.brand_dim_customer_id,
                rsrt.sales_rep_original,
                srtr.region, ri.invoice_volume
            )
        )

        df = df.where("case when system_id = 'S3' then product_code else '~' end not in ('SC', 'CR')")

        df = df.withColumn('iptmeta_source_system', F.lit('dataA'))
        df = df.withColumn('bol_number', F.lit(MISSING_NUMBER))

        df = df.withColumn(
            'product_sold_flag',
            F.when((df.weight_qty.isNull()) | (df.weight_qty == 0), F.lit('N')).otherwise(F.lit('Y'))
        )

        df = df.withColumn(
            'fx_conversion_to_usd',
            F.coalesce(
                F.when(df.currency_id == 'USD', 1)
                .otherwise(df.conversion_rate_multiplier.cast(T.DoubleType())),
                F.lit(MISSING_NUMBER)
            )
        )

        df = df.withColumn(
            'std_fx_conversion_to_usd',
            F.coalesce(
                F.when(df.std_cost_currency_id == 'USD', 1)
                .otherwise(df.std_conversion_rate_multiplier.cast(T.DoubleType())),
                F.lit(MISSING_NUMBER)
            )
        )

        df = df.withColumn('grade', df.product_code)

        df = df.withColumn('invoice_date', F.to_date(df.inv_date))

        df = prime_enrich(df)

        df = df.withColumn('sales_order_number', F.coalesce(df.sales_order, F.lit('0')))

        df = df.withColumn(
            'sale_type',
            F.when(df.sale_type == 'I', F.lit('Internal'))
            .when(df.sale_type == 'E', F.lit('External'))
            .otherwise(df.sale_type)
        )

        df = df.withColumn('subset', F.coalesce(df.cp_subset, df.med_desc, F.lit(NOT_APPLICABLE_DESC)))

        df = (
            df.withColumn('claims', F.when(df.rev_acct_id.isin('R4900', 'R4350'), df.invoice_line_value * df.fx_conversion_to_usd).otherwise(MISSING_NUMBER))
            .withColumn('discounts', F.when(df.rev_acct_id.isin('R4500'), df.invoice_line_value * df.fx_conversion_to_usd).otherwise(MISSING_NUMBER))
            .withColumn("freight_invoice_calc", F.lit('actual'))
            .withColumn('freight_invoice', F.when(df.rev_acct_id.isin('R8200'), df.invoice_line_value * df.fx_conversion_to_usd).otherwise(MISSING_NUMBER))
            .withColumn('freight_upcharge', F.when(df.rev_acct_id.isin('R0300'), df.invoice_line_value * df.fx_conversion_to_usd).otherwise(MISSING_NUMBER))
            .withColumn('gross_price', F.when(df.rev_acct_id.isin('R0100', 'R0500', 'R0700', 'R0105'), df.invoice_line_value * df.fx_conversion_to_usd).otherwise(MISSING_NUMBER))
            .withColumn('other_deductions', F.when(df.rev_acct_id.isin('R5300'), df.invoice_line_value * df.fx_conversion_to_usd).otherwise(MISSING_NUMBER))
            .withColumn('standard_cost', F.coalesce(df.inv_line_std_cost * df.std_fx_conversion_to_usd, F.lit(MISSING_NUMBER)))
            .withColumn('rebates', F.when(df.rev_acct_id.isin('R4110', 'R4130'), df.invoice_line_value * df.fx_conversion_to_usd).otherwise(MISSING_NUMBER))
            # TODO Confirm exclusions and/or data predicate should be here
            .withColumn('service_allowances', F.when(df.rev_acct_id.isin('R6000'), df.invoice_line_value * df.fx_conversion_to_usd).otherwise(MISSING_NUMBER))
        )

        df = df.withColumn(
            'msf',
            F.when(df.invoice_uom_id == 'MSF', df.line_qty)
            .when(df.invoice_uom_id == 'M2', df.line_qty * .0107639)
            .otherwise(0)
        )

        df = df.withColumn('nominal_tons', df.nominal_basis_weight * df.msf / 2000)

        df = df.withColumn(
            'net_price',
            df.gross_price + df.discounts + df.rebates + df.claims + df.freight_upcharge + df.other_deductions + df.service_allowances
        )

        df = df.withColumn('standard_gross_margin', df.net_price - (df.standard_cost + df.freight_invoice))

        df = dataA_sales_rep_override(df)
        df = df.withColumn('sales_rep_id', F.coalesce(df.sales_rep_id_override, df.ri_sales_rep_id, F.lit(MISSING_NUMBER)))

        df = (
            df.withColumn('ship_from_dim_location_id', F.coalesce(df.ship_from_dim_location_id, F.lit(MISSING_STRING_ID)))
            .withColumn('invoice_dim_location_id', F.coalesce(df.invoice_dim_location_id, F.lit(MISSING_STRING_ID)))
            .withColumn('ship1_dim_material_id', F.coalesce(df.ship1_dim_material_id, F.lit(MISSING_STRING_ID)))
            .withColumn('channel', F.coalesce(df.channel, F.lit(MISSING_DESC)))
            .withColumn('drop_ship_into_stock', F.coalesce(df.drop_ship_into_stock, F.lit(MISSING_DESC)))
            .withColumn('region', F.coalesce(df.region, F.lit(MISSING_DESC)))
            .withColumn('ship_dim_customer_id', F.coalesce(df.ship_dim_customer_id, F.lit(MISSING_STRING_ID)))
            .withColumn('sold_dim_customer_id', F.coalesce(df.sold_dim_customer_id, F.lit(MISSING_STRING_ID)))
            .withColumn('brand_dim_customer_id', F.coalesce(df.brand_dim_customer_id, F.lit(MISSING_STRING_ID)))
            .withColumn('invoice_period', F.lpad(df.month, 6, '0'))
        )

        df = (
            df.withColumnRenamed('system_id', 'billing_system')
            .withColumnRenamed('rev_acct_id', 'invoice_line_code')
            .withColumnRenamed('invoice_id', 'invoice_number')
            .withColumnRenamed('line_number', 'invoice_line_number')
            .withColumnRenamed('source_type', 'invoice_source_type')
            .withColumnRenamed('channel', 'commercial_print_channel')
            .withColumnRenamed('drop_ship_into_stock', 'commercial_print_mode')
            .withColumnRenamed('region', 'commercial_print_region')
            .withColumnRenamed('currency_id', 'invoiced_currency')
            .withColumnRenamed('weight_qty', 'actual_tons')
            .withColumnRenamed('period', 'report_month')
            .withColumnRenamed('year', 'report_year')
            .withColumnRenamed('line_desc1', 'invoice_line_desc_1')
        )

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
        )

        return df

    def invoice_dataframe(self, invoice_source):
        """
            Fact Invoice records and attributes from dataA Sources
        """
        ri = (
            self.read_source(source=invoice_source)
                .where("business_unit_id == '10'")
                .where("sale_type in ('I', 'E')")
                .where("system_id not in ('SA', '30')")
                # TODO Add year/period filter into config
                .where("concat(year,period) > '201609'")
        )
        ri = ri.withColumn('iptmeta_source_system', F.lit('dataA'))
        ri = ri.withColumn(
            'ship1_material_id_int',
            F.when(ri.ship_mat1_id.rlike('[^0-9]+'), F.lit(None)).otherwise(ri.ship_mat1_id.cast(T.IntegerType()))
        )

        lstrip_0_udf = lstrip_0()
        ri = ri.withColumn('sold_customer_id_lstrip_0', lstrip_0_udf(ri.sold_customer_id))
        ri = ri.withColumn('ship_customer_id_lstrip_0', lstrip_0_udf(ri.ship_customer_id))

        # Strip leading zeros from numeric material_id's
        ri = ri.withColumn('mmf_material',
                           F.concat(ri.system_id, F.lit('/'), F.coalesce(ri.ship1_material_id_int, ri.ship_mat1_id)))
        ri = ri.withColumn(
            'commercial_print_customer_key',
            F.concat(ri.system_id, F.lit('/'), ri.sold_customer_id_lstrip_0, ri.system_id, F.lit('/'),
                     ri.ship_customer_id_lstrip_0)
        )
        ri = ri.withColumn("inv_date", F.col('inv_date').cast(T.DateType()))
        ri = ri.withColumn("inv_month", F.month(F.col('inv_date')))
        ri = ri.withColumn("inv_year", F.year(F.col('inv_date')))
        ri = ri.withColumn('invoice_volume', F.coalesce(ri.line_qty, F.lit(MISSING_NUMBER)))
        ri = ri.withColumn('invoice_uom_id', F.coalesce(ri.invoice_uom_id, F.lit(MISSING_STRING_ID)))
        ri = ri.withColumnRenamed('sales_rep_id', 'ri_sales_rep_id')
        # some lines have multiple quality class values so if any are prime we treat the whole line as GOOD
        ri = ri.withColumn('prime_flag',
                           F.max(F
                                 .when(F.isnull(ri.quality_class), 1)
                                 .when(ri.quality_class == 'GOOD', 1)
                                 .otherwise(0))
                           .over(W.partitionBy(ri.system_id, ri.invoice_id, ri.line_number)))
        ri = ri.withColumn('quality_class', F.when(ri.prime_flag == 1, F.lit('GOOD')).otherwise(ri.quality_class))
        return ri
