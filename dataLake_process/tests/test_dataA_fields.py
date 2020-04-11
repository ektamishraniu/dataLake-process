import pytest
from transforms import dataB_adjust_currency_fields
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

STRUCT_TYPE = StructType(
    [StructField('currency_code', StringType(), True), StructField('curr_conv', DoubleType(), True),
     StructField('fx_conversion_to_usd', DoubleType(), True), StructField('extended_amount', DoubleType(), True),
     StructField('allowed_disc', DoubleType(), True)])


def test_dataB_conversion_null(spark_session: SparkSession):
    base_data = {'currency_code': None, 'curr_conv': None, 'fx_conversion_to_usd': None,
                 'extended_amount': None, 'allowed_disc': None}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount is None
    assert actual.adj_allowed_disc is None


def test_dataB_conversion_curr_conv_null(spark_session: SparkSession):
    base_data = {'currency_code': 'CAD', 'curr_conv': None, 'fx_conversion_to_usd': 0.75,
                 'extended_amount': 6417.34, 'allowed_disc': 64.17}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount == 0
    assert actual.adj_allowed_disc == 0

def test_dataB_conversion_curr_conv_null(spark_session: SparkSession):
    base_data = {'currency_code': 'CAD', 'curr_conv': 0.0, 'fx_conversion_to_usd': 0.75,
                 'extended_amount': 6417.34, 'allowed_disc': 64.17}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount == 0
    assert actual.adj_allowed_disc == 0


def test_dataB_conversion_cer_conversion_rate_multiplier_null(spark_session: SparkSession):
    base_data = {'currency_code': 'CAD', 'curr_conv': 0.73, 'fx_conversion_to_usd': None,
                 'extended_amount': 6417.34, 'allowed_disc': 64.17}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount == 0
    assert actual.adj_allowed_disc == 0

def test_dataB_conversion_currency_code_null(spark_session: SparkSession):
    base_data = {'currency_code': None, 'curr_conv': 0.73, 'fx_conversion_to_usd': 0.75,
                 'extended_amount': 6417.34, 'allowed_disc': 64.17}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount == 6417.34
    assert actual.adj_allowed_disc == 64.17


def test_dataB_currency_code_USD(spark_session: SparkSession):
    base_data = {'currency_code': 'USD', 'curr_conv': 0.73, 'fx_conversion_to_usd': 0.75,
                 'extended_amount': 6417.34, 'allowed_disc': 64.17}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount == 6417.34
    assert actual.adj_allowed_disc == 64.17


def test_dataB_currency_code_empty(spark_session: SparkSession):
    base_data = {'currency_code': '', 'curr_conv': 0.73, 'fx_conversion_to_usd': 0.75,
                 'extended_amount': 6417.34, 'allowed_disc': 64.17}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount == 6417.34
    assert actual.adj_allowed_disc == 64.17


def test_dataB_extended_amount_null(spark_session: SparkSession):
    base_data = {'currency_code': 'CAD', 'curr_conv': 0.73, 'fx_conversion_to_usd': 0.75,
                 'extended_amount': None, 'allowed_disc': 64.17}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount is None
    assert actual.adj_allowed_disc == 65.92808219178082


def test_dataB_allowed_disc_null(spark_session: SparkSession):
    base_data = {'currency_code': 'CAD', 'curr_conv': 0.73, 'fx_conversion_to_usd': 0.75,
                 'extended_amount': 6417.34, 'allowed_disc': None}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount == 6593.157534246575
    assert actual.adj_allowed_disc is None


def test_dataB_valid_record(spark_session: SparkSession):
    base_data = {'currency_code': 'CAD', 'curr_conv': 0.73, 'fx_conversion_to_usd': 0.75,
                 'extended_amount': 6417.34, 'allowed_disc': 64.17}
    actual = run_basics(base_data, spark_session)
    assert actual.adj_extended_amount == 6593.157534246575
    assert actual.adj_allowed_disc == 65.92808219178082


def run_basics(base_data, spark_session):
    df = spark_session.createDataFrame([Row(**base_data)], STRUCT_TYPE)
    df.show()
    assert 1 == df.count()
    df = dataB_adjust_currency_fields(df)
    assert 1 == df.count()
    assert 7 == len(df.columns)
    df.show()
    actual = df.select(df.columns).first()
    return actual
