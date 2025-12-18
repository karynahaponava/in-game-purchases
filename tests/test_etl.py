import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../etl')))

from etl_job import transform_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("UnitTesting") \
        .getOrCreate()

def test_transform_logic(spark):
    data = [("event_1", "game_1", "user_1", "prod_1", 100.0, 2, "2023-01-01")]
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("game_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])
    input_df = spark.createDataFrame(data, schema)

    result_df = transform_data(input_df)
    
    results = result_df.collect()
    row = results[0]

    assert "purchase_id" in result_df.columns
    assert "event_id" not in result_df.columns
    
    assert row.total_amount == 200.0
    
    assert row.purchase_id == "event_1"