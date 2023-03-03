import pytest
import os
from pyspark.sql import SparkSession


@pytest.fixture
def spark_local():
    os.environ['PYSPARK_GATEWAY_ENABLED'] = '0'
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
