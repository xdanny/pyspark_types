from pyspark.sql.types import (
    DecimalType,
    Row,
    BooleanType,
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    ArrayType,
)
from typing import List, Optional
from pyspark_types.auxiliary import create_bound_decimal_type

from pyspark_types.dataclass import LongT
from pyspark_types.pydantic import PySparkBaseModel
import pytest
import os


decimal = create_bound_decimal_type(10, 2)

os.environ['PYSPARK_GATEWAY_ENABLED'] = '0'


class Address(PySparkBaseModel):
    street: str
    city: str
    state: str
    zip: str


class Person(PySparkBaseModel):
    name: Optional[str]
    age: LongT
    height: float
    is_employed: bool
    salary: float
    decimal: decimal
    addresses: List[Address]


def test_person_spark_schema():
    schema = Person.schema()
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", LongType(), False),
            StructField("height", DoubleType(), False),
            StructField("is_employed", BooleanType(), False),
            StructField("salary", DoubleType(), False),
            StructField("decimal", DecimalType(10, 2), False),
            StructField(
                "addresses",
                ArrayType(
                    StructType(
                        [
                            StructField("street", StringType(), False),
                            StructField("city", StringType(), False),
                            StructField("state", StringType(), False),
                            StructField("zip", StringType(), False),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )

    assert schema == expected_schema

@pytest.mark.usefixtures("spark_local")
def test_to_dataframe_from_pydantic(spark_local):
    data = [
        Person(
            name="John",
            age=30,
            height=1.80,
            is_employed=True,
            salary=1000.00,
            decimal=decimal(100.00),
            addresses=[
                Address(street="123 Main St", city="Anytown", state="NY", zip="12345"),
                Address(street="456 Main St", city="Anytown", state="NY", zip="12345"),
            ],
        ),
        Person(
            name="Jane",
            age=25,
            height=1.60,
            is_employed=False,
            salary=0.00,
            decimal=decimal(0.00),
            addresses=[],
        ),
    ]
    df = Person.create_spark_dataframe(data, spark_local)

    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", LongType(), False),
            StructField("height", DoubleType(), False),
            StructField("is_employed", BooleanType(), False),
            StructField("salary", DoubleType(), False),
            StructField("decimal", DecimalType(10, 2), False),
            StructField(
                "addresses",
                ArrayType(
                    StructType(
                        [
                            StructField("street", StringType(), False),
                            StructField("city", StringType(), False),
                            StructField("state", StringType(), False),
                            StructField("zip", StringType(), False),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )

    assert df.schema == expected_schema
    assert df.count() == 2
