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

decimal = create_bound_decimal_type(10, 2)


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
    print(schema)
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
