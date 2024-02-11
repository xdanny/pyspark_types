import datetime

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DecimalType,
    ArrayType,
    DateType,
)
from typing import List, Optional
from dataclasses import dataclass

from pyspark_types.dataclass import map_dataclass_to_struct, LongT
from pyspark_types.auxiliary import create_bound_decimal_type

decimal = create_bound_decimal_type(10, 2)


@dataclass
class InnerDataClass:
    id: Optional[LongT]
    name: Optional[str]
    price: decimal


@dataclass
class OuterDataClass:
    field1: int
    field2: Optional[str]
    field3: List[InnerDataClass]
    field4: datetime.date


def test_map_simple_dataclass():
    expected_struct = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("price", DecimalType(10, 2), False),
        ]
    )
    result_struct = map_dataclass_to_struct(InnerDataClass)
    assert result_struct == expected_struct


def test_map_dataclass_with_list_of_dataclasses():
    expected_struct = StructType(
        [
            StructField("field1", IntegerType(), False),
            StructField("field2", StringType(), True),
            StructField(
                "field3",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType(), True),
                            StructField("name", StringType(), True),
                            StructField("price", DecimalType(10, 2), False),
                        ]
                    )
                ),
                False,
            ),
            StructField("field4", DateType(), False),
        ]
    )
    result_struct = map_dataclass_to_struct(OuterDataClass)
    assert result_struct == expected_struct
