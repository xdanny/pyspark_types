from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, ArrayType
from typing import List, Optional, Dict
from dataclasses import dataclass

from pyspark_types.dataclass import map_dataclass_to_struct, create_bound_decimal_type, LongT

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

def test_map_simple_dataclass():
    expected_struct = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("price", DecimalType(10, 2), False)
    ])
    result_struct = map_dataclass_to_struct(InnerDataClass)
    assert result_struct == expected_struct

def test_map_dataclass_with_list_of_dataclasses():
    expected_struct = StructType([
        StructField("field1", IntegerType(), False),
        StructField("field2", StringType(), True),
        StructField("field3", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("price", DecimalType(10, 2), False)
        ])), False)
    ])
    result_struct = map_dataclass_to_struct(OuterDataClass)
    print(result_struct)
    assert result_struct == expected_struct
