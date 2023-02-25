from pyspark.sql.types import DecimalType, Row, BooleanType, StructType, StructField, StringType, DoubleType, LongType, ArrayType
from typing import List, Optional
from pyspark_types.auxiliary import create_bound_decimal_type

from pyspark_types.dataclass import LongT
from pyspark_types.pydantic import PySparkBaseModel

decimal = create_bound_decimal_type(10, 2)

class Person(PySparkBaseModel):
    name: Optional[str]
    age: LongT
    height: float
    is_employed: bool
    salary: float
    decimal: decimal
    addresses: List[str]


def test_person_spark_schema():
    schema = Person.schema()
    print(schema)
    expected_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", LongType(), False),
        StructField("height", DoubleType(), False),
        StructField("is_employed", BooleanType(), False),
        StructField("salary", DoubleType(), False),
        StructField("decimal", DecimalType(10, 2), False),
        StructField("addresses", ArrayType(StringType(), True), False)
    ])
    assert schema == expected_schema


def test_person_to_spark_row():
    person = Person(name="John", age=LongT(30), height=5.5, is_employed=True, salary=10000.00, decimal=2.5, addresses=["123 Main St", "456 Oak St"])
    row = person.to_row()
    print(row)
    assert row["name"] == "John"
    assert row["age"] == 30
    assert row["height"] == 5.5
    assert row["is_employed"] == True
    assert row["salary"] == 10000.00
    assert row["decimal"] == decimal('2.5')
    assert row["addresses"] == ["123 Main St", "456 Oak St"]


def test_person_from_spark_row():
    row = Row(name='John',
              age=LongT(30),
              height=5.5,
              is_employed=True,
              salary=10000.0,
              decimal=decimal('2.5'),
              addresses=['123 Main St', '456 Oak St'])

    person = Person.from_row(row)
    assert person.name == "John"
    assert person.age == 30
    assert person.height == 5.5
    assert person.is_employed == True
    assert person.salary == 10000.00
    assert person.decimal == 2.5
    assert person.addresses == ["123 Main St", "456 Oak St"]
