from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DoubleType,
    DecimalType,
    BooleanType,
    LongType,
    DataType,
    ShortType,
    ByteType
)
from typing import Type, get_type_hints, Union
from dataclasses import is_dataclass, fields

class LongT:
    pass

class ShortT:
    pass

class ByteT:
    pass

class BoundDecimal:
    """
    Custom data type that represents a decimal with a specific scale and precision.
    """

    def __init__(self, precision: int, scale: int):
        self.precision = precision
        self.scale = scale

    def __repr__(self) -> str:
        return f"BoundDecimal(precision={self.precision}, scale={self.scale})"


def create_bound_decimal_type(precision: int, scale: int) -> Type[BoundDecimal]:
    """
    Factory method that creates a new BoundDecimal type with the specified precision and scale.
    """
    class _BoundDecimal(BoundDecimal):
        pass

    _BoundDecimal.__name__ = f"BoundDecimal_{precision}_{scale}"
    _BoundDecimal.precision = precision
    _BoundDecimal.scale = scale

    return _BoundDecimal

def map_dataclass_to_struct(dataclass_type: Type) -> StructType:
    """
    Map a Python data class to a PySpark struct.

    :param dataclass_type: The Python data class to be mapped.
    :return: A PySpark struct that corresponds to the data class.
    """
    fields_list = []
    hints = get_type_hints(dataclass_type)

    for field in fields(dataclass_type):
        field_name = field.name
        field_type = field.type

        if is_dataclass(field_type):
            # Recursively map nested data classes to PySpark structs
            sub_struct = map_dataclass_to_struct(field_type)
            nullable = is_field_nullable(field_name, hints)
            fields_list.append(StructField(field_name, sub_struct, nullable))
        elif hasattr(field_type, "__origin__") and field_type.__origin__ is list:
            # Handle lists of elements
            elem_type = field_type.__args__[0]
            if is_dataclass(elem_type):
                # Handle lists of data classes
                sub_struct = map_dataclass_to_struct(elem_type)
                nullable = is_field_nullable(field_name, hints)
                fields_list.append(StructField(field_name, ArrayType(sub_struct), nullable))
            else:
                # Handle lists of primitive types
                spark_type = get_spark_type(elem_type)
                nullable = is_field_nullable(field_name, hints)
                fields_list.append(StructField(field_name, spark_type, nullable))
        else:
            # Handle primitive types and BoundDecimal custom type
            spark_type = get_spark_type(field_type)
            nullable = is_field_nullable(field_name, hints)
            fields_list.append(StructField(field_name, spark_type, nullable))

    return StructType(fields_list)


def is_field_nullable(field_name: str, hints: dict) -> bool:
    """
    Returns True if the given field name is nullable, based on the type hint for the field in the given hints dictionary.
    """
    if field_name not in hints:
        return True
    field_type = hints[field_name]
    if is_optional_type(field_type):
        return True
    return False


def apply_nullability(dtype: DataType, is_nullable: bool) -> DataType:
    """
    Returns a new PySpark DataType with the nullable flag set to the given value.
    """
    if is_nullable:
        if isinstance(dtype, StructType):
            # Wrap the nullable field in a struct with a single field
            return StructType([StructField("value", dtype, True)])
        elif hasattr(dtype, 'add_nullable'):
            return dtype.add_nullable()
        else:
            raise TypeError(f"Type {dtype} does not support nullability")
    else:
        return dtype


def get_spark_type(py_type: Type) -> DataType:
    """
    Creates a mapping from a python type to a pyspark data type
    :param py_type:
    :return:
    """
    if py_type == str:
        return StringType()
    elif py_type == int:
        return IntegerType()
    elif py_type == LongT:
        return LongType()
    elif py_type == ShortT:
        return ShortType()
    elif py_type == ByteT:
        return ByteType()
    elif py_type == float:
        return DoubleType()
    elif py_type == bool:
        return BooleanType()
    elif isinstance(py_type, type) and issubclass(py_type, BoundDecimal):
        return DecimalType(precision=py_type.precision, scale=py_type.scale)
    elif is_optional_type(py_type):
        elem_type = py_type.__args__[0]
        spark_type = get_spark_type(elem_type)
        return spark_type
    else:
        raise Exception(f"Type {py_type} is not supported by PySpark")


def is_optional_type(py_type: Type) -> bool:
    """
    Returns True if the given type is an Optional type.
    """
    if hasattr(py_type, "__origin__") and py_type.__origin__ is Union:
        args = py_type.__args__
        if len(args) == 2 and args[1] is type(None):
            return True
    return False
