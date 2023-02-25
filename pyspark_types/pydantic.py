from typing import Type, Union, get_type_hints, get_origin, get_args
from pydantic import BaseModel
from pyspark.sql.types import *
from pyspark_types.dataclass import is_field_nullable
from pyspark_types.auxiliary import LongT, ShortT, ByteT, BoundDecimal



class PySparkBaseModel(BaseModel):
    """
    Base class for PySpark models. Provides methods for converting between PySpark Rows and Pydantic models.
    """

    class Config:
        arbitrary_types_allowed = True
        validate_assignment = True
        validate_all = True

    def to_row(self):
        return Row(**self.dict())

    @classmethod
    def from_row(cls, row):
        data = row.asDict()
        return cls(**data)

    def dict(self, *args, **kwargs):
        return super().dict(*args, **kwargs, by_alias=True, exclude_unset=True)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    @classmethod
    def schema(cls: Type[BaseModel]) -> StructType:
        """
        Map a Pydantic model to a PySpark struct.

        :param model_type: The Pydantic model to be mapped.
        :return: A PySpark struct that corresponds to the Pydantic model.
        """
        fields_list = []
        hints = get_type_hints(cls)

        for field_name, _ in cls.__fields__.items():
            field_type = hints[field_name]  # Unwrap Pydantic's Optional and List types

            if isinstance(field_type, BaseModel):
                # Recursively map nested Pydantic models to PySpark structs
                sub_struct = cls.map_pydantic_model_to_struct(field_type)
                nullable = field_type.__config__.allow_population_by_field_name or is_field_nullable(field_name, hints)
                fields_list.append(StructField(field_name, sub_struct, nullable))
            elif isinstance(field_type, list):
                # Handle lists of elements
                elem_type = field_type.__args__[0]
                if isinstance(elem_type, BaseModel):
                    # Handle lists of Pydantic models
                    sub_struct = cls.map_pydantic_model_to_struct(elem_type)
                    nullable = field_type.__origin__ is Union or is_field_nullable(field_name, hints)
                    fields_list.append(StructField(field_name, ArrayType(sub_struct), nullable))
                else:
                    # Handle lists of primitive types
                    spark_type = cls.get_spark_type(elem_type)
                    nullable = field_type.__origin__ is Union or is_field_nullable(field_name, hints)
                    fields_list.append(StructField(field_name, ArrayType(spark_type), nullable))
            elif isinstance(field_type, dict):
                # Handle dicts with any -> any
                key_type, value_type = field_type.__args__
                if isinstance(value_type, BaseModel):
                    sub_struct = cls.map_pydantic_model_to_struct(value_type)
                    spark_type = MapType(cls.get_spark_type(key_type), sub_struct)
                    nullable = field_type.__origin__ is Union or is_field_nullable(field_name, hints)
                    fields_list.append(StructField(field_name, spark_type, nullable))
                else:
                    spark_type = MapType(cls.get_spark_type(key_type), cls.get_spark_type(value_type))
                    nullable = field_type.__origin__ is Union or is_field_nullable(field_name, hints)
                    fields_list.append(StructField(field_name, spark_type, nullable))
            else:
                # Handle primitive types and Decimal types
                spark_type = cls.get_spark_type(field_type)
                nullable = is_field_nullable(field_name, hints)
                fields_list.append(StructField(field_name, spark_type, nullable))

        return StructType(fields_list)

    @classmethod
    def get_spark_type(cls, py_type: Type) -> DataType:
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
        # Check for list types
        if get_origin(py_type) is list:
            elem_type = get_args(py_type)[0]
            return ArrayType(cls.get_spark_type(elem_type))

        # Check for optional types
        if get_origin(py_type) is Union and type(None) in get_args(py_type):
            elem_type = [arg for arg in get_args(py_type) if arg != type(None)][0]
            return cls.get_spark_type(elem_type)

        raise Exception(f"Type {py_type} is not supported by PySpark")
