import typing
from typing import Type, Union, get_type_hints, get_origin, get_args, List
from pydantic import BaseModel
from pyspark.sql.types import *
from pyspark_types.dataclass import is_field_nullable, is_optional_type
from pyspark_types.auxiliary import LongT, ShortT, ByteT, BoundDecimal
from pyspark.sql import SparkSession, DataFrame
from decimal import Decimal


class PySparkBaseModel(BaseModel):
    """
    Base class for PySpark models. Provides methods for converting between PySpark Rows and Pydantic models.
    """

    class Config:
        arbitrary_types_allowed = True
        validate_assignment = True
        validate_all = True


    def dict(self, *args, **kwargs):
        """
        Override Pydantic's dict() method to return a dictionary with PySparkBaseModel field values
        instead of Pydantic field values
        """
        result = super().dict(*args, **kwargs)
        new_result = {}
        for k, v in result.items():
            if isinstance(v, PySparkBaseModel):
                new_result[k] = v.dict()
            elif isinstance(v, BoundDecimal):
                new_result[k] = Decimal(v)
            else:
                new_result[k] = v
        return new_result

    @classmethod
    def is_pyspark_basemodel_type(cls, t: Type) -> bool:
        return isinstance(t, type) and issubclass(t, PySparkBaseModel)

    @classmethod
    def is_optional_pyspark_basemodel_type(cls, t: Type) -> bool:
        return (
            get_origin(t) is Union
            and len(get_args(t)) == 2
            and get_args(t)[1] is type(None)
            and cls.is_pyspark_basemodel_type(get_args(t)[0])
        )

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
        raise Exception(f"Type {py_type} is not supported by PySpark")

    @classmethod
    def _get_struct_field(
        cls, field_name: str, field_type: Type, hints: typing.Dict[str, Type]
    ) -> StructField:
        # Handle PySparkBaseModel and Optional[PySparkBaseModel] fields
        if cls.is_pyspark_basemodel_type(
            field_type
        ) or cls.is_optional_pyspark_basemodel_type(field_type):
            if cls.is_optional_pyspark_basemodel_type(field_type):
                field_type = field_type.__args__[0]
            sub_struct = cls._schema(field_type)
            nullable = (
                field_type.__config__.allow_population_by_field_name
                or is_field_nullable(field_name, hints)
            )
            return StructField(field_name, sub_struct, nullable)

        # Handle list fields
        elif get_origin(field_type) is list:
            elem_type = get_args(field_type)[0]
            struct_field = cls._get_list_struct_field(field_name, elem_type, hints)
            return struct_field

        # Handle dict fields
        elif get_origin(field_type) is dict:
            key_type, value_type = get_args(field_type)
            struct_field = cls._get_dict_struct_field(
                field_name, key_type, value_type, hints
            )
            return struct_field

        # Handle all other types
        else:
            spark_type = cls.get_spark_type(field_type)
            nullable = is_field_nullable(field_name, hints)
            return StructField(field_name, spark_type, nullable)

    @classmethod
    def _get_list_struct_field(
        cls, field_name: str, elem_type: Type, hints: typing.Dict[str, Type]
    ) -> StructField:
        if cls.is_pyspark_basemodel_type(
            elem_type
        ) or cls.is_optional_pyspark_basemodel_type(elem_type):
            sub_struct = cls._schema(elem_type)
            nullable = True
            return StructField(field_name, ArrayType(sub_struct), nullable)

        else:
            spark_type = cls.get_spark_type(elem_type)
            nullable = is_field_nullable(field_name, hints)
            return StructField(field_name, ArrayType(spark_type), nullable)

    @classmethod
    def _get_dict_struct_field(
        cls,
        field_name: str,
        key_type: Type,
        value_type: Type,
        hints: typing.Dict[str, Type],
    ) -> StructField:
        if cls.is_pyspark_basemodel_type(
            value_type
        ) or cls.is_optional_pyspark_basemodel_type(value_type):
            sub_struct = cls._schema(value_type)
            spark_type = MapType(cls.get_spark_type(key_type), sub_struct)
            nullable = True
            return StructField(field_name, spark_type, nullable)

        else:
            spark_type = MapType(
                cls.get_spark_type(key_type), cls.get_spark_type(value_type)
            )
            nullable = is_field_nullable(field_name, hints)
            return StructField(field_name, spark_type, nullable)

    @classmethod
    def schema(cls: Type[BaseModel]) -> StructType:
        """
        Map a Pydantic model to a PySpark struct.

        :param model_type: The Pydantic model to be mapped.
        :return: A PySpark struct that corresponds to the Pydantic model.
        """
        return cls._schema(cls)

    @classmethod
    def _schema(cls, t: Type[BaseModel]) -> StructType:
        """
        Map a Pydantic model to a PySpark struct.

        :param model_type: The Pydantic model to be mapped.
        :return: A PySpark struct that corresponds to the Pydantic model.
        """
        fields_list = []
        hints = get_type_hints(t)

        for field_name, _ in t.__fields__.items():
            field_type = hints.get(field_name)
            struct_field = cls._get_struct_field(field_name, field_type, hints)
            fields_list.append(struct_field)

        return StructType(fields_list)

    @classmethod
    def get_spark_type(cls, py_type: Type) -> DataType:
        """
        Creates a mapping from a python type to a pyspark data type
        :param py_type:
        :return:
        """
        if py_type is None:
            return NullType()
        elif py_type == str:
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
            spark_type = cls.get_spark_type(elem_type)
            return spark_type
        raise Exception(f"Type {py_type} is not supported by PySpark")


    @classmethod
    def create_spark_dataframe(cls, data: List['PySparkBaseModel'], spark: SparkSession) -> DataFrame:
        """
        Creates a PySpark DataFrame from a list of Pydantic models.

        :param data: A list of Pydantic models.
        :param spark: A PySpark SparkSession.
        :return: A PySpark DataFrame.
        """
        # Generate schema based on base model
        schema = cls.schema()

        # Convert Pydantic models to dictionaries
        data_dicts = [item.dict() for item in data]

        # Create Spark DataFrame from list of dictionaries and schema
        df = spark.createDataFrame(data_dicts, schema)

        return df