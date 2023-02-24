# PySpark Types

`pyspark_types` is a Python library that provides a simple way to map Python data classes to PySpark StructTypes.

## Usage

To use pyspark_types, you first need to define a Python data class with the fields you want to map to PySpark. For example:

```python
from dataclasses import dataclass

@dataclass
class Person:
    name: str
    age: int
    is_student: bool

```
To map this data class to a PySpark StructType, you can use the map_dataclass_to_struct() function:

```python
from pyspark_types import map_dataclass_to_struct

person_struct = map_dataclass_to_struct(Person)
```

This will return a PySpark StructType that corresponds to the Person data class.

You can also use the apply_nullability() function to set the nullable flag for a given PySpark DataType:

```python
from pyspark.sql.types import StringType
from pyspark_types import apply_nullability

nullable_string_type = apply_nullability(StringType(), True)
```

This will return a new PySpark StringType with the nullable flag set to True.
