# PySpark Types

`pyspark_types` is a Python library that provides a simple way to map Python dataclasses to PySpark StructTypes.

## Usage

### Pydantic
PySparkBaseModel is a base class for PySpark models that provides methods for converting between PySpark Rows and Pydantic models.

Here's an example of a Pydantic model that will be used to create a PySpark DataFrame:

```python
from pydantic import BaseModel
from pyspark_types.auxiliary import BoundDecimal


class Person(BaseModel):
    name: str
    age: int
    addresses: dict[str, str]
    salary: BoundDecimal

```

To create a PySpark DataFrame from a list of Person Pydantic models, we can use PySparkBaseModel.create_spark_dataframe() method.

```python
from pyspark.sql import SparkSession
from pyspark_types.pydantic import PySparkBaseModel


spark = SparkSession.builder.appName("MyApp").getOrCreate()

# create a list of Pydantic models
data = [
    Person(
        name="Alice",
        age=25,
        addresses={"home": "123 Main St", "work": "456 Pine St"},
        salary=BoundDecimal("5000.00", precision=10, scale=2),
    ),
    Person(
        name="Bob",
        age=30,
        addresses={"home": "789 Elm St", "work": "321 Oak St"},
        salary=BoundDecimal("6000.50", precision=10, scale=2),
    ),
]

# create a PySpark DataFrame from the list of Pydantic models
df = Person.create_spark_dataframe(data, spark)

# show the contents of the DataFrame
df.show()

```

Output: 
```bash
+---+-----+--------------------+------+
|age| name|           addresses|salary|
+---+-----+--------------------+------+
| 25|Alice|[home -> 123 Main...|5000.00|
| 30|  Bob|[home -> 789 Elm ...|6000.50|
+---+-----+--------------------+------+

```

The PySparkBaseModel.create_spark_dataframe() method converts the list of Pydantic models to a list of dictionaries, and then creates a PySpark DataFrame from the list of dictionaries and schema generated from the Pydantic model.

You can also generate a schema based on a Pydantic model by calling the PySparkBaseModel.schema() method:
```python
schema = PySparkBaseModel.schema(Person)

```

This creates a PySpark schema for the Person Pydantic model.

Note that if you have custom types, such as BoundDecimal, you will need to add support for them in PySparkBaseModel. For example, you can modify the PySparkBaseModel.dict() method to extract BoundDecimal values when mapping to DecimalType.
### Dataclasses

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
