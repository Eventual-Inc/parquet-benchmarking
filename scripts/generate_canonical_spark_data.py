"""Generates nice Parquet files:

1. ~200MB per file
2. 2 rowgroups per file
3. Uses Spark to write the data
"""

import sys
import pyspark
import random
import string
import dataclasses
import numpy as np
from pyspark.sql.functions import pandas_udf, explode
import pandas as pd
import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, LongType, DataType as SparkDataType




@dataclasses.dataclass(frozen=True)
class ColumnSpec:
    name: str

    # Specify either num_unique_values, or the unique_values directly
    # Leave both as `None` to generate the maximum entropy for this type
    num_unique_values: float | None = None
    unique_values: np.ndarray | None = None

    def __post_init__(self):
        # Generate unique values if required
        if self.num_unique_values is not None and self.unique_values is None:
            self.unique_values = self.generate_unique_values(self.num_unique_values)
        elif self.num_unique_values is None and self.unique_values is not None:
            self.num_unique_values = len(self.unique_values)
        assert (self.unique_values is None and self.num_unique_values is None) or (len(self.unique_values) == self.num_unique_values)
        
    def spark_type(self) -> SparkDataType:
        """Corresponding Spark type"""
        raise NotImplementedError("Subclass to implement")

    def generate_unique_values(self, num: int) -> np.ndarray:
        """Generate values that should be globally unique across the entire dataframe column"""
        raise NotImplementedError("Subclass to implement")

    def random_generate(self, size: int) -> np.ndarray:
        """Randomly generate data values for a given partition, with maximum entropy"""
        raise NotImplementedError("Subclass to implement")
    
    def generate(self, size: int) -> np.ndarray:
        if self.unique_values is not None:
            return np.random.choice(self.unique_values, replace=True, size=(size,))
        return self.random_generate(size)



@dataclasses.dataclass(frozen=True)
class SchemaSpec:
    fields: list[ColumnSpec]

    def to_spark_schema(self) -> StructType:
        return StructType([StructField(f.name, f.spark_type()) for f in self.fields])


class IntegerColumn(ColumnSpec):

    def spark_type(self) -> SparkDataType:
        return IntegerType()

    def generate_unique_values(self, num: int) -> np.ndarray:
        return np.arange(num)

    def random_generate(self, size: int) -> np.ndarray:
        min_int32 = -2_147_483_648
        max_int32 = 2_147_483_647
        return np.random.randint(min_int32, max_int32, size=(size,))


class StringColumn(ColumnSpec):

    min_bytes_length: int = 32
    max_bytes_length: int = 64

    def spark_type(self) -> SparkDataType:
        return StringType()

    def generate_unique_values(self, num: int) -> np.ndarray:
        str_lengths = np.random.randint(self.min_bytes_length, self.max_bytes_length, size=(num,))
        strings = set()
        while len(strings) != num:
            str_len = str_lengths[len(strings)]
            generated = "".join(np.random.choice(CHAR_CHOICES, replace=True, size=(str_len,)))
            strings.add(generated)
        return np.array(list(strings))

    def random_generate(self, size: int) -> np.ndarray:
        str_lengths = np.random.randint(self.min_bytes_length, self.max_bytes_length, size=(size,))
        return np.array(list(map(
            lambda str_len: "".join(np.random.choice(CHAR_CHOICES, replace=True, size=(str_len,))),
            str_lengths,
        )))


NUM_ROWS = 2_440_000
SCHEMA_SPEC = SchemaSpec([
    IntegerColumn("int64"),
    StringColumn("categorical"),
])
NUM_PARTITIONS = 200
NUM_ROWS_PER_PARTITION = int(NUM_ROWS // NUM_PARTITIONS)
MIN_STRING_LENGTH = 32
MAX_STRING_LENGTH = 64
CHAR_CHOICES = list(string.ascii_letters)


def generate_data(schema_spec: SchemaSpec, seed: int) -> dict[str, list]:
    np.random.seed(seed)
    data = {field.name: field.generate(NUM_ROWS_PER_PARTITION) for field in schema_spec.fields}
    return [{name: data[name][i] for name in data} for i in range(NUM_ROWS_PER_PARTITION)]


@pandas_udf(ArrayType(SCHEMA_SPEC.to_spark_schema()))
def generate_data_per_partition(part_ids: pd.Series) -> pd.Series:
    data = [generate_data(SCHEMA_SPEC, seed=part_id) for part_id in part_ids]
    return pd.Series(data)


def main(
    dest: str,
    row_group_size_bytes: int = 134_217_728,
    page_size_bytes: int = 1_048_576,
):
    spark = SparkSession.builder.appName("DataGenerator").getOrCreate()
    # spark_schema = StructType([StructField(name, _get_spark_type(daft_type)) for name, daft_type in data_options.daft_schema.items()])
    # python_data = _generate_data(data_options)
    # df = spark.createDataFrame(data=python_data, schema=spark_schema)
    rdd = spark.sparkContext.parallelize([(i,) for i in range(NUM_PARTITIONS)])
    df = rdd.toDF(["part_id"])
    df = df.select(explode(generate_data_per_partition("part_id")).alias("data"))
    df = df.select(*[f"data.{f.name}" for f in SCHEMA_SPEC.fields])

    # Full list of available options: https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md
    df.repartition(1) \
        .write \
        .option("parquet.block.size", row_group_size_bytes) \
        .option("parquet.page.size", page_size_bytes) \
        .parquet(dest)


if __name__ == "__main__":
    main("data/spark.parquet")
