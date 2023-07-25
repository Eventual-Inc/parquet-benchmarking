"""Generates nice Parquet files:

1. ~200MB per file
2. 2 rowgroups per file
3. Uses Spark to write the data
"""

import decimal
import string
import dataclasses
import numpy as np
from pyspark.sql.functions import pandas_udf, explode
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DecimalType, LongType, DataType as SparkDataType


@dataclasses.dataclass
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


@dataclasses.dataclass
class SchemaSpec:
    fields: list[ColumnSpec]

    def to_spark_schema(self) -> StructType:
        return StructType([StructField(f.name, f.spark_type()) for f in self.fields])


@dataclasses.dataclass
class Int64SmallValuesColumn(ColumnSpec):

    def spark_type(self) -> SparkDataType:
        return LongType()

    def generate_unique_values(self, num: int) -> np.ndarray:
        return np.arange(num)

    def random_generate(self, size: int) -> np.ndarray:
        min_int64 = -(2**63)
        max_int64 = (2**63) - 1
        return np.random.randint(min_int64, max_int64, size=(size,))
    

@dataclasses.dataclass
class DecimalPctColumn(ColumnSpec):

    def spark_type(self) -> SparkDataType:
        return DecimalType(10, 2)

    def generate_unique_values(self, num: int) -> np.ndarray:
        return [round(decimal.Decimal(i), 2) for i in np.random.rand(num)]

    def random_generate(self, size: int) -> np.ndarray:
        return [round(decimal.Decimal(i), 2) for i in np.random.rand(size,)]


@dataclasses.dataclass
class StringCategoricalColumn(ColumnSpec):

    min_bytes_length: int = 8
    max_bytes_length: int = 16

    CHAR_CHOICES = list(string.ascii_letters)

    def spark_type(self) -> SparkDataType:
        return StringType()

    def generate_unique_values(self, num: int) -> np.ndarray:
        str_lengths = (
            np.ones((num,)).astype(np.int64) * self.min_bytes_length
            if self.min_bytes_length == self.max_bytes_length
            else np.random.randint(self.min_bytes_length, self.max_bytes_length, size=(num,))
        )
        strings = set()
        while len(strings) != num:
            str_len = str_lengths[len(strings)]
            generated = "".join(np.random.choice(StringCategoricalColumn.CHAR_CHOICES, replace=True, size=(str_len,)))
            strings.add(generated)
        return np.array(list(strings))

    def random_generate(self, size: int) -> np.ndarray:
        str_lengths = (
            np.ones((size,)).astype(np.int64) * self.min_bytes_length
            if self.min_bytes_length == self.max_bytes_length
            else np.random.randint(self.min_bytes_length, self.max_bytes_length, size=(size,))
        )
        return np.array(list(map(
            lambda str_len: "".join(np.random.choice(StringCategoricalColumn.CHAR_CHOICES, replace=True, size=(str_len,))),
            str_lengths,
        )))


NUM_ROWS = 2_440_000
SCHEMA_SPEC = SchemaSpec([
    Int64SmallValuesColumn(name="int64_unique-1", num_unique_values=1),
    Int64SmallValuesColumn(name="int64_unique-10", num_unique_values=10),
    Int64SmallValuesColumn(name="int64_unique-100", num_unique_values=100),
    Int64SmallValuesColumn(name="int64_unique-1000", num_unique_values=1000),
    Int64SmallValuesColumn(name="int64_unique-10000", num_unique_values=10000),
    Int64SmallValuesColumn(name="int64_unique-100000", num_unique_values=100000),
    Int64SmallValuesColumn(name="int64_unique-1000000", num_unique_values=1000000),
    StringCategoricalColumn(name="small_categoricals_unique-1", num_unique_values=1),
    StringCategoricalColumn(name="small_categoricals_unique-10", num_unique_values=10),
    StringCategoricalColumn(name="small_categoricals_unique-100", num_unique_values=100),
    StringCategoricalColumn(name="small_categoricals_unique-1000", num_unique_values=1000),
    StringCategoricalColumn(name="small_categoricals_unique-10000", num_unique_values=10000),
    StringCategoricalColumn(name="small_categoricals_unique-100000", num_unique_values=100000),
    StringCategoricalColumn(name="small_categoricals_unique-1000000", num_unique_values=1000000),
    DecimalPctColumn(name="decimal_p-2_s-2_unique-1", num_unique_values=1),
    DecimalPctColumn(name="decimal_p-2_s-2_unique-10", num_unique_values=10),
    DecimalPctColumn(name="decimal_p-2_s-2_unique-100", num_unique_values=100),
])
NUM_PARTITIONS = 200
NUM_ROWS_PER_PARTITION = int(NUM_ROWS // NUM_PARTITIONS)
MIN_STRING_LENGTH = 32
MAX_STRING_LENGTH = 64


def generate_data(schema_spec: SchemaSpec, seed: int) -> dict[str, list]:
    np.random.seed(seed)
    data = {field.name: field.generate(NUM_ROWS_PER_PARTITION) for field in schema_spec.fields}
    return [{name: data[name][i] for name in data} for i in range(NUM_ROWS_PER_PARTITION)]


@pandas_udf(ArrayType(SCHEMA_SPEC.to_spark_schema()))
def generate_data_per_partition(part_ids: pd.Series) -> pd.Series:
    data = [generate_data(SCHEMA_SPEC, seed=part_id) for part_id in part_ids]
    return pd.Series(data)


def main(dest: str):
    spark = SparkSession.builder.appName("DataGenerator").getOrCreate()
    rdd = spark.sparkContext.parallelize([(i,) for i in range(NUM_PARTITIONS)])
    df = rdd.toDF(["part_id"])
    df = df.select(explode(generate_data_per_partition("part_id")).alias("data"))
    df = df.select(*[f"data.{f.name}" for f in SCHEMA_SPEC.fields])

    # Full list of available options: https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md
    df.repartition(1) \
        .write \
        .parquet(dest)


if __name__ == "__main__":
    main("data/spark.parquet")
