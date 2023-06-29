import dataclasses
import daft

import pyspark
import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DataType as SparkDataType

from typing import Any


DAFT_TO_SPARK = {
    daft.DataType.int32(): IntegerType(),
    daft.DataType.int64(): LongType(),
    daft.DataType.string(): StringType(),
}

# TODO: We can move to something like Hypothesis strategies here
DAFT_DATATYPE_TO_PYDATA = {
    daft.DataType.int32(): 1,
    daft.DataType.int64(): 1,
    daft.DataType.string(): "foo",
}
    


@dataclasses.dataclass(frozen=True)
class DataOptions:
    daft_schema: dict[str, daft.DataType]
    num_rows: int = 10000


def _get_spark_type(daft_type: daft.DataType) -> SparkDataType:
    if daft_type not in DAFT_TO_SPARK:
        raise NotImplementedError(f"Conversion of Daft type {daft_type} to Spark type (through PyArrow) has not been implemented yet")
    return DAFT_TO_SPARK[daft_type]

def _generate_pydata_value(daft_type: daft.DataType) -> Any:
    if daft_type not in DAFT_DATATYPE_TO_PYDATA:
        raise NotImplementedError(f"Generating a Python value for type {daft_type} has not been implemented yet")
    return DAFT_DATATYPE_TO_PYDATA[daft_type]

def _generate_data(data_options: DataOptions) -> list[tuple[Any, ...]]:
    return [tuple(_generate_pydata_value(daft_type) for daft_type in data_options.daft_schema.values()) for _ in range(data_options.num_rows)]



def generate_spark(
    dest: str,
    data_options: DataOptions,
    row_group_size_bytes: int = 134_217_728,
    page_size_bytes: int = 1_048_576,
):
    spark = SparkSession.builder.appName("DataGenerator").getOrCreate()

    spark_schema = StructType([StructField(name, _get_spark_type(daft_type)) for name, daft_type in data_options.daft_schema.items()])
    python_data = _generate_data(data_options)

    df = spark.createDataFrame(data=python_data, schema=spark_schema)

    # Full list of available options: https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md
    df.write \
        .option("parquet.block.size", row_group_size_bytes) \
        .option("parquet.page.size", page_size_bytes) \
        .parquet(dest)
