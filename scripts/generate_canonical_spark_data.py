"""Generates nice Parquet files:

1. ~200MB per file
2. 2 rowgroups per file
3. Uses Spark to write the data
"""

import pyspark
from pyspark.sql.functions import pandas_udf, explode
import pandas as pd
import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, LongType, DataType as SparkDataType



NUM_ROWS = 1_220_000
SCHEMA = StructType([
    StructField("int64", IntegerType()),
    StructField("strings", StringType()),
])
NUM_PARTITIONS = 200
NUM_ROWS_PER_PARTITION = int(NUM_ROWS // NUM_PARTITIONS)


def generate_data(schema: StructType, seed: int) -> dict[str, list]:
    # TODO(jay): Hardcoded
    return [{
        "int64": i,
        "strings": f"prefix-{i}",
    } for i in range(NUM_ROWS_PER_PARTITION)]



@pandas_udf(ArrayType(SCHEMA))
def generate_data_per_partition(part_ids: pd.Series) -> pd.Series:
    data = [generate_data(SCHEMA, seed=part_id) for part_id in part_ids]
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
    df = df.select(*[f"data.{c}" for c in SCHEMA.fieldNames()])

    # Full list of available options: https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md
    df.repartition(1) \
        .write \
        .option("parquet.block.size", row_group_size_bytes) \
        .option("parquet.page.size", page_size_bytes) \
        .parquet(dest)


if __name__ == "__main__":
    main("data/spark.parquet")
