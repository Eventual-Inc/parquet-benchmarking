import dataclasses

import pyarrow as pa
from pyspark.sql import types as spark_types


SPARK_BYTE_TYPE = spark_types.ByteType()
SPARK_SHORT_TYPE = spark_types.ShortType()
