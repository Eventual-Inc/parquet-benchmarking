import dataclasses

import pyarrow as pa


class DataType:
    """Superclass of the DataTypes that are supported"""
    pass


@dataclasses.dataclass(frozen=True)
def ArrowDataType(DataType):
    arrow_type: pa.DataType
