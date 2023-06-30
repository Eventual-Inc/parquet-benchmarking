import argparse

import os
from typing import Any
import pyarrow.parquet as papq
import json
import statistics


def inspect_file(
    path: str,
) -> dict[str, Any]:
    f = papq.ParquetFile(path)
    meta = f.metadata
    num_row_groups = f.num_row_groups
    row_groups = [meta.row_group(rg_idx) for rg_idx in range(num_row_groups)]

    row_group_sizes_uncompressed = [rg.total_byte_size for rg in row_groups]
    mean_row_group_size_uncompressed = statistics.mean(row_group_sizes_uncompressed)
    stddev_row_group_size_uncompressed = statistics.pstdev(row_group_sizes_uncompressed)
    row_group_sizes_compressed = [sum([rg.column(col_idx).total_compressed_size for col_idx in range(meta.num_columns)]) for rg in row_groups]
    mean_row_group_sizes_compressed = statistics.mean(row_group_sizes_compressed)
    stddev_row_group_sizes_compressed = statistics.pstdev(row_group_sizes_compressed)

    row_group_nrows = [rg.num_rows for rg in row_groups]
    mean_row_group_nrows = statistics.mean(row_group_nrows)
    stddev_row_group_nrows = statistics.pstdev(row_group_nrows)

    raw_column_metadata = {
        meta.schema.column(col_idx).name: {
            "sizes_uncompressed": [rg.column(col_idx).total_uncompressed_size for rg in row_groups],
            "sizes_compressed": [rg.column(col_idx).total_uncompressed_size for rg in row_groups],
        } for col_idx in range(meta.num_columns)
    }
    column_mean_size_uncompressed = {
        colname: statistics.mean(meta["sizes_uncompressed"]) for colname, meta in raw_column_metadata.items()
    }
    column_stddev_size_uncompressed = {colname: statistics.pstdev(meta["sizes_uncompressed"]) for colname, meta in raw_column_metadata.items()}
    column_mean_size_compressed = {colname: statistics.mean(meta["sizes_compressed"]) for colname, meta in raw_column_metadata.items()}
    column_stddev_size_compressed = {colname: statistics.pstdev(meta["sizes_compressed"]) for colname, meta in raw_column_metadata.items()}

    return {
        "total_file_size": os.stat(path).st_size,
        "created_by": meta.created_by,
        "format_version": meta.format_version,
        "footer_thrift_size_bytes": meta.serialized_size,
        "num_rows": meta.num_rows,
        "num_row_groups": f.num_row_groups,
        "parquet_logical_schema": {f.name: f.logical_type.type for f in f.schema},
        "parquet_physical_schema": {f.name: f.physical_type for f in f.schema},

        # Row group metadata
        "mean_row_group_size_uncompressed": mean_row_group_size_uncompressed,
        "stddev_row_group_size_uncompressed": stddev_row_group_size_uncompressed,
        "mean_row_group_sizes_compressed": mean_row_group_sizes_compressed,
        "stddev_row_group_sizes_compressed": stddev_row_group_sizes_compressed,
        "mean_row_group_nrows": mean_row_group_nrows,
        "stddev_row_group_nrows": stddev_row_group_nrows,

        # Column chunk metadata
        "column_mean_size_uncompressed": column_mean_size_uncompressed,
        "column_stddev_size_uncompressed": column_stddev_size_uncompressed,
        "column_mean_size_compressed": column_mean_size_compressed,
        "column_stddev_size_compressed": column_stddev_size_compressed,
    }
    


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="Local path to the file to be inspected")
    parser.add_argument("--output-format", help="Output one of [json|tsv]", default="json")
    args = parser.parse_args()

    inspected_data = inspect_file(args.file)

    if args.output_format == "json":
        print(json.dumps(inspected_data, indent=2))
    elif args.output_format == "tsv":
        print("\t".join(inspected_data.keys()))
        print("\t".join([json.dumps(v) for v in inspected_data.values()]))
    else:
        raise NotImplementedError(f"Unsupported output_format: {args.output_format}")


if __name__ == "__main__":
    main()
