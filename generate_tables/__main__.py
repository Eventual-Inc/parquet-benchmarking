import daft

from generate_tables.generate import DataOptions, generate_spark

def main():
    generate_spark(
        "file.parquet",
        DataOptions(
            daft_schema={"foo": daft.DataType.int64(), "bar": daft.DataType.string()},
            num_rows=100_000_000,
        ),
        row_group_size_bytes=134_217_728,
        page_size_bytes=1_048_576,
    )


if __name__ == "__main__":
    main()
