import argparse
import pyarrow as pa
import pyarrow.parquet as papq


def generate_pyarrow(
    dest: str,
):
    tbl = pa.Table.from_pydict({
        "a": pa.array([None if i % 10 == 0 else i for i in range(100)], type=pa.int64()),
        "b": pa.array([None if i % 10 == 0 else chr(i) * i for i in range(100)], type=pa.string()),
    })
    papq.write_table(
        tbl,
        dest,
        row_group_size=10,
        version="1.0",  # Use minimal set of logical types
        use_dictionary=False,  # No dictionary encoding
        compression="NONE",  # No compression
        write_statistics=False,  # No stats either
        column_encoding="PLAIN",
        data_page_version="1.0",
        write_batch_size=2,  # Smaller batches to write smaller pages
        data_page_size=16,  # 16 bytes per page forces multiple pages per chunk
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dest", help="Destination for Parquet file")
    args = parser.parse_args()
    generate_pyarrow(args.dest)
