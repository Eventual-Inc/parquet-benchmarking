import argparse

import os
from typing import Any, IO
import json
import statistics
import tabulate
import dataclasses

from parquet_benchmarking.parquet_thrift.ttypes import FileMetaData, RowGroup, ColumnChunk, ColumnMetaData, PageHeader, Encoding, CompressionCodec, PageType, LogicalType, Type, ConvertedType, SchemaElement
from thrift.protocol.TCompactProtocol import TCompactProtocolFactory
from thrift.transport.TTransport import TFileObjectTransport


@dataclasses.dataclass(frozen=True)
class Stat:
    min: Any
    max: Any
    mean: Any
    stddev: Any


def _logical_type_to_string(logical_type: LogicalType) -> str:
    if logical_type is None:
        return str(logical_type)
    for field in ["STRING", "MAP", "LIST", "ENUM", "DECIMAL", "DATE", "TIME", "TIMESTAMP", "INTEGER", "UNKNOWN", "JSON", "BSON", "UUID"]:
        oneof = getattr(logical_type, field)
        if oneof is not None:
            return str(oneof)
    raise NotImplementedError(f"Failed to convert logical type to string: {logical_type}")


def _physical_type_to_string(physical_type: int) -> str:
    return Type._VALUES_TO_NAMES[physical_type]


def inspect_file(
    path: str,
) -> dict[str, Any]:
    protocol_factory = TCompactProtocolFactory()
    with open(path, "rb") as f:

        # Parquet files all end with b"PAR1"
        f.seek(-4, 2)
        assert f.read().decode("utf-8") == "PAR1", \
            f"Encountered file at {path} that does not have b`PAR1` footer - this is either not a Parquet file or may be encrypted"

        # Parse FileMetaData thrift message
        f.seek(-4 - 4, 2)
        file_metadata_size = int.from_bytes(f.read(4), "little")
        f.seek(-4 - 4 - file_metadata_size, 2)
        metadata = FileMetaData()
        metadata.read(protocol_factory.getProtocol(TFileObjectTransport(f)))

        features = get_features(metadata, f)
        stats = get_statistics(metadata)
        column_stats = get_column_statistics(metadata, f)

    return {
        "total_file_size": os.stat(path).st_size,
        "file_metadata_size": file_metadata_size,
        **features,
        **stats,
        **column_stats,
    }


def get_column_statistics(metadata: FileMetaData, f: IO) -> dict:
    schema: dict[str, SchemaElement] = {schema_element.name: schema_element for schema_element in metadata.schema if schema_element.num_children is None}
    column_chunk_compressed_size_bytes = []
    column_chunk_uncompressed_size_bytes = []
    column_page_num = []
    column_page_compressed_size_bytes = []
    column_page_uncompressed_size_bytes = []

    column_names = [fname for fname in schema]
    column_logical_types = [_logical_type_to_string(schema[fname].logicalType) for fname in schema]
    column_physical_types = [_physical_type_to_string(schema[fname].type) for fname in schema]

    for i, _ in enumerate(schema):
        row_groups: list[RowGroup] = metadata.row_groups
        chunks: list[ColumnChunk] = [rg.columns[i] for rg in row_groups]
        page_headers: list[list[PageHeader]] = [_get_data_page_headers(cc.meta_data, f) for cc in chunks]

        compressed_sizes = [cc.meta_data.total_compressed_size for cc in chunks]
        column_chunk_compressed_size_bytes.append(
            Stat(
                mean=statistics.mean(compressed_sizes),
                stddev=statistics.pstdev(compressed_sizes),
                max=max(compressed_sizes),
                min=min(compressed_sizes),
            )
        )

        uncompressed_sizes = [cc.meta_data.total_uncompressed_size for cc in chunks]
        column_chunk_uncompressed_size_bytes.append(
            Stat(
                mean=statistics.mean(uncompressed_sizes),
                stddev=statistics.pstdev(uncompressed_sizes),
                max=max(uncompressed_sizes),
                min=min(uncompressed_sizes),
            )
        )

        num_pages = [len(headers) for headers in page_headers]
        column_page_num.append(Stat(
            mean=statistics.mean(num_pages),
            stddev=statistics.pstdev(num_pages),
            max=max(num_pages),
            min=min(num_pages),
        ))

        compressed_page_sizes = [header.compressed_page_size for headers in page_headers for header in headers]
        column_page_compressed_size_bytes.append(Stat(
            mean=statistics.mean(compressed_page_sizes),
            stddev=statistics.pstdev(compressed_page_sizes),
            max=max(compressed_page_sizes),
            min=min(compressed_page_sizes),
        ))

        uncompressed_page_sizes = [header.uncompressed_page_size for headers in page_headers for header in headers]
        column_page_uncompressed_size_bytes.append(Stat(
            mean=statistics.mean(uncompressed_page_sizes),
            stddev=statistics.pstdev(uncompressed_page_sizes),
            max=max(uncompressed_page_sizes),
            min=min(uncompressed_page_sizes),
        ))

    return {
        # Column names (private by default) and types
        "_COLUMN_names": column_names,
        "COLUMN_logical_types": column_logical_types,
        "COLUMN_physical_types": column_physical_types,

        # Column chunk stats
        "STATS_column_chunk_compressed_size_bytes": column_chunk_compressed_size_bytes,
        "STATS_column_chunk_uncompressed_size_bytes": column_chunk_uncompressed_size_bytes,

        # Page-level stats
        "STATS_column_page_num": column_page_num,
        "STATS_column_page_compressed_size_bytes": column_page_compressed_size_bytes,
        "STATS_column_page_uncompressed_size_bytes": column_page_uncompressed_size_bytes,
    }


def get_statistics(metadata: FileMetaData) -> dict:
    num_row_groups = len(metadata.row_groups)
    row_groups: list[RowGroup] = metadata.row_groups

    row_group_sizes_uncompressed = [rg.total_byte_size for rg in row_groups]
    row_group_size_uncompressed_stat = Stat(
        mean=statistics.mean(row_group_sizes_uncompressed),
        stddev=statistics.pstdev(row_group_sizes_uncompressed),
        max=max(row_group_sizes_uncompressed),
        min=min(row_group_sizes_uncompressed),
    )
    row_group_columns: list[list[ColumnChunk]] = [rg.columns for rg in row_groups]
    row_group_sizes_compressed = [sum([col.meta_data.total_compressed_size for col in rgcol]) for rgcol in row_group_columns]
    row_group_size_compressed_stat = Stat(
        mean=statistics.mean(row_group_sizes_compressed),
        stddev=statistics.pstdev(row_group_sizes_compressed),
        max=max(row_group_sizes_compressed),
        min=min(row_group_sizes_compressed),
    )

    row_group_nrows = [rg.num_rows for rg in row_groups]
    row_group_nrows_stat = Stat(
        mean=statistics.mean(row_group_nrows),
        stddev=statistics.pstdev(row_group_nrows),
        max=max(row_group_nrows),
        min=min(row_group_nrows),
    )

    return {
        # Row group metadata
        "STATS_row_group_num": num_row_groups,
        "STATS_row_group_uncompressed": row_group_size_uncompressed_stat,
        "STATS_row_group_compressed": row_group_size_compressed_stat,
        "STATS_row_group_nrows": row_group_nrows_stat,
    }


def get_features(metadata: FileMetaData, f: IO) -> dict:
    # Grab row group features
    row_groups: list[RowGroup]  = metadata.row_groups
    FEAT_RG_sorting_columns = any([rg.sorting_columns is not None for rg in row_groups])
    FEAT_RG_file_offset = any([rg.file_offset is not None for rg in row_groups])
    FEAT_RG_ordinal = any([rg.ordinal is not None for rg in row_groups])
    FEAT_RG_total_compressed_size = any([rg.total_compressed_size is not None for rg in row_groups])

    # Grab all ColumnChunks
    column_chunks: list[ColumnChunk] = [cc for rg in row_groups for cc in rg.columns]
    FEAT_CC_remote_filepath = any([cc.file_path is not None for cc in column_chunks])
    FEAT_CC_offset_index = any([(cc.offset_index_offset is not None) and (cc.offset_index_length is not None) for cc in column_chunks])
    FEAT_CC_column_index = any([(cc.column_index_offset is not None) and (cc.column_index_length is not None) for cc in column_chunks])
    FEAT_CC_column_crypto_metadata = any([(cc.encrypted_column_metadata is not None) and (cc.crypto_metadata is not None) for cc in column_chunks])
    FEAT_CC_inlined_column_metadata = any([cc.meta_data is not None for cc in column_chunks])

    # NOTE: each ColumnChunk.file_offset points to a location in the file that contains a duplicate ColumnChunk... Apparently?
    # We can turn on this assert to check that, but this has turned out to be true for our test files
    # for cc in column_chunks:
    #     with open(path, "rb") as f:
    #         f.seek(cc.file_offset)
    #         cc_at_offset = ColumnChunk()
    #         cc_at_offset.read(TCompactProtocolFactory().getProtocol(TFileObjectTransport(f)))
    #         assert cc == cc_at_offset

    # Grab all ColumnChunkMetadata
    column_chunk_metadata = [cc.meta_data for cc in column_chunks]
    assert all([ccm is not None for ccm in column_chunk_metadata]), "Cannot have empty ColumnChunkMetadata"
    all_encodings = {enc for ccm in column_chunk_metadata for enc in ccm.encodings}
    all_compression_codecs = {ccm.codec for ccm in column_chunk_metadata}
    FEAT_CCM_bloom_filter_offset = any([ccm.bloom_filter_offset is not None for ccm in column_chunk_metadata])
    FEAT_CCM_bloom_filter_length = any([ccm.bloom_filter_length is not None for ccm in column_chunk_metadata])
    FEAT_CCM_index_page = any([(ccm.index_page_offset is not None) for ccm in column_chunk_metadata])
    FEAT_CCM_dictionary_page = any([(ccm.dictionary_page_offset is not None) for ccm in column_chunk_metadata])
    FEAT_CCM_key_value_metadata = any([(ccm.key_value_metadata is not None) for ccm in column_chunk_metadata])
    FEAT_CCM_statistics = any([(ccm.statistics is not None) for ccm in column_chunk_metadata])
    FEAT_CCM_page_encoding_stats = any([(ccm.encoding_stats is not None) for ccm in column_chunk_metadata])

    FEAT_CCM_encoding_PLAIN = Encoding.PLAIN in all_encodings
    FEAT_CCM_encoding_PLAIN_DICTIONARY = Encoding.PLAIN_DICTIONARY in all_encodings
    FEAT_CCM_encoding_RLE = Encoding.RLE in all_encodings
    FEAT_CCM_encoding_BIT_PACKED = Encoding.BIT_PACKED in all_encodings
    FEAT_CCM_encoding_DELTA_BINARY_PACKED = Encoding.DELTA_BINARY_PACKED in all_encodings
    FEAT_CCM_encoding_DELTA_LENGTH_BYTE_ARRAY = Encoding.DELTA_LENGTH_BYTE_ARRAY in all_encodings
    FEAT_CCM_encoding_DELTA_BYTE_ARRAY = Encoding.DELTA_BYTE_ARRAY in all_encodings
    FEAT_CCM_encoding_RLE_DICTIONARY = Encoding.RLE_DICTIONARY in all_encodings
    FEAT_CCM_encoding_BYTE_STREAM_SPLIT = Encoding.BYTE_STREAM_SPLIT in all_encodings

    FEAT_CCM_compression_codec_UNCOMPRESSED = CompressionCodec.UNCOMPRESSED in all_compression_codecs
    FEAT_CCM_compression_codec_SNAPPY = CompressionCodec.SNAPPY in all_compression_codecs
    FEAT_CCM_compression_codec_GZIP = CompressionCodec.GZIP in all_compression_codecs
    FEAT_CCM_compression_codec_LZO = CompressionCodec.LZO in all_compression_codecs
    FEAT_CCM_compression_codec_BROTLI = CompressionCodec.BROTLI in all_compression_codecs
    FEAT_CCM_compression_codec_LZ4 = CompressionCodec.LZ4 in all_compression_codecs
    FEAT_CCM_compression_codec_ZSTD = CompressionCodec.ZSTD in all_compression_codecs
    FEAT_CCM_compression_codec_LZ4_RAW = CompressionCodec.LZ4_RAW in all_compression_codecs

    # Grab all Pages
    page_headers = [headers for ccm in column_chunk_metadata for headers in _get_data_page_headers(ccm, f)]

    FEAT_PAGE_data_page_v2 = any([ph.type == PageType.DATA_PAGE_V2 for ph in page_headers])
    FEAT_PAGE_page_crc_checksum = any([ph.crc is not None for ph in page_headers])

    return {
        "num_rows": metadata.num_rows,
        "parquet_version": metadata.version,
        "created_by": metadata.created_by,

        # Feature matrix
        "FEAT_RG_sorting_columns": FEAT_RG_sorting_columns,
        "FEAT_RG_file_offset": FEAT_RG_file_offset,
        "FEAT_RG_ordinal": FEAT_RG_ordinal,
        "FEAT_RG_total_compressed_size": FEAT_RG_total_compressed_size,
        "FEAT_CC_remote_filepath": FEAT_CC_remote_filepath,
        "FEAT_CC_offset_index": FEAT_CC_offset_index,
        "FEAT_CC_column_index": FEAT_CC_column_index,
        "FEAT_CC_column_crypto_metadata": FEAT_CC_column_crypto_metadata,
        "FEAT_CC_inlined_column_metadata": FEAT_CC_inlined_column_metadata,
        "FEAT_CCM_bloom_filter_offset": FEAT_CCM_bloom_filter_offset,
        "FEAT_CCM_bloom_filter_length": FEAT_CCM_bloom_filter_length,
        "FEAT_CCM_index_page": FEAT_CCM_index_page,
        "FEAT_CCM_dictionary_page": FEAT_CCM_dictionary_page,
        "FEAT_CCM_key_value_metadata": FEAT_CCM_key_value_metadata,
        "FEAT_CCM_statistics": FEAT_CCM_statistics,
        "FEAT_CCM_page_encoding_stats": FEAT_CCM_page_encoding_stats,
        "FEAT_CCM_encoding_PLAIN": FEAT_CCM_encoding_PLAIN,
        "FEAT_CCM_encoding_PLAIN_DICTIONARY": FEAT_CCM_encoding_PLAIN_DICTIONARY,
        "FEAT_CCM_encoding_RLE": FEAT_CCM_encoding_RLE,
        "FEAT_CCM_encoding_BIT_PACKED": FEAT_CCM_encoding_BIT_PACKED,
        "FEAT_CCM_encoding_DELTA_BINARY_PACKED": FEAT_CCM_encoding_DELTA_BINARY_PACKED,
        "FEAT_CCM_encoding_DELTA_LENGTH_BYTE_ARRAY": FEAT_CCM_encoding_DELTA_LENGTH_BYTE_ARRAY,
        "FEAT_CCM_encoding_DELTA_BYTE_ARRAY": FEAT_CCM_encoding_DELTA_BYTE_ARRAY,
        "FEAT_CCM_encoding_RLE_DICTIONARY": FEAT_CCM_encoding_RLE_DICTIONARY,
        "FEAT_CCM_encoding_BYTE_STREAM_SPLIT": FEAT_CCM_encoding_BYTE_STREAM_SPLIT,
        "FEAT_CCM_compression_codec_UNCOMPRESSED": FEAT_CCM_compression_codec_UNCOMPRESSED,
        "FEAT_CCM_compression_codec_SNAPPY": FEAT_CCM_compression_codec_SNAPPY,
        "FEAT_CCM_compression_codec_GZIP": FEAT_CCM_compression_codec_GZIP,
        "FEAT_CCM_compression_codec_LZO": FEAT_CCM_compression_codec_LZO,
        "FEAT_CCM_compression_codec_BROTLI": FEAT_CCM_compression_codec_BROTLI,
        "FEAT_CCM_compression_codec_LZ4": FEAT_CCM_compression_codec_LZ4,
        "FEAT_CCM_compression_codec_ZSTD": FEAT_CCM_compression_codec_ZSTD,
        "FEAT_CCM_compression_codec_LZ4_RAW": FEAT_CCM_compression_codec_LZ4_RAW,
        "FEAT_PAGE_data_page_v2": FEAT_PAGE_data_page_v2,
        "FEAT_PAGE_page_crc_checksum": FEAT_PAGE_page_crc_checksum,
    }


def _get_data_page_headers(ccm: ColumnMetaData, f: IO) -> list[PageHeader]:
    protocol_factory = TCompactProtocolFactory()

    # Grab all Pages for the column
    page_headers: list[PageHeader] = []
    f.seek(ccm.data_page_offset)

    # Iterate until all rows are read
    read_values = 0
    while read_values < ccm.num_values:
        page_header = PageHeader()
        page_header.read(protocol_factory.getProtocol(TFileObjectTransport(f)))
        page_headers.append(page_header)
        f.seek(page_header.compressed_page_size, 1)

        # Increment row pointer
        if page_header.type == PageType.DATA_PAGE:
            read_values += page_header.data_page_header.num_values
        elif page_header.type == PageType.DATA_PAGE_V2:
            read_values += page_header.data_page_header_v2.num_values

    return page_headers


def display(inspected_data: list[dict]):
    for meta in inspected_data:
        print(f"===== File: {meta['filepath']} =====")
        general_metadata_keys = [k for k in meta if (not k.startswith("STATS_")) and (not k.startswith("FEAT_"))]
        feature_keys = [k for k in meta if k.startswith("FEAT_")]
        rg_stats_keys = [k for k in meta if k.startswith("STATS_row_group")]
        column_keys = [k for k in meta if k.startswith("_COLUMN") or k.startswith("COLUMN")]
        column_chunk_stats_keys = [k for k in meta if k.startswith("STATS_column_chunk")]
        column_page_stats_keys = [k for k in meta if k.startswith("STATS_column_page")]

        print("General Metadata:")
        for k in general_metadata_keys:
            print(f"    {k}:\t{meta[k]}".expandtabs(30))
        print("\nFeatures:")
        for k in feature_keys:
            print(f"    {k}:\t{meta[k]}".expandtabs(60))
        print("\nRowgroup Stats:")
        for k in rg_stats_keys:
            print(f"    {k}:\t{meta[k]}".expandtabs(30))

        print("\nColumnChunk Stats:")
        column_data = {
            k: meta[k] for k in [
                *column_keys,
                *column_chunk_stats_keys,
            ]
        }
        print(tabulate.tabulate(column_data, headers=list(column_data.keys())))

        print("\nColumnChunkPage Stats:")
        column_data = {
            k: meta[k] for k in [
                *column_keys,
                *column_page_stats_keys,
            ]
        }
        print(tabulate.tabulate(column_data, headers=list(column_data.keys())))


def clean_private_fields(data: list[dict]):
    """Cleans up all fields prefixed with _, which are not intended for export to JSON/TSV"""
    return [{k: d[k] for k in d if not k.startswith("_")} for d in data]


def expand_stats(data: dict):
    result = {}
    for k in data:
        if isinstance(data[k], list):
            if all(isinstance(x, Stat) for x in data[k]):
                result[f"{k}_mean"] = [stat.mean for stat in data[k]]
                result[f"{k}_stddev"] = [stat.stddev for stat in data[k]]
                result[f"{k}_max"] = [stat.max for stat in data[k]]
                result[f"{k}_min"] = [stat.min for stat in data[k]]
            else:
                result[k] = data[k]
        elif isinstance(data[k], Stat):
            result[f"{k}_mean"] = data[k].mean
            result[f"{k}_stddev"] = data[k].stddev
            result[f"{k}_max"] = data[k].max
            result[f"{k}_min"] = data[k].min
        else:
            result[k] = data[k]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", help="Local path(s) to the files to be inspected", nargs="+")
    parser.add_argument("--output-format", help="Output one of [json|tsv|human]", default="human")
    parser.add_argument("--show-private-fields", help="Output private fields (prefixed with _)", default=False, action="store_true")
    args = parser.parse_args()
    inspected_data = [{"filepath": str(fpath), **inspect_file(str(fpath))} for fpath in args.paths]

    if not args.show_private_fields:
        inspected_data = clean_private_fields(inspected_data)

    if len(inspected_data) == 0:
        raise ValueError(f"No files selected for inspection at: {args.path}")

    if args.output_format == "human":
        display(inspected_data)
    else:
        inspected_data = [expand_stats(d) for d in inspected_data]
        if args.output_format == "json":
            print(json.dumps(inspected_data, indent=2))
        elif args.output_format == "tsv":
            print("\t".join(inspected_data[0].keys()))
            for data in inspected_data:
                print("\t".join([json.dumps(v) for v in data.values()]))
        else:
            raise NotImplementedError(f"Unsupported output_format: {args.output_format}")


if __name__ == "__main__":
    main()
