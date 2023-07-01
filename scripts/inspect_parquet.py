import argparse

import os
from typing import Any
import json

from parquet_benchmarking.parquet_thrift.ttypes import FileMetaData, RowGroup, ColumnChunk, PageHeader, Encoding, CompressionCodec, PageType
from thrift.protocol.TCompactProtocol import TCompactProtocolFactory
from thrift.transport.TTransport import TMemoryBuffer, TFileObjectTransport


def inspect_file(
    path: str,
) -> dict[str, Any]:
    protocol_factory = TCompactProtocolFactory()
    with open(path, "rb") as f:

        # Parquet files all end with b"PAR1"
        f.seek(-4, 2)
        assert f.read().decode("utf-8") == "PAR1"

        # Parse FileMetaData thrift message
        f.seek(-4 - 4, 2)
        file_metadata_size = int.from_bytes(f.read(4), "little")
        f.seek(-4 - 4 - file_metadata_size, 2)
        # data = f.read(file_metadata_size)
        metadata = FileMetaData()
        metadata.read(protocol_factory.getProtocol(TFileObjectTransport(f)))

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
        FEAT_CCM_bloom_filter = any([(ccm.bloom_filter_length is not None) and (ccm.bloom_filter_offset is not None) for ccm in column_chunk_metadata])
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
        page_headers: list[PageHeader] = []
        for ccm in column_chunk_metadata:
            start, end = (ccm.data_page_offset, ccm.data_page_offset + ccm.total_compressed_size)
            ptr = start
            
            while ptr < end:
                f.seek(ptr)
                page_header = PageHeader()
                page_header.read(protocol_factory.getProtocol(TFileObjectTransport(f)))
                page_headers.append(page_header)

                # TODO(jay): Figure out how to seek to the next page - we need to check all the pages
                break

        FEAT_PAGE_data_page_v2 = any([ph.type == PageType.DATA_PAGE_V2 for ph in page_headers])
        FEAT_PAGE_page_crc_checksum = any([ph.crc is not None for ph in page_headers])

    return {
        "total_file_size": os.stat(path).st_size,
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
        "FEAT_CCM_bloom_filter": FEAT_CCM_bloom_filter,
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