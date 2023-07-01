# Parquet Thrift Definitions

Code in this folder is generated with the Thrift tool:

```
curl https://github.com/apache/parquet-format/blob/1603152f8991809e8ad29659dffa224b4284f31b/src/main/thrift/parquet.thrift > parquet.thrift
thrift -r --gen py parquet.thrift
```
