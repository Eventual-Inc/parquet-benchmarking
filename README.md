# Parquet Benchmarking

Code for generating and inspecting Parquet files with the intention of systematically benchmarking I/O.

## Inspecting Parquet files

First download the Parquet file you are interested in interrogating:

```bash
aws s3 cp s3://daft-public-data/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet data/sample.parquet
```

Next, run the inspection script:

```bash
python parquet_benchmarking/inspectpq/__main__.py data/sample.parquet
```

Available options:

```bash
`--output=tsv`: outputs data in a TSV format instead which is easier for copying data into a spreadsheet
`--human-readable`: uses human-readable suffixes in sizes for bytes instead, which is easier to read in a spreadsheet
```
