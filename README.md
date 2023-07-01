# Parquet Benchmarking

Code for generating and inspecting Parquet files with the intention of systematically benchmarking I/O.

## Setup

Create a virtual environment and install dependencies (We use Python 3.10.9)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Inspecting Parquet files

First download the Parquet file you are interested in interrogating:

```bash
aws s3 cp s3://daft-public-data/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet data/sample.parquet
```

Next, run the inspection script to print a JSON of the inspection results:

```bash
python scripts/inspect_parquet.py data/sample.parquet
```

Available options:

```bash
`--output=tsv`: outputs data in a TSV format instead which is easier for copying data into a spreadsheet
```
