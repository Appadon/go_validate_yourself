# Go Validate Yourself (`gvy`)

`gvy` is a high-throughput CSV validation and export pipeline written in Go.

It supports:

- validating CSV rows against a JSON schema
- writing valid rows to Parquet
- writing invalid rows to error CSV files
- batching Parquet files into grouped outputs
- running the full split, validate, and batch pipeline in one command
- exposing the same engine through a localhost-only HTTP API

## Overview

At a high level, `gvy` works like this:

1. Split an input CSV by a primary key.
2. Validate each split file against a schema.
3. Write valid rows to Parquet.
4. Write invalid rows to an error CSV with row-level details.
5. Batch the Parquet outputs into larger typed grouped files.

The codebase exposes the same core workflow in three ways:

- CLI modes for direct shell usage
- a localhost-only synchronous HTTP API
- a Python SDK for starting the server and calling the API

## Features

- Schema-driven validation with no hardcoded field mappings.
- Streaming CSV processing for large files.
- Deterministic directory traversal and sorted file processing.
- Progress logging for split, validation, and batch phases.
- Shared service layer for CLI and HTTP execution paths.
- Localhost-only API binding and request-origin enforcement.
- Linux-only binary auto-download in the Python package when no local `gvy` binary is available.

## Requirements

### Go runtime and build

- Go `1.25+`

### Python SDK

- Python `3.10+`

## Build

```bash
go mod tidy
go build -o gvy .
```

## CLI Usage

### Help

```bash
./gvy -h
```

### Mode summary

- `auto`: split, validate, and batch in one run
- `validate`: validate one CSV file or a directory of CSV files
- `split`: split one CSV by primary key
- `batch`: batch Parquet files into grouped outputs
- `server`: start the localhost-only HTTP API

## Quick Start

### Full pipeline

```bash
./gvy input.csv schema_example.json
```

This inferred `auto` mode will:

- split `input.csv`
- validate all split files
- write Parquet outputs to `success/`
- write error CSV outputs to `errors/`
- batch Parquet outputs into `batch_export/`

### Single-file validation

```bash
./gvy -mode validate -schema schema_example.json input.csv
```

### Directory validation

```bash
./gvy -mode validate -schema schema_example.json -dir split -t 8
```

### Split only

```bash
./gvy -mode split input.csv -split-primary-key "Record ID"
```

### Batch only

```bash
./gvy -mode batch -batch-dir success -batch-export-dir batch_export -batch-size 1000 -t 8
```

## CLI Reference

### Auto mode

Explicit:

```bash
./gvy -mode auto <main.csv> <schema.json>
```

Implicit:

```bash
./gvy <main.csv> <schema.json>
```

Behavior:

- runs split, then directory validation, then batch export
- uses the first CSV header as the split key if `-split-primary-key` is omitted
- defaults worker count to roughly 60 percent of CPU cores
- defaults `-batch-size` to `1000`
- defaults `-batch-dir` to `-success-dir`
- defaults `-clear-validation-cache` to `true` only when auto mode is inferred without `-mode`

### Validate mode

Single file:

```bash
./gvy -mode validate -schema schema_example.json input.csv
```

Directory:

```bash
./gvy -mode validate -schema schema_example.json -dir split -t 8
```

Notes:

- if `-schema` is omitted and `schema_example.json` exists in the working directory, that file is used
- `-dir` and single-file input are mutually exclusive

### Split mode

```bash
./gvy -mode split <input.csv>
```

or:

```bash
./gvy -mode split -split-input <input.csv>
```

Notes:

- if `-split-primary-key` is omitted, the first CSV header is auto-detected
- `-split-max-open` controls the split writer LRU cache

### Batch mode

```bash
./gvy -mode batch -batch-dir success -batch-export-dir batch_export -batch-size 1000
```

Notes:

- batch mode clears `-batch-export-dir` by default unless `-clear-validation-cache=false` is passed

### Server mode

```bash
./gvy -mode server -host 127.0.0.1 -port 8080
```

Notes:

- server mode only supports loopback hosts
- requests from non-loopback remote addresses are rejected
- the API is synchronous and only allows one active run at a time

### Key flags

- `-mode <auto|validate|split|batch|server>`
- `-schema <path>`
- `-dir <path>`
- `-t <n>`
- `-write-empty-error`
- `-clear-validation-cache`
- `-success-dir <path>`
- `-error-dir <path>`
- `-split-input <path>`
- `-split-output-dir <path>`
- `-split-primary-key <header>`
- `-split-max-open <n>`
- `-split-missing-file <name>`
- `-batch-dir <path>`
- `-batch-export-dir <path>`
- `-batch-size <n>`
- `-host <addr>`
- `-port <n>`

## Schema Format

Validation behavior is driven by a JSON schema.

Example:

```json
{
  "fields": [
    {
      "name": "Member Number",
      "parquet_name": "member_number",
      "type": "string",
      "required": true,
      "min_length": 1
    },
    {
      "name": "Collection Method",
      "parquet_name": "collection_method",
      "type": "string",
      "required": false,
      "override": "card",
      "default": "unknown",
      "lower": true,
      "inline_replace": {
        "crad": "card"
      }
    },
    {
      "name": "Coverage Start Date",
      "parquet_name": "coverage_start_date",
      "type": "date",
      "required": true
    }
  ]
}
```

### Supported field properties

- `name`: source CSV header name
- `parquet_name`: Parquet column name, optional
- `type`: `string`, `float`, `int`, or `date`
- `required`: reject missing values
- `exclude_if_missing`: reject immediately if missing
- `min_length`: minimum string length
- `lower`: lowercase string normalization
- `allowed_values`: allowed set for string values
- `inline_replace`: exact replacements before validation
- `override`: replace every input value for the field before validation
- `default`: fallback value
- `non_zero`: reject zero for integer fields
- `date_formats`: custom parse layouts for date fields

### Missing values

The validator treats these values as missing:

- empty string
- `none`
- `null`
- `nan`
- `na`
- `n/a`

### Schema validation rules

The program fails fast on schema errors such as:

- empty `fields`
- duplicate source field names
- duplicate `parquet_name` values
- unsupported types
- empty `inline_replace` keys

## Output Layout

### Validation outputs

For `input.csv`:

- valid rows: `success/input.parquet`
- invalid rows: `errors/input_error.csv`

Error CSV structure:

- `__row_number`
- `__errors`
- original CSV columns

### Split outputs

Split mode writes:

- one CSV file per key into `split/` or the configured split directory
- rows with blank keys into the configured missing-key file, default `missing_keys.csv`

### Batch outputs

Batch mode writes:

- `validation_batch_1.parquet`
- `validation_batch_2.parquet`
- and so on

into the configured batch export directory.

## HTTP API

The server mode exposes a synchronous localhost-only API.

### `GET /health`

Response:

```json
{
  "status": "ok",
  "busy": false,
  "version": "v1"
}
```

### `POST /shutdown`

Response:

```json
{
  "ok": true,
  "message": "shutdown scheduled"
}
```

### `POST /run/validate-auto`

Minimal request:

```json
{
  "input_csv": "/abs/path/file.csv",
  "schema_path": "/abs/path/schema.json"
}
```

Expanded request:

```json
{
  "main_input_csv": "/abs/path/main.csv",
  "schema_path": "/abs/path/schema.json",
  "split_output_dir": "/abs/path/split",
  "split_primary_key": "Member Number",
  "split_max_open": 256,
  "split_missing_file": "missing_keys.csv",
  "threads": 8,
  "write_empty_error": false,
  "clear_validation_cache": true,
  "success_dir": "/abs/path/success",
  "error_dir": "/abs/path/errors",
  "batch_dir": "/abs/path/success",
  "batch_export_dir": "/abs/path/batch_export",
  "batch_size": 1000
}
```

Success response shape:

```json
{
  "ok": true,
  "mode": "auto",
  "outputs": {
    "split_output_dir": "/abs/path/split",
    "success_dir": "/abs/path/success",
    "error_dir": "/abs/path/errors",
    "batch_dir": "/abs/path/success",
    "batch_export_dir": "/abs/path/batch_export"
  },
  "result": {
    "main_input_csv": "/abs/path/main.csv",
    "schema_path": "/abs/path/schema.json",
    "split_primary_key": "Policy Number"
  }
}
```

Error response shape:

```json
{
  "ok": false,
  "error_code": "VALIDATION_FAILED",
  "message": "required schema field \"X\" not found in CSV header"
}
```

### API constraints

- only loopback clients are allowed
- only one active run is allowed at a time
- concurrent run attempts return `409 Conflict` with `BUSY`
- requests require absolute paths
- schema paths must be `.json`
- input CSV paths must be `.csv`
- output directories must be absolute directory paths

## cURL Examples

### Start the server

```bash
./gvy -mode server -host 127.0.0.1 -port 8080
```

### Health check

```bash
curl http://127.0.0.1:8080/health
```

### Run `validate-auto`

```bash
curl -X POST http://127.0.0.1:8080/run/validate-auto \
  -H 'Content-Type: application/json' \
  --data '{
    "input_csv": "/abs/path/input.csv",
    "schema_path": "/abs/path/schema_example.json",
    "split_output_dir": "/tmp/gvy_api_split",
    "success_dir": "/tmp/gvy_api_success",
    "error_dir": "/tmp/gvy_api_errors",
    "batch_export_dir": "/tmp/gvy_api_batch",
    "clear_validation_cache": true
  }'
```

## Python SDK

The repository now includes a pip-installable package: `gvy-sdk`.

### Install from Git

Add this to `requirements.txt`:

```text
gvy-sdk @ git+https://github.com/Appadon/go_validate_yourself.git
```

Then install:

```bash
pip install -r requirements.txt
```

### Import

```python
from gvy_sdk import Gvy
```

### Basic usage

```python
from gvy_sdk import Gvy

with Gvy.start() as gvy:
    result = gvy.run_validate_auto(
        input_csv="/abs/path/input.csv",
        schema_path="/abs/path/schema_example.json",
        split_output_dir="/tmp/gvy_api_split",
        success_dir="/tmp/gvy_api_success",
        error_dir="/tmp/gvy_api_errors",
        batch_export_dir="/tmp/gvy_api_batch",
        clear_validation_cache=True,
    )
    print(result)
```

### Reusable defaults

```python
from gvy_sdk import Gvy

with Gvy.start() as gvy:
    gvy.set_validate_auto_defaults(
        input_csv="/data/input.csv",
        schema_path="/data/schema_example.json",
        split_output_dir="/tmp/gvy_api_split",
        success_dir="/tmp/gvy_api_success",
        error_dir="/tmp/gvy_api_errors",
        batch_export_dir="/tmp/gvy_api_batch",
        clear_validation_cache=True,
    )

    result = gvy.run_validate_auto()
    print(result)
```

### Binary resolution behavior

The SDK resolves the binary in this order:

1. Use the configured `binary_path` if it exists.
2. If not found, download the latest Linux release asset named `gvy` from:

```text
https://github.com/Appadon/go_validate_yourself/releases/latest/download/gvy
```

3. Cache the downloaded binary at:

```text
~/.cache/gvy-sdk/gvy
```

## Project Layout

```text
.
├── main.go
├── internal/
│   ├── api/
│   ├── batchparquet/
│   ├── console/
│   ├── service/
│   ├── splitcsv/
│   └── validator/
├── gvy_sdk/
│   ├── __init__.py
│   └── client.py
├── python_sdk.py
├── pyproject.toml
├── schema_example.json
└── README.md
```

## Operational Notes

- Directory validation exits non-zero if any file fails.
- Partial output files are removed on failed single-file validation or failed batch writes.
- Split mode uses a writer cache to control open file descriptors.
- Large runs benefit from fast local storage and carefully chosen worker counts.
- The current HTTP API is synchronous by design, so the request blocks until the job completes.

## Current Limitations

- API execution is synchronous and single-run only.
- No job queue, progress endpoint, WebSocket, or SSE support yet.
- Python binary auto-download is currently Linux-only.
- The SDK depends on a valid GitHub release asset when no local binary is supplied.
