# Go Validate Yourself (`gvy`)

`gvy` is a high-throughput CSV validation and Parquet export pipeline written in Go.

It can:

- split a large CSV into smaller CSV files by primary key
- validate CSV rows against a JSON schema
- write valid rows to Parquet
- write invalid rows to error CSV files
- batch Parquet outputs into grouped Parquet files
- expose the same pipeline through a localhost-only HTTP API and Python SDK

The current center of the system is config-first: `internal/config` defines the canonical run configuration, and CLI, HTTP, and SDK entry points resolve that config before execution.

## Requirements

- Go `1.25+`
- Python `3.10+` for the SDK

## Build

```bash
go mod tidy
go build -o gvy .
```

## CLI Usage

Print complete CLI help:

```bash
./gvy -h
```

### Common Commands

Full auto pipeline, using legacy CLI shorthand:

```bash
./gvy input.csv schema.example.json
```

Explicit auto mode:

```bash
./gvy -mode auto input.csv schema.example.json
```

Config-first run:

```bash
./gvy -config gvy.config.json
```

Preview the effective resolved config without running:

```bash
./gvy -config gvy.config.json -print-config
```

Single-file validation:

```bash
./gvy -mode validate -schema schema.example.json input.csv
```

Directory validation:

```bash
./gvy -mode validate -schema schema.example.json -dir split -t 8
```

Split only:

```bash
./gvy -mode split input.csv -split-primary-key "Record ID"
```

Batch only:

```bash
./gvy -mode batch -batch-dir success -batch-export-dir batch_export -batch-size 1000 -t 8
```

Start the HTTP API and browser UI:

```bash
./gvy
```

This starts server mode on `http://127.0.0.1:1818/`. You can still override the bind address explicitly:

```bash
./gvy -mode server -host 127.0.0.1 -port 1818
```

## Config-First Usage

GVY has two JSON file types:

- GVY run config controls pipeline phases, inputs, outputs, runtime settings, and server settings.
- Validation schema describes CSV field validation rules, such as required fields, types, defaults, and allowed values.

The run config usually points at a validation schema with `inputs.schema`; it does not replace the schema.

A minimal full pipeline config:

```json
{
  "mode": "auto",
  "inputs": {
    "main_csv": "input.csv",
    "schema": "schema.example.json"
  }
}
```

Run it:

```bash
./gvy -config gvy.config.json
```

Override a config value from the CLI:

```bash
./gvy -config gvy.config.json -t 12
```

`mode` presets expand to phases:

- `auto`: `split`, `validate`, `batch`
- `split`: `split`
- `validate`: `validate`
- `batch`: `batch`
- `server`: runtime entry point only, not a data phase

Explicit `pipeline.phases` overrides `mode`:

```json
{
  "mode": "auto",
  "pipeline": {
    "phases": ["validate", "batch"]
  },
  "inputs": {
    "schema": "schema.example.json",
    "validate_dir": "split"
  }
}
```

Useful config fields:

```json
{
  "mode": "auto",
  "pipeline": {
    "phases": ["split", "validate", "batch"],
    "resume_policy": "reuse_valid_outputs"
  },
  "inputs": {
    "main_csv": "input.csv",
    "schema": "schema.example.json",
    "validate_csv": "",
    "validate_dir": ""
  },
  "outputs": {
    "split_dir": "split",
    "success_dir": "success",
    "error_dir": "errors",
    "batch_export_dir": "batch_export"
  },
  "split": {
    "primary_key": "Record ID",
    "max_open_writers": 256,
    "missing_keys_file": "missing_keys.csv",
    "reuse_cache": true
  },
  "validation": {
    "write_empty_error": false,
    "clear_outputs": false
  },
  "batch": {
    "input_dir": "",
    "size": 1000,
    "clear_output": false
  },
  "runtime": {
    "workers": 8
  },
  "server": {
    "host": "127.0.0.1",
    "port": 1818,
    "workspace_dir": ".gvy/runs"
  }
}
```

Omitted fields use `internal/config.Defaults()`. CLI flags can still override config file values, for example:

```bash
./gvy -config gvy.config.json -phases validate,batch -dir split -t 12
```

### Phase Config Examples

Split only:

```json
{
  "mode": "split",
  "pipeline": {
    "phases": ["split"]
  },
  "inputs": {
    "main_csv": "input.csv"
  },
  "outputs": {
    "split_dir": "split"
  },
  "split": {
    "primary_key": "Record ID"
  }
}
```

Validate from an existing split directory:

```json
{
  "mode": "validate",
  "pipeline": {
    "phases": ["validate"]
  },
  "inputs": {
    "schema": "schema.example.json",
    "validate_dir": "split"
  },
  "outputs": {
    "success_dir": "success",
    "error_dir": "errors"
  },
  "runtime": {
    "workers": 8
  }
}
```

Validate and batch from an existing split directory:

```json
{
  "mode": "auto",
  "pipeline": {
    "phases": ["validate", "batch"]
  },
  "inputs": {
    "schema": "schema.example.json",
    "validate_dir": "split"
  },
  "outputs": {
    "success_dir": "success",
    "error_dir": "errors",
    "batch_export_dir": "batch_export"
  },
  "runtime": {
    "workers": 8
  }
}
```

Batch from an existing Parquet directory:

```json
{
  "mode": "batch",
  "pipeline": {
    "phases": ["batch"]
  },
  "batch": {
    "input_dir": "success",
    "size": 1000
  },
  "outputs": {
    "batch_export_dir": "batch_export"
  },
  "runtime": {
    "workers": 8
  }
}
```

## CLI Modes

### Auto

```bash
./gvy -mode auto <main.csv> <schema.json>
```

Auto mode runs split, then directory validation, then batch export. If `-split-primary-key` is omitted, the first CSV header is used. Split output is reused when the input hash and split settings match.

### Validate

Single file:

```bash
./gvy -mode validate -schema schema.example.json input.csv
```

Directory:

```bash
./gvy -mode validate -schema schema.example.json -dir split -t 8
```

`-dir` and a positional input CSV are mutually exclusive.

### Split

```bash
./gvy -mode split <input.csv>
```

or:

```bash
./gvy -mode split -split-input <input.csv>
```

### Batch

```bash
./gvy -mode batch -batch-dir success -batch-export-dir batch_export -batch-size 1000
```

Batch mode reads Parquet files from `-batch-dir` and writes grouped Parquet outputs into `-batch-export-dir`.

## Key Flags

- `-config <path>`: GVY config JSON file
- `-print-config`: print resolved effective config and exit
- `-phases <list>`: override phases, for example `split,validate,batch`
- `-mode <auto|validate|split|batch|server>`
- `-schema <path>`: validation schema JSON
- `-dir <path>`: directory of CSV files for validate mode
- `-t <n>`: worker count
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

Supported field properties:

- `name`: source CSV header name
- `parquet_name`: Parquet column name, optional
- `type`: `string`, `float`, `int`, `date`, or `datetime`
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
- `datetime_formats`: custom parse layouts for datetime fields

Missing values are: empty string, `none`, `null`, `nan`, `na`, and `n/a`.

## Output Layout

Validation outputs for `input.csv`:

- valid rows: `success/input.parquet`
- invalid rows: `errors/input_error.csv`

Error CSV files include:

- `__row_number`
- `__errors`
- original CSV columns

Split outputs:

- one CSV file per key in `split/` or the configured split directory
- rows with blank keys in `missing_keys.csv` or the configured missing-key file

Batch outputs:

- `validation_batch_1.parquet`
- `validation_batch_2.parquet`
- and so on, in the configured batch export directory

## HTTP API

Start the server:

```bash
./gvy
```

The API is localhost-only and keeps one active run at a time. Non-loopback requests are rejected.
Open `http://127.0.0.1:1818/` in a browser for the UI. The UI fetches backend defaults from
`GET /api/config/defaults`, previews the effective config with `POST /api/config/resolve`, then starts
config-first runs with `POST /api/runs/config`.

### Health

```http
GET /health
```

Example:

```bash
curl -s http://127.0.0.1:1818/health
```

### Config Defaults

```http
GET /api/config/defaults
```

Returns `internal/config.Defaults()`:

```bash
curl -s http://127.0.0.1:1818/api/config/defaults
```

### Config Resolve

```http
POST /api/config/resolve
```

Strictly decodes a GVY config JSON object, rejects unknown fields, resolves defaults and phase-derived inputs, and returns `resolved_config` without executing.

```bash
curl -s -X POST http://127.0.0.1:1818/api/config/resolve \
  -H 'Content-Type: application/json' \
  --data '{
    "mode": "auto",
    "inputs": {
      "main_csv": "input.csv",
      "schema": "schema.example.json"
    }
  }'
```

### Config Run

```http
POST /api/runs/config
```

Preferred run endpoint for new clients. It accepts a GVY config JSON object, resolves it, executes through `service.RunPipeline`, and returns run metadata plus the resolved config.

```bash
curl -s -X POST http://127.0.0.1:1818/api/runs/config \
  -H 'Content-Type: application/json' \
  --data '{
    "mode": "auto",
    "inputs": {
      "main_csv": "/abs/path/input.csv",
      "schema": "/abs/path/schema.example.json"
    },
    "outputs": {
      "split_dir": "/abs/path/split",
      "success_dir": "/abs/path/success",
      "error_dir": "/abs/path/errors",
      "batch_export_dir": "/abs/path/batch_export"
    },
    "validation": {
      "clear_outputs": true
    }
  }'
```

Successful responses include:

- `run`: run snapshot metadata
- `resolved_config`: effective config after defaults and derived inputs
- `result`: pipeline result

### Run Inspection

Config runs and UI/upload runs can be inspected:

```http
GET /api/runs/{run_id}
GET /api/runs/{run_id}/result
GET /api/runs/{run_id}/events
```

### Browser File Selection Runs

```http
POST /api/runs
```

With `Content-Type: application/json`, this endpoint accepts UI-selected files under the server working root:

```json
{
  "csv_path": "incoming/input.csv",
  "schema_path": "schemas/schema.json"
}
```

These paths are constrained to the server working directory.

### Compatibility: Validate Auto

```http
POST /run/validate-auto
```

This endpoint remains for legacy callers. New clients should prefer `POST /api/runs/config`.

Minimal legacy request:

```json
{
  "input_csv": "/abs/path/file.csv",
  "schema_path": "/abs/path/schema.json"
}
```

Legacy `/run/validate-auto` keeps absolute-path behavior and maps its request into config internally before running the pipeline.

### Shutdown

```http
POST /shutdown
```

```bash
curl -s -X POST http://127.0.0.1:1818/shutdown
```

## Python SDK

Install from Git:

```text
gvy-sdk @ git+https://github.com/Appadon/go_validate_yourself.git
```

Import:

```python
from gvy_sdk import Gvy
```

Recommended: send a GVY run config payload to the config-first endpoint:

```python
from gvy_sdk import Gvy

config = {
    "mode": "auto",
    "pipeline": {
        "phases": ["split", "validate", "batch"],
    },
    "inputs": {
        "main_csv": "/abs/path/input.csv",
        "schema": "/abs/path/schema.example.json",
    },
}

with Gvy.start(binary_path="./gvy") as gvy:
    preview = gvy.resolve_config(config)
    result = gvy.run_config(config)
```

Load and run a config JSON file from Python:

```python
from gvy_sdk import Gvy

with Gvy.start(binary_path="./gvy") as gvy:
    preview = gvy.resolve_config(config_path="gvy.config.json")
    result = gvy.run_config(config_path="gvy.config.json")
```

`config_path` is loaded client-side and sent as JSON. Relative paths inside the config are still interpreted by the
GVY server process.

Discover server defaults:

```python
with Gvy.start(binary_path="./gvy") as gvy:
    defaults = gvy.config_defaults()
```

Simple auto convenience usage:

```python
from gvy_sdk import Gvy

with Gvy.start(binary_path="./gvy") as gvy:
    result = gvy.validate_auto(
        "/abs/path/input.csv",
        "/abs/path/schema.example.json",
        batch_export_dir="/tmp/gvy_api_batch",
        clear_validation_cache=True,
    )
    print(result)
```

Compatibility method with the older SDK method name:

```python
with Gvy.start(binary_path="./gvy") as gvy:
    result = gvy.run_validate_auto(
        input_csv="/abs/path/input.csv",
        schema_path="/abs/path/schema.example.json",
    )
```

`validate_auto(...)` and `run_validate_auto(...)` keep the old SDK method names, construct an explicit
`split`, `validate`, `batch` config, and send it to `/api/runs/config`. `validate_auto_defaults` is retained as
a compatibility overlay; new code should fetch defaults with `config_defaults()` instead of recreating backend
defaults in Python.

The SDK resolves the binary in this order:

1. Use the configured `binary_path` if it exists.
2. If not found on Linux, download the latest release asset named `gvy`.
3. Cache the downloaded binary at `~/.cache/gvy-sdk/gvy`.

## Project Layout

```text
.
├── main.go
├── internal/
│   ├── api/
│   ├── batchparquet/
│   ├── config/
│   ├── console/
│   ├── service/
│   ├── splitcsv/
│   └── validator/
├── gvy_sdk/
│   ├── __init__.py
│   └── client.py
├── pyproject.toml
├── schema.example.json
└── README.md
```

## Operational Notes

- Directory validation exits non-zero if any file fails.
- Partial output files are removed on failed single-file validation or failed batch writes.
- Full auto compatibility runs reuse split cache and can clear validation and batch outputs without deleting reusable split output.
- New API clients should use `/api/config/defaults`, `/api/config/resolve`, and `/api/runs/config` rather than duplicating defaults client-side.
