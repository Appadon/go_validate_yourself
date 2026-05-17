<p align="center">
  <img alt="GVY" src="https://img.shields.io/badge/GVY-Go%20Validate%20Yourself-4B8DFF?style=for-the-badge&labelColor=06080d">
</p>

<p align="center">
    Local Data validation pipeline with schema based validation.
</p>

<p align="center">
  <img alt="Console" src="https://img.shields.io/badge/console-local-121827?style=flat-square&labelColor=06080d">
  <img alt="Config first" src="https://img.shields.io/badge/config-first-4B8DFF?style=flat-square&labelColor=06080d">
  <img alt="Parquet" src="https://img.shields.io/badge/parquet-export-B7FF5A?style=flat-square&labelColor=06080d">
  <img alt="Warnings" src="https://img.shields.io/badge/warnings-visible-FFD25F?style=flat-square&labelColor=06080d">
  <img alt="Errors" src="https://img.shields.io/badge/errors-reviewable-FF6D78?style=flat-square&labelColor=06080d">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> |
  <a href="#workflow">Workflow</a> |
  <a href="#web-console">Web console</a> |
  <a href="#configuration">Configuration</a> |
  <a href="#api-and-sdk">API and SDK</a> |
  <a href="#development">Development</a>
</p>

## Why GVY

GVY turns one-off structured data checks into a repeatable local pipeline. The browser UI, CLI, HTTP API, and Python SDK all resolve the same run config before execution, so a preview in the console maps directly to the payload sent to `/api/runs/config`.

| You need to | GVY gives you |
| --- | --- |
| Validate rows against a schema | Clean Parquet output plus row-level error files |
| Review failures quickly | Browser error summaries by field, message, file, and sample row |
| Automate repeat runs | One config model across UI, CLI, API, and SDK |

## Quick Start

Download the Linux release binary and start the console:

```bash
curl -L -o gvy https://github.com/Appadon/go_validate_yourself/releases/latest/download/gvy
chmod +x gvy
./gvy
```

Open the local console:

```text
http://127.0.0.1:1818/
```

Run the full pipeline from the CLI:

```bash
./gvy main.csv schema.json
```

Build from source when you are working inside the repo:

```bash
go mod tidy
go build -o gvy .
./gvy
```

## Workflow

<p>
  <img alt="split" src="https://img.shields.io/badge/1-split-4B8DFF?style=for-the-badge&labelColor=06080d">
  <img alt="validate" src="https://img.shields.io/badge/2-validate-B7FF5A?style=for-the-badge&labelColor=06080d">
  <img alt="batch" src="https://img.shields.io/badge/3-batch-FFD25F?style=for-the-badge&labelColor=06080d">
</p>

Useful commands:

```bash
./gvy main.csv schema.json -t 10
./gvy -mode validate -dir split/ -schema schema.json
./gvy -mode split main.csv -split-primary-key policy_number
./gvy -mode batch -batch-dir success/ -batch-export-dir batch_export
./gvy -config gvy.config.json -phases validate,batch -dir split/
```

## Web Console

Start GVY with no arguments:

```bash
./gvy
```

## CLI Reference

Command shapes:

```text
gvy
gvy <main.csv|main.parquet> <schema.json> [flags]
gvy -mode validate <input.csv|input.parquet> [-schema <schema.json>] [flags]
gvy -mode validate -dir <input_dir> [-schema <schema.json>] [flags]
gvy -mode split <input.csv|input.parquet> [flags]
gvy -mode batch -batch-dir <input_dir> [flags]
gvy -mode server [-host 127.0.0.1] [-port 1818]
gvy -config gvy.config.json [flags]
```

Modes:

| Mode | Behavior | Required input |
| --- | --- | --- |
| `server` | Starts the localhost web console and HTTP API. This is the default when no args are passed. | None |
| `auto` | Runs `split`, then `validate`, then `batch`. | `<main.csv|main.parquet> <schema.json>` |
| `validate` | Validates one data file or every supported file in a directory. | `<input.csv|input.parquet>` or `-dir <input_dir>` plus schema |
| `split` | Splits one structured data file into smaller working files by primary key. | `<input.csv|input.parquet>` or `-split-input <input.csv|input.parquet>` |
| `batch` | Groups Parquet files into batched Parquet outputs. | `-batch-dir <input_dir>` |

Print the built-in help UI:

```bash
./gvy -h
```

## Configuration

GVY uses two JSON document types:

| File | Purpose |
| --- | --- |
| GVY run config | Chooses phases, inputs, outputs, runtime settings, and server settings. |
| Validation schema | Describes field-level structured data validation rules. |

Minimal full-pipeline config:

```json
{
  "mode": "auto",
  "inputs": {
    "main_csv": "main.csv",
    "schema": "schema.json"
  }
}
```

<details>
<summary>Show explicit run config with defaults</summary>

```json
{
  "mode": "auto",
  "pipeline": {
    "phases": ["split", "validate", "batch"],
    "resume_policy": "reuse_valid_outputs"
  },
  "inputs": {
    "main_csv": "main.csv",
    "schema": "schema.json",
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
    "primary_key": "",
    "max_open_writers": 256,
    "missing_keys_file": "missing_keys.parquet",
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
    "workers": 0
  },
  "server": {
    "host": "127.0.0.1",
    "port": 1818,
    "workspace_dir": ".gvy/runs"
  }
}
```

</details>

Config notes:

| Setting | Meaning |
| --- | --- |
| `pipeline.phases` | Overrides the `mode` preset. |
| `auto` | Expands to `split`, `validate`, `batch`. |
| `runtime.workers: 0` | Lets GVY pick its default worker count. |
| `inputs.validate_dir` | Derived from `outputs.split_dir` when `validate` follows `split`. |
| `batch.input_dir` | Derived from `outputs.success_dir` when `batch` follows `validate`. |
| Resume policies | `reuse_valid_outputs`, `start_at_first_missing`, `run_all`. |

## Validation Schema

Schemas describe how source columns are normalized, validated, and written to Parquet.

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

Supported field settings:

| Key | Meaning |
| --- | --- |
| `name` | Source data header. |
| `parquet_name` | Output Parquet column name. |
| `type` | `string`, `float`, `int`, `date`, or `datetime`. |
| `required` | Reject blank or missing values. |
| `exclude_if_missing` | Reject the row immediately when the field is missing. |
| `min_length` | Minimum string length. |
| `lower` | Lowercase string values before writing. |
| `allowed_values` | Accepted string values. |
| `inline_replace` | Exact replacements before validation. |
| `override` | Replace every input value for the field. |
| `default` | Fallback value for missing input. |
| `non_zero` | Reject zero for integer fields. |
| `date_formats` | Custom date parse layouts. |
| `datetime_formats` | Custom datetime parse layouts. |

GVY treats these values as missing: empty string, `none`, `null`, `nan`, `na`, and `n/a`.

## API and SDK

Start the server:

```bash
./gvy
```

Useful HTTP endpoints:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Server status, busy flag, version, working root, and latest run. |
| `GET` | `/api/config/defaults` | Canonical run-config defaults. |
| `POST` | `/api/config/resolve` | Strictly decode and resolve a config without executing. |
| `POST` | `/api/runs/config` | Run a config-first pipeline. |
| `GET` | `/api/runs/{run_id}` | Run snapshot. |
| `GET` | `/api/runs/{run_id}/result` | Terminal result or final error. |
| `GET` | `/api/runs/{run_id}/events` | Server-sent progress events. |
| `GET` | `/api/files?kind=csv` or `/api/files?kind=schema` | Working-root-scoped file browser data. |
| `GET` | `/api/errors/report` | Aggregated validation error report. |
| `GET` / `PUT` | `/api/schema` | Load or save schema JSON. |
| `POST` | `/api/schema/infer` | Infer a schema from a structured data sample. |
| `POST` | `/shutdown` | Stop the local server. |

Resolve a config:

```bash
curl -s -X POST http://127.0.0.1:1818/api/config/resolve \
  -H 'Content-Type: application/json' \
  --data '{
    "mode": "auto",
    "inputs": {
      "main_csv": "main.csv",
      "schema": "schema.json"
    }
  }'
```

Start a config run:

```bash
curl -s -X POST http://127.0.0.1:1818/api/runs/config \
  -H 'Content-Type: application/json' \
  --data '{
    "mode": "auto",
    "inputs": {
      "main_csv": "main.csv",
      "schema": "schema.json"
    },
    "validation": {
      "clear_outputs": true
    }
  }'
```

Legacy callers can still use `POST /run/validate-auto`, but new clients should prefer `/api/config/resolve` and `/api/runs/config`.

### Python SDK

Install from Git:

```text
gvy-sdk @ git+https://github.com/Appadon/go_validate_yourself.git
```

Run a config-first pipeline:

```python
from gvy_sdk import Gvy

config = {
    "mode": "auto",
    "inputs": {
        "main_csv": "/abs/path/main.csv",
        "schema": "/abs/path/schema.json",
    },
}

with Gvy.start(binary_path="./gvy") as gvy:
    preview = gvy.resolve_config(config)
    result = gvy.run_config(config)
```

Run a saved config file:

```python
from gvy_sdk import Gvy

with Gvy.start(binary_path="./gvy") as gvy:
    preview = gvy.resolve_config(config_path="gvy.config.json")
    result = gvy.run_config(config_path="gvy.config.json")
```

Compatibility helpers remain available:

```python
from gvy_sdk import Gvy

with Gvy.start(binary_path="./gvy") as gvy:
    result = gvy.validate_auto(
        "/abs/path/main.csv",
        "/abs/path/schema.json",
        clear_validation_cache=True,
        batch_export_dir="/tmp/gvy_batch",
    )
```

The SDK starts a local GVY server, waits for `/health`, sends JSON requests to the same localhost API used by the browser, and shuts the server down when the context exits. If `./gvy` is not present on Linux, the SDK can download the configured release asset into `~/.cache/gvy-sdk/`.

## Output Layout

For `input.csv`, validation writes:

```text
success/input.parquet
errors/input_error.parquet
```

Validation error files include:

```text
__row_number,__errors,__error_fields,__row_values,__search_text
```

`__row_values` stores the original row columns in order as JSON so the browser explorer can show samples without downloading full error files.

Split writes one Parquet working file per key into `split/` by default. Rows with blank split keys are written to `missing_keys.parquet` unless configured otherwise.

Batch writes:

```text
batch_export/validation_batch_1.parquet
batch_export/validation_batch_2.parquet
...
```

## Environment Controls

| Variable | Effect |
| --- | --- |
| `NO_COLOR=1` | Disable color output. |
| `GVY_COLOR=false` | Force color off. |
| `GVY_COLOR=true` | Force color on. |
| `GVY_CLEAR=false` | Disable startup screen clear. |
| `GVY_ANIMATE=false` | Disable startup animation. |

## Project Layout

```text
.
|-- main.go
|-- internal/
|   |-- api/           # localhost HTTP API and UI routes
|   |-- config/        # canonical run config and resolver
|   |-- help/          # styled CLI help renderer
|   |-- schemaeditor/  # schema load/save normalization
|   |-- schemainfer/   # data sampling and schema inference
|   |-- service/       # pipeline orchestration
|   |-- splitcsv/      # split phase
|   `-- validator/     # schema validation and Parquet writing
|-- web/
|   |-- templates/     # console, schema, and error explorer pages
|   `-- static/        # CSS and browser JavaScript
|-- gvy_sdk/           # Python SDK
|-- tests/             # Python SDK tests
|-- schema.example.json
`-- README.md
```

## Development

Requirements:

| Runtime | Version |
| --- | --- |
| Go | `1.25+` |
| Python SDK | `3.10+` |

Run Go tests:

```bash
go test ./...
```

Run Python SDK tests:

```bash
pytest -q
```

Operational notes:

| Note | Detail |
| --- | --- |
| Directory validation | Exits non-zero if any input file fails. |
| Partial outputs | Failed single-file validation and failed batch writes remove partial output files. |
| Auto runs | Can reuse compatible split output while clearing validation and batch outputs. |
| Client defaults | New clients should ask `/api/config/defaults` instead of duplicating defaults client-side. |
