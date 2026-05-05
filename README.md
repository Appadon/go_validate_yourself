# Go Validate Yourself

<p align="center">
  <strong>GVY</strong><br>
  A localhost-first CSV validation console for splitting large files, validating rows, exporting Parquet, and reviewing errors without leaving the browser.
</p>

<p align="center">
  <code>web console</code> · <code>config-first runs</code> · <code>CSV to Parquet</code> · <code>schema inference</code> · <code>Python SDK</code>
</p>

```text
+--------------------------------------------------------------+
| GVY                                                          |
| Multithreaded data validation framework                      |
+--------------------------------------------------------------+
```

## What GVY Does

GVY turns a CSV validation job into a repeatable pipeline:

| Phase | Purpose | Main output |
| --- | --- | --- |
| `split` | Split one large CSV into smaller CSV files by primary key. | `split/` |
| `validate` | Validate a CSV file or directory against a JSON schema. | `success/*.parquet`, `errors/*_error.csv` |
| `batch` | Group generated Parquet files into larger batch files. | `batch_export/validation_batch_*.parquet` |

The current app is **config-first**. The browser UI, CLI, HTTP API, and Python SDK all resolve the same GVY run config before execution, so a run preview in the UI maps directly to the payload sent to `/api/runs/config`.

## Web Console

Start GVY with no arguments:

```bash
./gvy
```

Then open:

```text
http://127.0.0.1:1818/
```

The console is built around a six-step workflow:

| Step | Screen | What happens |
| --- | --- | --- |
| 1 | Input | Select a main CSV, validation CSV/directory, or batch input directory from the server working root. |
| 2 | Schema | Load a schema, generate one from a CSV sample, or edit an existing schema. |
| 3 | Options | Choose `split`, `validate`, and `batch`; set workers, output directories, cache behavior, batch size, and resume policy. |
| 4 | Confirm | Preview the resolved server config before starting. |
| 5 | Progress | Watch live phase progress, run state, row counts, and recent diagnostics. |
| 6 | Review | Copy or download the run report and open the Error Explorer. |

The UI style matches the CLI help screen: compact panels, green/cyan status accents, and a workflow-first layout instead of a raw flag form.

### Included UI Tools

| Tool | Route | Notes |
| --- | --- | --- |
| Run Console | `/` | Main run wizard and progress view. |
| Schema Inference | `/schema-infer` | Samples a CSV and proposes a validation schema. |
| Schema Editor | `/schema-editor` | Loads, edits, validates, and saves schema JSON under the working root. |
| Schema Workbench | `/schema-workbench` | Combined schema browsing, inference, and editing workspace. |
| Error Explorer | `/error-explorer` | Summarizes error CSV files by field, message, file, and sample rows. |

Browser file access is intentionally scoped to the process working directory. The API is loopback-only and allows one active run at a time.

## Requirements

| Runtime | Version |
| --- | --- |
| Go | `1.25+` |
| Python SDK | `3.10+` |

## Quick Start

Build the binary:

```bash
go mod tidy
go build -o gvy .
```

Launch the console:

```bash
./gvy
```

Run the full pipeline from the CLI:

```bash
./gvy main.csv schema.json
```

Validate one CSV:

```bash
./gvy -mode validate input.csv -schema schema.json
```

Validate a directory:

```bash
./gvy -mode validate -dir split/ -schema schema.json
```

Run a saved config:

```bash
./gvy -config gvy.config.json
```

Preview a saved config without executing:

```bash
./gvy -config gvy.config.json -print-config
```

## Command Shapes

```text
gvy
gvy <main.csv> <schema.json> [flags]
gvy -mode validate <input.csv> [-schema <schema.json>] [flags]
gvy -mode validate -dir <input_dir> [-schema <schema.json>] [flags]
gvy -mode split <input.csv> [flags]
gvy -mode batch -batch-dir <input_dir> [flags]
gvy -mode server [-host 127.0.0.1] [-port 1818]
gvy -config gvy.config.json [flags]
```

Print the built-in help UI:

```bash
./gvy -h
```

## Modes

| Mode | Behavior | Required input |
| --- | --- | --- |
| `server` | Starts the localhost web console and HTTP API. This is the default when no args are passed. | None |
| `auto` | Runs `split`, then `validate`, then `batch`. | `<main.csv> <schema.json>` |
| `validate` | Validates one CSV file or every CSV file in a directory. | `<input.csv>` or `-dir <input_dir>` plus schema |
| `split` | Splits one CSV into smaller CSV files by primary key. | `<input.csv>` or `-split-input <input.csv>` |
| `batch` | Groups Parquet files into batched Parquet outputs. | `-batch-dir <input_dir>` |

Useful examples:

```bash
./gvy main.csv schema.json -t 10
./gvy -mode validate -dir split/ -schema schema.json
./gvy -mode split main.csv -split-primary-key policy_number
./gvy -mode batch -batch-dir success/ -batch-export-dir batch_export
./gvy -config gvy.config.json -phases validate,batch -dir split/
```

## Run Config

GVY uses two JSON document types:

| File | Purpose |
| --- | --- |
| GVY run config | Chooses phases, inputs, outputs, runtime settings, and server settings. |
| Validation schema | Describes field-level CSV validation rules. |

A minimal full-pipeline config:

```json
{
  "mode": "auto",
  "inputs": {
    "main_csv": "main.csv",
    "schema": "schema.json"
  }
}
```

An explicit config with current defaults shown:

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
    "workers": 0
  },
  "server": {
    "host": "127.0.0.1",
    "port": 1818,
    "workspace_dir": ".gvy/runs"
  }
}
```

Notes:

- `pipeline.phases` overrides the `mode` preset.
- `auto` expands to `split`, `validate`, `batch`.
- `runtime.workers: 0` means GVY picks its default worker count.
- When `validate` follows `split`, `inputs.validate_dir` is derived from `outputs.split_dir`.
- When `batch` follows `validate`, `batch.input_dir` is derived from `outputs.success_dir`.
- Resume policies are `reuse_valid_outputs`, `start_at_first_missing`, and `run_all`.

## Validation Schema

Schemas describe how CSV columns are normalized, validated, and written to Parquet.

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
| `name` | Source CSV header. |
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

## HTTP API

Start the server:

```bash
./gvy
```

Useful endpoints:

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
| `POST` | `/api/schema/infer` | Infer a schema from a CSV sample. |
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

## Python SDK

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
errors/input_error.csv
```

Error CSV files include:

```text
__row_number,__errors,<original CSV columns...>
```

Split writes one CSV per key into `split/` by default. Rows with blank split keys are written to `missing_keys.csv` unless configured otherwise.

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
├── main.go
├── internal/
│   ├── api/           # localhost HTTP API and UI routes
│   ├── config/        # canonical run config and resolver
│   ├── help/          # styled CLI help renderer
│   ├── schemaeditor/  # schema load/save normalization
│   ├── schemainfer/   # CSV sampling and schema inference
│   ├── service/       # pipeline orchestration
│   ├── splitcsv/      # split phase
│   └── validator/     # schema validation and Parquet writing
├── web/
│   ├── templates/     # console, schema, and error explorer pages
│   └── static/        # CSS and browser JavaScript
├── gvy_sdk/           # Python SDK
├── tests/             # Python SDK tests
├── schema.example.json
└── README.md
```

## Development

Run Go tests:

```bash
go test ./...
```

Run Python SDK tests:

```bash
pytest -q
```

Operational notes:

- Directory validation exits non-zero if any input file fails.
- Failed single-file validation and failed batch writes remove partial output files.
- Full auto runs can reuse compatible split output while clearing validation and batch outputs.
- New clients should not duplicate defaults client-side; ask the server with `/api/config/defaults`.
