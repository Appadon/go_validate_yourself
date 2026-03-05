# Go Validate Yourself (`gvy`)

High-throughput CSV data quality pipeline written in Go.

`gvy` can:
- split a large CSV into per-key CSV files,
- validate CSV rows against a JSON schema,
- write valid rows to Parquet,
- write invalid rows to error CSV files,
- batch parquet files into grouped outputs,
- run the full split+validate+batch pipeline in one command (auto mode).

## Table of Contents
- [What It Does](#what-it-does)
- [Core Concepts](#core-concepts)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Modes](#cli-modes)
- [CLI Reference](#cli-reference)
- [Schema Reference](#schema-reference)
- [Output Contracts](#output-contracts)
- [Exit Codes and Failure Behavior](#exit-codes-and-failure-behavior)
- [Performance and Operations](#performance-and-operations)
- [Project Layout](#project-layout)

## What It Does
Given one or more CSV files plus a schema definition:

1. It validates each configured field per row.
2. Valid rows are converted into typed Parquet columns.
3. Invalid rows are preserved in a CSV with row-level error details.

Optional split mode first partitions one big CSV by a primary key, then those split files can be validated in parallel.

## Core Concepts
- Schema-driven validation: no hardcoded field logic.
- Strict output separation:
  - `success/*.parquet` for valid rows.
  - `errors/*_error.csv` for invalid rows.
- Deterministic file ordering in directory mode (`.csv` files sorted by name).
- Streaming processing for large files.
- Progress logs every ~2 seconds for long-running split, directory-validation, and batch phases.

## Installation

### Requirements
- Go `1.25+` (as defined in `go.mod`)

### Build
```bash
go mod tidy
go build -o gvy .
```

### Help
```bash
./gvy -h
```

## Quick Start

### 1) Run the full pipeline in one command (auto mode)
```bash
./gvy example_dataset.csv example_schema.json
```
This will:
- auto-detect split key as the first CSV header column (unless `-split-primary-key` is provided),
- split into `-split-output-dir` (default `split`),
- validate all split files into `-success-dir` and `-error-dir`,
- batch parquet outputs from `-batch-dir` (default `-success-dir`) into `-batch-export-dir` (default `batch_export`) using `-batch-size` (default `1000`).

### 2) Validate one CSV file
```bash
./gvy \
  -schema example_schema.json \
  -success-dir success \
  -error-dir errors \
  example.csv
```

### 3) Split one large CSV by primary key
```bash
./gvy \
  -split-input example_dataset.csv \
  -split-primary-key "Record ID" \
  -split-output-dir split
```

### 4) Validate all CSV files in a directory with 8 workers
```bash
./gvy \
  -schema example_schema.json \
  -dir split \
  -t 8 \
  -success-dir success \
  -error-dir errors
```

## CLI Modes

### 1) Auto mode (split + validate)
Auto mode can be selected explicitly:

```bash
./gvy -mode auto <main.csv> <schema.json>
```

or inferred when `-mode` is omitted and positional args begin with:

```text
<main.csv> <schema.json>
```

Behavior:
- Split phase runs first.
- Validation phase runs on the split output directory.
- Batch phase runs on parquet files from `-batch-dir` (default `-success-dir`).
- If `-split-primary-key` is omitted, the first header in `<main.csv>` is used.
- If `-t` is omitted, worker threads default to ~60% of CPU cores (`max(1, int(0.6 * NumCPU))`).
- `-t` applies to both directory validation and parquet batching worker pools.
- If `-batch-size` is omitted, it defaults to `1000`.
- `-write-empty-error` defaults to `false`.
- `-clear-validation-cache` defaults to:
  - `true` when auto mode is inferred (no `-mode` provided),
  - `false` when auto mode is explicit (`-mode auto`), unless the flag is passed.
  - In batch mode, clearing `-batch-export-dir` defaults to `true` unless you explicitly pass `-clear-validation-cache=false`.

### 2) Split-only mode
Use `-mode split` when you only want to partition one CSV by key.

`<input.csv>` can be provided either positionally or via `-split-input`.
If `-split-primary-key` is omitted, the first CSV header is auto-detected.

Example:
```bash
./gvy \
  -mode split \
  example_dataset.csv \
  -split-output-dir split \
  -split-max-open 256 \
  -split-missing-file missing_keys.csv
```

### 3) Validation mode
Use `-mode validate` for validation-only execution.

You can validate:
- a single CSV file, or
- all CSV files in a directory (`-dir`).

Single-file:
```bash
./gvy -mode validate -schema example_schema.json input.csv
```

Directory:
```bash
./gvy -mode validate -schema example_schema.json -dir split -t 8
```

### 4) Batch mode
Use `-mode batch` to group parquet files into fixed-size batches.
By default, batch mode clears `-batch-export-dir` before writing output files.

Example:
```bash
./gvy \
  -mode batch \
  -t 8 \
  -batch-dir success \
  -batch-size 1000 \
  -batch-export-dir batch_export
```

## CLI Reference

### Flags
- `-mode <auto|validate|split|batch>`: explicit execution mode. If omitted, mode is inferred.
- `-schema <path>`: schema JSON path.
- `-dir <path>`: directory containing CSV files for validation mode.
- `-t <n>`: worker count for directory validation and batch mode (default is ~60% of CPU cores when omitted).
- `-write-empty-error`: write empty error CSV outputs for valid files (default `false`).
- `-clear-validation-cache`: in auto mode, clears split/success/error/batch_export directories; in batch mode, clears `-batch-export-dir` before batching (default `true` for batch mode unless explicitly set to false).
- `-success-dir <path>`: Parquet output directory (default `success`).
- `-error-dir <path>`: error CSV output directory (default `errors`).
- `-split-input <path>`: input CSV for split-only mode.
- `-split-output-dir <path>`: split file output directory (default `split`).
- `-split-primary-key <header>`: header used as split key.
- `-split-max-open <n>`: max open split file writers (default `256`).
- `-split-missing-file <name>`: output filename for blank split-key rows (default `missing_keys.csv`).
- `-batch-size <n>`: parquet files per batch output (default `1000`).
- `-batch-dir <path>`: parquet input directory for batch mode (in auto mode defaults to `-success-dir`).
- `-batch-export-dir <path>`: batch output directory (default `batch_export`).

### Positional Arguments
- Auto mode:
  - `<main.csv> <schema.json>`
- Validation mode:
  - `<input.csv>` for single-file validation, or no positional when `-dir` is used.
- Split mode:
  - `<input.csv>` when not using `-split-input`.
- Batch mode:
  - `<input_dir>` when not using `-batch-dir`.

### Important Argument Rules
- Flags can be placed before or after positional args.
- Inferred auto mode expects positional shape `<main.csv> <schema.json>`.
- If `-dir` is used with inferred auto shape, CLI prints an INFO message guiding `-mode validate`.
- Validation mode uses `policy_schema.json` by default when `-schema` is not provided and that file exists.
- Use either single-file input or `-dir` for validation mode (not both).
- In split mode and auto mode, omitting `-split-primary-key` enables key auto-detection (first header).

## Schema Reference
`schema.json` drives all CSV validation and Parquet typing. The structure below is an anonymized, production-style template based on `policy_schema.json` rule patterns.

### Schema Template
```json
{
  "fields": [
    {"name": "Record ID", "parquet_name": "record_id", "type": "string", "required": true, "min_length": 1},
    {"name": "Record Group ID", "parquet_name": "record_group_id", "type": "string", "required": true, "min_length": 1},
    {"name": "Coverage Start Date", "parquet_name": "coverage_start_date", "type": "date", "required": true},
    {"name": "Coverage End Date", "parquet_name": "coverage_end_date", "type": "date", "required": false},
    {"name": "Lifecycle Status", "parquet_name": "lifecycle_status", "type": "string", "required": true, "min_length": 1, "lower": true},
    {"name": "Lifecycle Status Reason", "parquet_name": "lifecycle_status_reason", "type": "string", "required": true, "min_length": 1, "lower": true},
    {"name": "Primary Subject Identifier", "parquet_name": "primary_subject_identifier", "type": "string", "required": true, "min_length": 1},
    {"name": "Billing Party Identifier", "parquet_name": "billing_party_identifier", "type": "string", "required": true, "min_length": 1},
    {"name": "Product Identifier", "parquet_name": "product_identifier", "type": "string", "required": true, "min_length": 1},
    {"name": "Marketing Source", "parquet_name": "marketing_source", "type": "string", "required": true, "min_length": 1},
    {
      "name": "Collection Method",
      "parquet_name": "collection_method",
      "type": "string",
      "required": false,
      "exclude_if_missing": false,
      "min_length": 1,
      "default": "default_payment_method",
      "lower": true,
      "inline_replace": {"crad": "card"}
    },
    {"name": "Contract Effective Date", "parquet_name": "contract_effective_date", "type": "date", "required": true},
    {"name": "Contract Start Date", "parquet_name": "contract_start_date", "type": "date", "required": true},
    {"name": "Covered Subject Identifier", "parquet_name": "covered_subject_identifier", "type": "string", "required": true, "min_length": 1},
    {"name": "Covered Amount", "parquet_name": "covered_amount", "type": "float", "required": false, "default": 0.0},
    {"name": "Maximum Covered Amount", "parquet_name": "maximum_covered_amount", "type": "float", "required": false, "default": 0.0},
    {"name": "Total Charge", "parquet_name": "total_charge", "type": "float", "required": false, "default": 0.0},
    {"name": "Secondary Subject Identifier", "parquet_name": "secondary_subject_identifier", "type": "string", "required": true, "min_length": 1},
    {"name": "Relationship Category", "parquet_name": "relationship_category", "type": "string", "required": true, "min_length": 1},
    {"name": "Distribution Code", "parquet_name": "distribution_code", "type": "string", "required": false, "min_length": 1},
    {"name": "Period Code", "parquet_name": "period_code", "type": "int", "required": false, "default": 0},
    {"name": "Acquisition Channel", "parquet_name": "acquisition_channel", "type": "string", "required": false, "default": "unknown", "min_length": 1},
    {"name": "Servicing Region", "parquet_name": "servicing_region", "type": "string", "required": true, "min_length": 1},
    {"name": "Contract Maturity Date", "parquet_name": "contract_maturity_date", "type": "date", "required": true}
  ]
}
```

Use this as a pattern:
- Replace each `name` with your real CSV header (exact match required).
- Keep anonymized names out of production schemas; they are examples only.
- Keep `parquet_name` in stable `snake_case` for downstream analytics.

### Field Properties
- `name` (string, required): source CSV header name (case-sensitive match).
- `parquet_name` (string, optional): output Parquet column name. Auto-generated from `name` (snake_case) when empty.
- `type` (string, required): one of `string | float | int | date`.
- `required` (bool): reject missing/null-like values.
- `exclude_if_missing` (bool): if missing/null-like, reject immediately (takes precedence over `default`).
- `default` (any): fallback for missing/null-like values.
- `min_length` (int): minimum character length for `string`.
- `lower` (bool): lowercase normalization for string processing.
- `allowed_values` ([]string): allowed set for `string` values.
- `inline_replace` (object): exact value replacement map before validation.
- `non_zero` (bool): for `int`, rejects `0`.
- `date_formats` ([]string): parse layouts for `date`. If not provided, defaults are:
  - `2006-01-02`
  - `2006-01-02 15:04:05`
  - `RFC3339`

### Common Field Patterns
- Identifier fields: `type: "string"`, `required: true`, `min_length: 1`.
- Status/category fields: `type: "string"`, usually `lower: true`, optional `allowed_values`.
- Monetary fields: `type: "float"`, often `required: false`, `default: 0.0`.
- Date fields:
  - lifecycle start/critical dates: usually `required: true`
  - optional lifecycle end dates: usually `required: false`
- Channel/method fields:
  - often optional with fallback `default`
  - can use `inline_replace` to normalize known typos before validation.

### Validation Order (Per Field)
For each row/field:
1. Read raw value from CSV header match.
2. Apply `inline_replace` (exact match; case-normalized when `lower=true`).
3. Evaluate missing/null-like.
4. Apply `exclude_if_missing`, `default`, `required` rules.
5. Apply type-specific checks/normalization (`string`, `float`, `int`, `date`).
6. Emit normalized value to Parquet.

### Missing/Null-like Values
These values are treated as missing (case-insensitive):
- `""` (empty)
- `none`
- `null`
- `nan`
- `na`
- `n/a`

### Type Notes
- `string`: optional lowercase conversion, min length, allowed-values enforcement.
- `float`: parsed as `float64`; written as Parquet `DOUBLE`.
- `int`: accepts integer strings; also accepts float-looking values if mathematically integral (e.g. `"10.0"`), rejects fractional values.
- `date`: parsed with configured/default layouts; written as Parquet logical `DATE` (`INT32` days since Unix epoch).

### Schema Validation Constraints
`gvy` fails fast if schema is invalid, including:
- empty `fields`,
- duplicate `name`,
- duplicate `parquet_name`,
- unsupported `type`,
- empty `inline_replace` keys.

## Output Contracts

### Parquet output
For input `path/to/input_file.csv`:
- `success/input_file.parquet`

Only rows that pass all schema field validations are written.

### Error CSV output
For input `path/to/input_file.csv`:
- `errors/input_file_error.csv`

Columns:
- `__row_number`
- `__errors`
- original input header columns

`__errors` contains pipe-separated field errors, for example:
```text
Collection Method: value "cardd" not in allowed_values | Coverage Start Date: invalid date: "2024/13/01"
```

If a run fails while writing outputs, partial parquet/error files for that input are removed.

### Split output
For split mode:
- One CSV file per key under `-split-output-dir`, filename `<key>.csv` (with `/`, `\`, NUL sanitized).
- Rows with blank split keys are written to `-split-missing-file` (created only when needed).

## Exit Codes and Failure Behavior
- `0`: success.
- `1`: runtime/process failure (including split/validation errors, or directory validation with failed files).
- `2`: invalid usage in argument-validation paths.

Additional behavior:
- Directory validation exits non-zero if any file fails.
- In validation mode, output directories are created automatically.
- In inferred auto mode (no `-mode` flag), cache clearing is enabled by default.
- In explicit `-mode auto`, cache clearing is disabled by default unless `-clear-validation-cache` is set.

## Performance and Operations
- Directory mode uses a worker pool (`-t`).
- Split mode controls file descriptor pressure with LRU writer cache (`-split-max-open`).
- Progress logs include throughput, elapsed time, and ETA.
- For large runs:
  - place input/output on fast local storage,
  - tune `-t` based on CPU and I/O (default is ~60% of CPU cores),
  - tune `-split-max-open` based on OS file-descriptor limits.

## Project Layout
```text
.
├── main.go                     # CLI and mode orchestration
├── internal/
│   ├── validator/validator.go  # schema loading, row validation, parquet/error writing
│   ├── splitcsv/split.go       # streaming CSV split by primary key
│   └── console/console.go      # structured and progress logging
├── schema.example.json         # minimal schema example
└── README.md
```
