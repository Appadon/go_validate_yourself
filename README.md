# Go Validate Yourself

`gvy` validates split policy CSV files against a JSON schema and writes:
- validated records to Parquet (`success/<file>.parquet`)
- rejected records to CSV (`errors/<file>_error.csv`)

It supports split mode, single-file validation mode, and high-throughput directory mode with concurrent workers.

## Features
- JSON-configured schema (types, required, defaults, allowed values)
- Optional row exclusion for missing/null-like values (`exclude_if_missing`)
- Parquet output with typed columns (including logical `DATE`)
- Error CSV with row number and field-level validation messages
- Concurrent directory processing (`-dir` + `-t`)
- Progress heartbeat with percentage, throughput, elapsed, and ETA
- Streaming CSV split by primary key into many files (`-split-input`)
- Dedicated `missing_keys.csv` for rows where the split key is blank

## Build
```bash
go mod tidy
go build -o gvy .
```

## Usage

Split one large CSV by primary key:
```bash
./gvy \
  -split-input giant.csv \
  -split-primary-key "Policy Number"
```

Optional split flags (defaults shown):
- `-split-output-dir split`
- `-split-max-open 256`
- `-split-missing-file missing_keys.csv`

Single file:
```bash
./gvy \
  -schema policy_schema.json \
  -success-dir success \
  -error-dir errors \
  input_file.csv [write_empty_error]
```

Directory mode (concurrent):
```bash
./gvy \
  -schema policy_schema.json \
  -dir path_to_files \
  -t 8 \
  -success-dir success \
  -error-dir errors \
  [write_empty_error]
```

CLI help:
```bash
./gvy -h
```

## Arguments
- Positional `<input.csv>`: validates one file.
- Optional positional `[write_empty_error]`: `true|false` (default `false`). When `false`, no error CSV is written for files with zero invalid rows.
- `-dir <path>`: validates all `.csv` files in a directory.
- `-t <n>`: number of workers in directory mode (default `1`).
- `-schema <path>`: schema JSON path (required).
- `-success-dir <path>`: parquet output directory (default `success`).
- `-error-dir <path>`: error CSV output directory (default `errors`).
- `-split-input <path>`: split input CSV by primary key into one-file-per-key output.
- `-split-primary-key <header>`: header name used as split key (required for split mode).
- `-split-output-dir <path>`: output directory for split files (default `split`).
- `-split-max-open <n>`: max concurrently open split writers (default `256`).
- `-split-missing-file <name>`: filename for blank-key rows (default `missing_keys.csv`).

Rules:
- Use either positional `<input.csv>` or `-dir` (not both).
- Output directories are auto-created.
- In split mode, rows with blank split keys are written to `missing_keys.csv`.

## Output files
For input `some/path/input_file.csv`:
- `success/input_file.parquet`
- `errors/input_file_error.csv`

Error CSV columns:
- `__row_number`
- `__errors`
- original CSV columns...

## Schema format
Schema root:
```json
{
  "fields": [
    { "name": "Policy Number", "parquet_name": "policy_number", "type": "string", "required": true, "min_length": 1 }
  ]
}
```

Per-field options:
- `name`: source CSV header to read
- `parquet_name`: output parquet column name (auto-derived if empty)
- `type`: `string | float | int | date`
- `required`: if true, missing/null-like values fail
- `default`: fallback value when missing/null-like (unless excluded by rule below)
- `exclude_if_missing`: if true, missing/null-like values fail even if default exists
- `min_length`: minimum length for `string`
- `lower`: lowercase normalization before validation
- `allowed_values`: accepted set for `string`
- `inline_replace`: per-column exact replacements applied before missing/required/type checks
- `non_zero`: enforce non-zero for `int`
- `date_formats`: parse layouts for `date` (defaults include `2006-01-02` and `2006-01-02 15:04:05`)

Example `inline_replace`:
```json
{
  "name": "Payment Type",
  "type": "string",
  "lower": true,
  "inline_replace": {
    "cahs": "cash",
    "debitorder": "debit order"
  },
  "allowed_values": ["cash/card", "debit order", "cash", "card"]
}
```

Missing/null-like values recognized:
- empty string, `none`, `null`, `nan`, `na`, `n/a` (case-insensitive)

## Operational notes
- Files are processed in stable sorted order.
- If writing a file fails, partial output files for that input are removed.
- Directory mode returns non-zero exit code when one or more files fail to process.
- Extra CSV columns are ignored unless referenced in schema.
