package help

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	gvyconfig "go_validate_yourself/internal/config"
)

const (
	colorReset = "\033[0m"
	colorBold  = "\033[1m"
	colorCyan  = "\033[36m"
	colorGreen = "\033[32m"
	colorGray  = "\033[90m"
)

/* Options controls deterministic CLI help rendering. */
type Options struct {
	BinaryName string
	Color      bool
}

type renderer struct {
	w     io.Writer
	color bool
	err   error
}

/* Print writes the GVY CLI help screen to w. */
func Print(w io.Writer, opts Options) error {
	bin := strings.TrimSpace(opts.BinaryName)
	if bin == "" {
		bin = "gvy"
	}

	r := &renderer{
		w:     w,
		color: isColorEnabled(opts),
	}
	defaults := gvyconfig.Defaults()
	url := fmt.Sprintf("http://%s:%d/", defaults.Server.Host, defaults.Server.Port)

	r.header()
	r.section("Web Console")
	r.linef("  %s", r.command(bin))
	r.linef("  opens %s", r.command(url))

	r.section("Quick Start")
	r.commandBlock("Start the web console:", bin)
	r.commandBlock("Run the full pipeline:", bin+" main.csv schema.json")
	r.commandBlock("Validate one file:", bin+" -mode validate input.csv -schema schema.json")
	r.commandBlock("Validate a directory:", bin+" -mode validate -dir split/ -schema schema.json")
	r.commandBlock("Use a config file:", bin+" -config gvy.config.json")

	r.section("Command Shapes")
	r.commandLine(bin)
	r.commandLine(bin + " <main.csv|main.parquet> <schema.json> [flags]")
	r.commandLine(bin + " -mode validate <input.csv|input.parquet> [-schema <schema.json>] [flags]")
	r.commandLine(bin + " -mode validate -dir <input_dir> [-schema <schema.json>] [flags]")
	r.commandLine(bin + " -mode split <input.csv|input.parquet> [flags]")
	r.commandLine(bin + " -mode batch -batch-dir <input_dir> [flags]")
	r.commandLine(fmt.Sprintf("%s -mode server [-host %s] [-port %d]", bin, defaults.Server.Host, defaults.Server.Port))
	r.commandLine(bin + " -config gvy.config.json [flags]")

	r.section("Modes")
	r.mode("server",
		"Starts the localhost web console and HTTP API.",
		"Default command: "+bin,
		"URL: "+url,
		"Example: "+bin+" -mode server -host 127.0.0.1 -port 1818")
	r.mode("auto",
		"Splits a main CSV or Parquet file, validates split files, then batches parquet output.",
		"Required: <main.csv|main.parquet> <schema.json>",
		"Useful flags: -t <n>, -write-empty-error, -clear-validation-cache",
		"Example: "+bin+" main.csv schema.json -t 10")
	r.mode("validate",
		"Validates one CSV or Parquet file, or every supported file in a directory.",
		"Required: <input.csv|input.parquet> or -dir <input_dir>",
		"Useful flags: -schema <schema.json>, -success-dir <path>, -error-dir <path>",
		"Example: "+bin+" -mode validate -dir split/ -schema schema.json")
	r.mode("split",
		"Splits one CSV or Parquet file into smaller Parquet files by primary key.",
		"Required: <input.csv|input.parquet> or -split-input <path>",
		"Useful flags: -split-primary-key <header>, -split-output-dir <path>",
		"Example: "+bin+" -mode split main.csv -split-primary-key policy_number")
	r.mode("batch",
		"Groups parquet files into batched parquet outputs.",
		"Required: -batch-dir <input_dir>",
		"Useful flags: -batch-size <n>, -batch-export-dir <path>, -t <n>",
		"Example: "+bin+" -mode batch -batch-dir success/ -batch-size 1000")

	r.section("Common Workflows")
	r.commandBlock("Launch browser UI and local API:", bin)
	r.commandBlock("Run selected config phases:", bin+" -config gvy.config.json -phases validate,batch -dir split/")
	r.commandBlock("Inspect resolved config:", bin+" -config gvy.config.json -print-config")

	r.section("Flags")
	r.flagGroup("General",
		[2]string{"-h, -help", "Show this help screen"},
		[2]string{"-mode <mode>", "auto | validate | split | batch | server"},
		[2]string{"-config <path>", "GVY run config JSON file"},
		[2]string{"-print-config", "Print resolved config and exit"},
		[2]string{"-phases <list>", "Override phases: split,validate,batch"},
	)
	r.flagGroup("Server",
		[2]string{"-host <addr>", "Loopback host for server mode (default 127.0.0.1)"},
		[2]string{"-port <n>", "Port for server mode (default 1818)"},
	)
	r.flagGroup("Runtime",
		[2]string{"-t <n>", "Concurrent workers for split finalization, validation, and batch"},
		[2]string{"-clear-validation-cache", "Clear output dirs before compatible runs"},
	)
	r.flagGroup("Validation",
		[2]string{"-schema <path>", "Schema JSON file for validation"},
		[2]string{"-dir <path>", "Directory of CSV or Parquet files to validate"},
		[2]string{"-write-empty-error", "Write empty error Parquet files for valid inputs"},
		[2]string{"-success-dir <path>", "Directory for valid parquet output (default success)"},
		[2]string{"-error-dir <path>", "Directory for error Parquet output (default errors)"},
	)
	r.flagGroup("Split",
		[2]string{"-split-input <path>", "Input CSV or Parquet file for split mode"},
		[2]string{"-split-output-dir <path>", "Output directory for split Parquet files (default split)"},
		[2]string{"-split-primary-key <name>", "Header name used as split key"},
		[2]string{"-split-max-open <n>", "Maximum open split writers (default 256)"},
		[2]string{"-split-missing-file <name>", "File for blank split keys (default missing_keys.parquet)"},
	)
	r.flagGroup("Batch",
		[2]string{"-batch-dir <path>", "Directory containing parquet files for batch mode"},
		[2]string{"-batch-size <n>", "Parquet files per output batch (default 1000)"},
		[2]string{"-batch-export-dir <path>", "Batch output directory (default batch_export)"},
	)

	r.section("Config File")
	r.line("  GVY run config controls phases, inputs, outputs, runtime, and server settings.")
	r.line("  Validation schema is separate and controls field validation.")
	r.line("  CLI flags override config file values.")
	r.line("  pipeline.phases overrides mode.")
	r.line("")
	r.line("  {")
	r.line(`    "mode": "auto",`)
	r.line(`    "inputs": {`)
	r.line(`      "main_csv": "main.csv",`)
	r.line(`      "schema": "schema.json"`)
	r.line("    }")
	r.line("  }")
	r.line("")
	r.line("  See README.md for complete config details.")

	r.section("HTTP API")
	r.linef("  Web console: %s", r.command(url))
	r.line("  Requests are localhost-only.")
	r.line("")
	r.line("  Useful endpoints:")
	r.endpoint("GET", "/health")
	r.endpoint("GET", "/api/config/defaults")
	r.endpoint("POST", "/api/config/resolve")
	r.endpoint("POST", "/api/runs/config")
	r.endpoint("POST", "/shutdown")

	r.section("Environment Controls")
	r.env("NO_COLOR=1", "Disable color output")
	r.env("GVY_COLOR=false", "Force color off")
	r.env("GVY_COLOR=true", "Force color on")
	r.env("GVY_CLEAR=false", "Disable startup screen clear")
	r.env("GVY_ANIMATE=false", "Disable startup animation")

	r.section("Examples")
	r.commandLine(bin + " main.csv schema.json")
	r.commandLine(bin + " -mode validate input.csv -schema schema.json -write-empty-error")
	r.commandLine(bin + " -mode validate -dir split/ -schema schema.json")
	r.commandLine(bin + " -mode split main.csv -split-primary-key policy_number")
	r.commandLine(bin + " -mode batch -batch-dir success/ -batch-export-dir batch_export")
	r.commandLine(bin + " -config gvy.config.json -phases split")
	r.commandLine("curl -s http://127.0.0.1:1818/api/config/defaults")

	return r.renderErr()
}

func (r *renderer) header() {
	border := strings.Repeat("-", 62)
	r.line(r.paint("+"+border+"+", colorGray))
	r.line(r.frame(r.paint("GVY", colorBold+colorGreen), 62))
	r.line(r.frame("Multithreaded data validation framework", 62))
	r.line(r.paint("+"+border+"+", colorGray))
}

func (r *renderer) section(title string) {
	r.line("")
	r.line(r.paint(title, colorBold+colorGreen))
}

func (r *renderer) commandBlock(label, command string) {
	r.linef("  %s", label)
	r.linef("    %s", r.command(command))
}

func (r *renderer) commandLine(command string) {
	r.linef("  %s", r.command(command))
}

func (r *renderer) mode(name string, lines ...string) {
	r.linef("  %s", r.command(name))
	for _, line := range lines {
		r.linef("    %s", r.decorate(line))
	}
	r.line("")
}

func (r *renderer) flagGroup(name string, rows ...[2]string) {
	r.linef("  %s", r.paint(name, colorBold+colorGreen))
	for _, row := range rows {
		r.linef("    %s %s", r.padRight(r.command(row[0]), 27), row[1])
	}
	r.line("")
}

func (r *renderer) endpoint(method, path string) {
	r.linef("    %s %s", r.padRight(r.paint(method, colorCyan), 4), r.command(path))
}

func (r *renderer) env(name, desc string) {
	r.linef("  %s %s", r.padRight(r.command(name), 18), desc)
}

func (r *renderer) frame(content string, width int) string {
	plainLen := visibleLen(content)
	if plainLen > width-1 {
		return r.paint("|", colorGray) + " " + content + " " + r.paint("|", colorGray)
	}
	padding := width - plainLen - 1
	return r.paint("|", colorGray) + " " + content + strings.Repeat(" ", padding) + r.paint("|", colorGray)
}

func (r *renderer) command(s string) string {
	return r.paint(s, colorBold+colorCyan)
}

func (r *renderer) decorate(s string) string {
	out := s
	for _, token := range []string{"http://127.0.0.1:1818/", "/api/config/defaults", "/api/config/resolve", "/api/runs/config"} {
		out = strings.ReplaceAll(out, token, r.command(token))
	}
	return out
}

func (r *renderer) paint(s, color string) string {
	if !r.color || s == "" {
		return s
	}
	return color + s + colorReset
}

func (r *renderer) padRight(s string, width int) string {
	padding := width - visibleLen(s)
	if padding <= 0 {
		return s
	}
	return s + strings.Repeat(" ", padding)
}

func (r *renderer) line(s string) {
	if r.err != nil {
		return
	}
	_, r.err = fmt.Fprintln(r.w, s)
}

func (r *renderer) linef(format string, args ...interface{}) {
	if r.err != nil {
		return
	}
	_, r.err = fmt.Fprintf(r.w, format+"\n", args...)
}

func (r *renderer) renderErr() error {
	return r.err
}

func isColorEnabled(opts Options) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if force, err := strconv.ParseBool(os.Getenv("GVY_COLOR")); err == nil {
		return force
	}
	if opts.Color {
		return true
	}
	term := strings.ToLower(strings.TrimSpace(os.Getenv("TERM")))
	return term != "" && term != "dumb"
}

func visibleLen(s string) int {
	length := 0
	inEscape := false
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\033':
			inEscape = true
		case inEscape && s[i] == 'm':
			inEscape = false
		case !inEscape:
			length++
		}
	}
	return length
}
