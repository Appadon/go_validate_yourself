package help

import (
	"bytes"
	"strings"
	"testing"
)

/* TestPrintNoColorContainsGuideSections verifies the core help redesign content. */
func TestPrintNoColorContainsGuideSections(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	t.Setenv("GVY_COLOR", "")
	t.Setenv("TERM", "xterm-256color")

	var out bytes.Buffer
	if err := Print(&out, Options{BinaryName: "validatecsv"}); err != nil {
		t.Fatalf("Print() error = %v", err)
	}

	text := out.String()
	required := []string{
		"+--------------------------------------------------------------+",
		"| GVY",
		"Multithreaded data validation framework",
		"Web Console",
		"validatecsv",
		"opens http://127.0.0.1:1818/",
		"Start the web console:",
		"validatecsv main.csv schema.json",
		"validatecsv -mode validate input.csv -schema schema.json",
		"validatecsv -mode validate -dir split/ -schema schema.json",
		"validatecsv -config gvy.config.json",
		"Command Shapes",
		"validatecsv -mode server [-host 127.0.0.1] [-port 1818]",
		"Modes",
		"server",
		"auto",
		"validate",
		"split",
		"batch",
		"Config File",
		"CLI flags override config file values.",
		"pipeline.phases overrides mode.",
		`"main_csv": "main.csv"`,
		"HTTP API",
		"GET  /health",
		"GET  /api/config/defaults",
		"POST /api/config/resolve",
		"POST /api/runs/config",
		"POST /shutdown",
		"Environment Controls",
		"NO_COLOR=1",
		"GVY_COLOR=false",
		"GVY_COLOR=true",
		"GVY_CLEAR=false",
		"GVY_ANIMATE=false",
		"Examples",
	}
	assertContainsAll(t, text, required)

	if strings.Contains(text, "\033[") {
		t.Fatal("help output contains ANSI escape codes with NO_COLOR=1")
	}
	assertASCII(t, text)
}

/* TestPrintDocumentsAllCurrentFlags keeps the grouped flag list complete. */
func TestPrintDocumentsAllCurrentFlags(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	var out bytes.Buffer
	if err := Print(&out, Options{BinaryName: "gvy"}); err != nil {
		t.Fatalf("Print() error = %v", err)
	}

	flags := []string{
		"-h, -help",
		"-mode <mode>",
		"-config <path>",
		"-print-config",
		"-phases <list>",
		"-host <addr>",
		"-port <n>",
		"-t <n>",
		"-clear-validation-cache",
		"-schema <path>",
		"-dir <path>",
		"-write-empty-error",
		"-success-dir <path>",
		"-error-dir <path>",
		"-split-input <path>",
		"-split-output-dir <path>",
		"-split-primary-key <name>",
		"-split-max-open <n>",
		"-split-missing-file <name>",
		"-batch-dir <path>",
		"-batch-size <n>",
		"-batch-export-dir <path>",
	}
	assertContainsAll(t, out.String(), flags)
}

/* TestNoColorOverridesForcedColor verifies NO_COLOR remains authoritative. */
func TestNoColorOverridesForcedColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	t.Setenv("GVY_COLOR", "true")

	var out bytes.Buffer
	if err := Print(&out, Options{BinaryName: "gvy", Color: true}); err != nil {
		t.Fatalf("Print() error = %v", err)
	}
	if strings.Contains(out.String(), "\033[") {
		t.Fatal("help output contains ANSI escape codes when NO_COLOR is set")
	}
}

func assertContainsAll(t *testing.T, text string, needles []string) {
	t.Helper()
	for _, needle := range needles {
		if !strings.Contains(text, needle) {
			t.Fatalf("help output missing %q\n\n%s", needle, text)
		}
	}
}

func assertASCII(t *testing.T, text string) {
	t.Helper()
	for _, r := range text {
		if r == '\n' || r == '\t' || r == '\r' {
			continue
		}
		if r < 32 || r > 126 {
			t.Fatalf("help output contains non-ASCII rune %q", r)
		}
	}
}
