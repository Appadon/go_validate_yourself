package banner

import (
	"strings"
	"testing"
)

func TestDefaultBannerRendersMessageAndArt(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	var out strings.Builder
	if err := PrintDefault(&out); err != nil {
		t.Fatalf("PrintDefault() error = %v", err)
	}

	rendered := out.String()
	if !strings.Contains(rendered, "Welcome, and please...") {
		t.Fatalf("rendered banner missing welcome message: %q", rendered)
	}
	if !strings.Contains(rendered, "Multithreaded data validation framework") {
		t.Fatalf("rendered banner missing subtitle: %q", rendered)
	}
	if !strings.Contains(rendered, "Run with -h or -help to see options (example: ./gvy -h).") {
		t.Fatalf("rendered banner missing help hint: %q", rendered)
	}
	if !strings.Contains(rendered, `\___/\____/`) {
		t.Fatalf("rendered banner missing ASCII art: %q", rendered)
	}
	if !strings.HasSuffix(rendered, "\n\n") {
		t.Fatalf("rendered banner should end with a blank line: %q", rendered)
	}
}

func TestDefaultBannerBoldsHelpFlagsWhenColorEnabled(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("GVY_COLOR", "true")

	var out strings.Builder
	if err := PrintDefault(&out); err != nil {
		t.Fatalf("PrintDefault() error = %v", err)
	}

	rendered := out.String()
	for _, want := range []string{
		colorBold + colorGray + "-h" + colorReset,
		colorBold + colorGray + "-help" + colorReset,
		colorBold + colorGray + "./gvy -h" + colorReset,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered banner missing bold hint segment %q: %q", want, rendered)
		}
	}
}

func TestPrintConsoleURLRendersProminentCallout(t *testing.T) {
	var out strings.Builder
	if err := PrintConsoleURL(&out, "http://127.0.0.1:1818/"); err != nil {
		t.Fatalf("PrintConsoleURL() error = %v", err)
	}

	rendered := out.String()
	if !strings.Contains(rendered, "Web console available at:") {
		t.Fatalf("rendered URL callout missing label: %q", rendered)
	}
	if !strings.Contains(rendered, "http://127.0.0.1:1818/") {
		t.Fatalf("rendered URL callout missing URL: %q", rendered)
	}
}

func TestAnimateConsoleReadySkipsNonTerminalWritersByDefault(t *testing.T) {
	var out strings.Builder
	if err := AnimateConsoleReady(&out); err != nil {
		t.Fatalf("AnimateConsoleReady() error = %v", err)
	}
	if out.String() != "" {
		t.Fatalf("AnimateConsoleReady() wrote to non-terminal writer: %q", out.String())
	}
}

func TestSelectStartupMessagesReturnsDistinctSubset(t *testing.T) {
	messages := []string{"one", "two", "three", "four"}

	selected := selectStartupMessages(messages, 2)

	if len(selected) != 2 {
		t.Fatalf("selected %d messages, want 2: %v", len(selected), selected)
	}
	seen := make(map[string]bool, len(selected))
	for _, message := range selected {
		if seen[message] {
			t.Fatalf("selected duplicate message %q from %v", message, selected)
		}
		seen[message] = true
		if !containsString(messages, message) {
			t.Fatalf("selected unknown message %q from %v", message, messages)
		}
	}
}

func TestSelectStartupMessagesCapsCountAtAvailableMessages(t *testing.T) {
	messages := []string{"one", "two"}

	selected := selectStartupMessages(messages, 5)

	if len(selected) != len(messages) {
		t.Fatalf("selected %d messages, want %d: %v", len(selected), len(messages), selected)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
