package console

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	colorReset  = "\033[0m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
	colorGray   = "\033[90m"
)

/* BannerItem stores one key/value setting to display in a run banner. */
type BannerItem struct {
	Key   string
	Value string
}

/* ProgressSnapshot represents a standardized progress status line. */
type ProgressSnapshot struct {
	Segments []string
}

/* Infof prints an informational log line. */
func Infof(format string, args ...interface{}) {
	logf(os.Stderr, "INFO", colorBlue, format, args...)
}

/* Successf prints a success log line. */
func Successf(format string, args ...interface{}) {
	logf(os.Stderr, "SUCCESS", colorGreen, format, args...)
}

/* Warnf prints a warning log line. */
func Warnf(format string, args ...interface{}) {
	logf(os.Stderr, "WARN", colorYellow, format, args...)
}

/* Errorf prints an error log line. */
func Errorf(format string, args ...interface{}) {
	logf(os.Stderr, "ERROR", colorRed, format, args...)
}

/* GreenValue returns a green-colored value when color output is enabled. */
func GreenValue(v string) string {
	return paint(v, colorGreen)
}

/* Progressf prints a standardized progress log line for long-running phases. */
func Progressf(p ProgressSnapshot) {
	msg := buildProgressMessage(p.Segments)
	logf(os.Stderr, "RUNNING", colorCyan, "%s", msg)
}

/* PrintBanner renders the current run configuration in a simple boxed block. */
func PrintBanner(title string, items []BannerItem) {
	width := computeBannerWidth(title, items)
	border := strings.Repeat("=", width)
	fmt.Fprintln(os.Stderr, paint(border, colorGray))
	fmt.Fprintln(os.Stderr, paint(center(" "+title+" ", width), colorCyan))
	fmt.Fprintln(os.Stderr, paint(border, colorGray))
	for _, item := range items {
		key := strings.TrimSpace(item.Key)
		val := strings.TrimSpace(item.Value)
		if key == "" {
			continue
		}
		fmt.Fprintf(os.Stderr, "%-24s %s\n", key+":", val)
	}
	fmt.Fprintln(os.Stderr, paint(border, colorGray))
}

/* FormatDuration returns a compact duration string like 5s, 2m04s, or 1h03m22s. */
func FormatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	seconds := int(d.Seconds())
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
	if hours > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", hours, minutes, secs)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%02ds", minutes, secs)
	}
	return fmt.Sprintf("%ds", secs)
}

/* FormatBytes returns a human-readable byte count. */
func FormatBytes(v int64) string {
	const (
		ki = 1024
		mi = 1024 * ki
		gi = 1024 * mi
	)
	switch {
	case v >= gi:
		return fmt.Sprintf("%.2f GiB", float64(v)/gi)
	case v >= mi:
		return fmt.Sprintf("%.2f MiB", float64(v)/mi)
	case v >= ki:
		return fmt.Sprintf("%.2f KiB", float64(v)/ki)
	default:
		return fmt.Sprintf("%d B", v)
	}
}

/* logf writes one formatted log line with timestamp and level label. */
func logf(w *os.File, level, color, format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05")
	levelLabel := paint(level, color)
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(w, "[%s] %-8s %s\n", timestamp, levelLabel, msg)
}

/* paint wraps output in ANSI color codes when color output is enabled. */
func paint(s, color string) string {
	if !isColorEnabled() {
		return s
	}
	return color + s + colorReset
}

/* isColorEnabled reports whether ANSI color output should be used. */
func isColorEnabled() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	term := strings.ToLower(strings.TrimSpace(os.Getenv("TERM")))
	return term != "" && term != "dumb"
}

/* center pads a string with spaces so it is centered at the given width. */
func center(s string, width int) string {
	if len(s) >= width {
		return s
	}
	space := width - len(s)
	left := space / 2
	right := space - left
	return strings.Repeat(" ", left) + s + strings.Repeat(" ", right)
}

/* max returns the larger integer value. */
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

/* computeBannerWidth calculates a minimum banner width for title and key-values. */
func computeBannerWidth(title string, items []BannerItem) int {
	width := max(76, len(title)+8)
	for _, item := range items {
		key := strings.TrimSpace(item.Key)
		val := strings.TrimSpace(item.Value)
		if key == "" {
			continue
		}
		lineLen := len(fmt.Sprintf("%-24s %s", key+":", val))
		width = max(width, lineLen+2)
	}
	return width
}

/* buildProgressMessage renders normalized progress segments into one line. */
func buildProgressMessage(segments []string) string {
	out := make([]string, 0, len(segments))
	for _, segment := range segments {
		s := strings.TrimSpace(segment)
		if s == "" {
			continue
		}
		out = append(out, formatSegmentWithColors(s))
	}
	return strings.Join(out, " ")
}

/* formatSegmentWithColors applies segment-level color formatting for progress logs. */
func formatSegmentWithColors(segment string) string {
	if !isColorEnabled() {
		return "[" + segment + "]"
	}
	content := segment
	if isPercentageSegment(segment) {
		content = paint(segment, colorGreen)
	} else if label, value, ok := splitLabelValueSegment(segment); ok {
		content = label + " " + paint(value, colorCyan)
	}
	return paint("[", colorBlue) + content + paint("]", colorBlue)
}

/* isPercentageSegment reports whether a segment is a numeric percentage token. */
func isPercentageSegment(segment string) bool {
	value := strings.TrimSpace(segment)
	if !strings.HasSuffix(value, "%") {
		return false
	}
	numberPart := strings.TrimSpace(strings.TrimSuffix(value, "%"))
	if numberPart == "" {
		return false
	}
	_, err := strconv.ParseFloat(numberPart, 64)
	return err == nil
}

/* splitLabelValueSegment parses supported label/value progress segments. */
func splitLabelValueSegment(segment string) (string, string, bool) {
	value := strings.TrimSpace(segment)
	if strings.HasPrefix(value, "eta ") {
		return "eta", strings.TrimSpace(strings.TrimPrefix(value, "eta ")), true
	}
	if strings.HasPrefix(value, "elapsed ") {
		return "elapsed", strings.TrimSpace(strings.TrimPrefix(value, "elapsed ")), true
	}
	return "", "", false
}
