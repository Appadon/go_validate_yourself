package banner

import (
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	colorReset = "\033[0m"
	colorBold  = "\033[1m"
	colorCyan  = "\033[36m"
	colorGreen = "\033[32m"
	colorGray  = "\033[90m"
)

var startupMessages = []string{
	"preparing workspace",
	"definitely not stealing your data",
	"rebuilding AI from CSV headers",
	"negotiating with parquet",
	"waking multithreaded validators",
	"polishing the web console",
	"asking Excel to behave",
}

/* Banner stores one startup banner message and its ASCII art. */
type Banner struct {
	Message  string
	Subtitle string
	Art      string
	HelpHint string
}

const defaultMessage = "Welcome, and please..."
const defaultSubtitle = "Multithreaded data validation framework"
const defaultHelpHint = "Run with -h or -help to see options (example: ./gvy -h)."

const defaultArt = `  _________    _   _____   __   _______  ___ __________  __  ______  __  _____  __________   ____
 / ___/ __ \  | | / / _ | / /  /  _/ _ \/ _ /_  __/ __/  \ \/ / __ \/ / / / _ \/ __/ __/ /  / __/
/ (_ / /_/ /  | |/ / __ |/ /___/ // // / __ |/ / / _/     \  / /_/ / /_/ / , _/\ \/ _// /__/ _/
\___/\____/   |___/_/ |_/____/___/____/_/ |_/_/ /___/     /_/\____/\____/_/|_/___/___/____/_/`

/* Default returns the standard GVY startup banner. */
func Default() Banner {
	return Banner{
		Message:  defaultMessage,
		Subtitle: defaultSubtitle,
		Art:      defaultArt,
		HelpHint: defaultHelpHint,
	}
}

/* Write renders the banner to w with a trailing blank line. */
func (b Banner) Write(w io.Writer) error {
	message := strings.TrimSpace(b.Message)
	subtitle := strings.TrimSpace(b.Subtitle)
	helpHint := strings.TrimSpace(b.HelpHint)
	art := strings.TrimRight(b.Art, "\n")
	lines := strings.Split(art, "\n")
	width := bannerWidth(message, subtitle, helpHint, lines)
	border := strings.Repeat("-", width)

	if _, err := fmt.Fprintln(w, paint("+"+border+"+", colorGray)); err != nil {
		return err
	}
	if message != "" {
		if _, err := fmt.Fprintln(w, frameLine(paint(message, colorBold+colorGreen), width)); err != nil {
			return err
		}
	}
	if art != "" {
		for i, line := range lines {
			color := colorCyan
			if i == len(lines)-1 {
				color = colorGreen
			}
			if _, err := fmt.Fprintln(w, frameLine(paint(line, color), width)); err != nil {
				return err
			}
		}
	}
	if subtitle != "" {
		if _, err := fmt.Fprintln(w, frameLine("", width)); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, frameLine(paint(subtitle, colorGray), width)); err != nil {
			return err
		}
	}
	if helpHint != "" {
		if _, err := fmt.Fprintln(w, frameLine(renderHelpHint(helpHint), width)); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w, paint("+"+border+"+", colorGray)); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

/* PrintDefault renders the standard GVY startup banner to w. */
func PrintDefault(w io.Writer) error {
	return Default().Write(w)
}

/* PrintStartup clears the terminal and renders the standard banner. */
func PrintStartup(w io.Writer) error {
	if shouldClearScreen(w) {
		if _, err := fmt.Fprint(w, "\033[2J\033[H"); err != nil {
			return err
		}
	}
	return PrintDefault(w)
}

/* AnimateConsoleReady plays a short web-console startup animation. */
func AnimateConsoleReady(w io.Writer) error {
	if !shouldAnimate(w) {
		return nil
	}
	return animateStartup(w)
}

/* PrintConsoleURL renders a prominent web-console URL callout. */
func PrintConsoleURL(w io.Writer, rawURL string) error {
	label := "Web console available at:"
	cleanURL := strings.TrimSpace(rawURL)
	width := max(len(label), len(cleanURL)) + 4
	border := strings.Repeat("=", width)

	if _, err := fmt.Fprintln(w, paint("+"+border+"+", colorGray)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, frameLine(paint(label, colorBold+colorGreen), width)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, frameLine(paint(cleanURL, colorBold+colorCyan), width)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, paint("+"+border+"+", colorGray)); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

func bannerWidth(message, subtitle, helpHint string, lines []string) int {
	width := max(max(len(message), len(subtitle)), len(helpHint))
	for _, line := range lines {
		width = max(width, len(line))
	}
	return width + 4
}

func frameLine(content string, width int) string {
	plainLen := visibleLen(content)
	if plainLen > width {
		return paint("|", colorGray) + " " + content + " " + paint("|", colorGray)
	}
	padding := width - plainLen
	return paint("|", colorGray) + " " + content + strings.Repeat(" ", padding+1) + paint("|", colorGray)
}

func renderHelpHint(helpHint string) string {
	if helpHint != defaultHelpHint {
		return paint(helpHint, colorGray)
	}
	return paint("Run with ", colorGray) +
		paint("-h", colorBold+colorGray) +
		paint(" or ", colorGray) +
		paint("-help", colorBold+colorGray) +
		paint(" to see options (example: ", colorGray) +
		paint("./gvy -h", colorBold+colorGray) +
		paint(").", colorGray)
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

func paint(s, color string) string {
	if !isColorEnabled() || s == "" {
		return s
	}
	return color + s + colorReset
}

func animateStartup(w io.Writer) error {
	messages := selectStartupMessages(startupMessages, 2)
	spinner := []string{"-", "\\", "|", "/"}
	const ticksPerMessage = 6
	const tickDelay = 100 * time.Millisecond

	for _, message := range messages {
		for tick := 0; tick < ticksPerMessage; tick++ {
			frame := fmt.Sprintf("[%s] %s", spinner[tick%len(spinner)], message)
			if _, err := fmt.Fprintf(w, "\r%s", paint(frame, colorCyan)); err != nil {
				return err
			}
			time.Sleep(tickDelay)
		}
	}
	_, err := fmt.Fprint(w, "\r\033[2K")
	return err
}

func selectStartupMessages(messages []string, count int) []string {
	if count <= 0 || len(messages) == 0 {
		return nil
	}
	available := append([]string{}, messages...)
	if count > len(available) {
		count = len(available)
	}
	selected := make([]string, 0, count)
	for len(selected) < count {
		idx := randomIndex(len(available))
		selected = append(selected, available[idx])
		available = append(available[:idx], available[idx+1:]...)
	}
	return selected
}

func randomIndex(max int) int {
	if max <= 1 {
		return 0
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return int(time.Now().UnixNano() % int64(max))
	}
	return int(value.Int64())
}

func isColorEnabled() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if force, err := strconv.ParseBool(os.Getenv("GVY_COLOR")); err == nil {
		return force
	}
	term := strings.ToLower(strings.TrimSpace(os.Getenv("TERM")))
	return term != "" && term != "dumb"
}

func shouldClearScreen(w io.Writer) bool {
	if force, err := strconv.ParseBool(os.Getenv("GVY_CLEAR")); err == nil {
		return force
	}
	return isTerminalWriter(w)
}

func shouldAnimate(w io.Writer) bool {
	if force, err := strconv.ParseBool(os.Getenv("GVY_ANIMATE")); err == nil {
		return force
	}
	return isTerminalWriter(w)
}

func isTerminalWriter(w io.Writer) bool {
	file, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
