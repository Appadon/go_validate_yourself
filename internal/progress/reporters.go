package progress

/* MultiReporter fan-outs one event to multiple downstream reporters. */
type MultiReporter struct {
	reporters []Reporter
}

/* Combine returns one reporter that forwards events to each non-nil reporter. */
func Combine(reporters ...Reporter) Reporter {
	filtered := make([]Reporter, 0, len(reporters))
	for _, reporter := range reporters {
		if reporter != nil {
			filtered = append(filtered, reporter)
		}
	}
	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		return MultiReporter{reporters: filtered}
	}
}

/* Report forwards the event to every configured reporter. */
func (m MultiReporter) Report(event Event) {
	for _, reporter := range m.reporters {
		reporter.Report(event)
	}
}
