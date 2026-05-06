package monitor

import "testing"

func TestNormalizedCPUPercentUsesTotalMachineCapacity(t *testing.T) {
	if got := normalizedCPUPercent(100, 1000); got != 10 {
		t.Fatalf("normalizedCPUPercent() = %v, want 10", got)
	}
}

func TestNormalizedCPUPercentCapsImpossibleSamples(t *testing.T) {
	if got := normalizedCPUPercent(1200, 1000); got != 100 {
		t.Fatalf("normalizedCPUPercent() = %v, want 100", got)
	}
}

func TestNormalizedCPUPercentHandlesZeroSystemDelta(t *testing.T) {
	if got := normalizedCPUPercent(100, 0); got != 0 {
		t.Fatalf("normalizedCPUPercent() = %v, want 0", got)
	}
}
