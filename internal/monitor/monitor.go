package monitor

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

/* DiskSnapshot captures capacity for the filesystem containing Path. */
type DiskSnapshot struct {
	Path           string  `json:"path,omitempty"`
	TotalBytes     uint64  `json:"total_bytes"`
	FreeBytes      uint64  `json:"free_bytes"`
	AvailableBytes uint64  `json:"available_bytes"`
	UsedBytes      uint64  `json:"used_bytes"`
	UsedPercent    float64 `json:"used_percent"`
}

/* MemorySnapshot captures process memory from Go plus RSS when available. */
type MemorySnapshot struct {
	AllocBytes uint64 `json:"alloc_bytes"`
	SysBytes   uint64 `json:"sys_bytes"`
	RSSBytes   uint64 `json:"rss_bytes,omitempty"`
}

/* ResourceSnapshot is one point-in-time process and disk sample. */
type ResourceSnapshot struct {
	Time       time.Time      `json:"time"`
	CPUPercent *float64       `json:"cpu_percent,omitempty"`
	Memory     MemorySnapshot `json:"memory"`
	Disk       DiskSnapshot   `json:"disk"`
}

/* DiskEstimate describes pre-run storage inputs and a rough peak estimate. */
type DiskEstimate struct {
	InputFileBytes      int64               `json:"input_file_bytes"`
	AvailableBytes      uint64              `json:"available_bytes"`
	FreeBytes           uint64              `json:"free_bytes"`
	EstimatedRunBytes   int64               `json:"estimated_run_bytes"`
	EstimatedPeakBytes  int64               `json:"estimated_peak_bytes"`
	EstimatedComponents []EstimateComponent `json:"estimated_components,omitempty"`
	Warnings            []string            `json:"warnings,omitempty"`
}

/* EstimateComponent explains one contributor to the rough disk estimate. */
type EstimateComponent struct {
	Name  string  `json:"name"`
	Bytes int64   `json:"bytes"`
	Ratio float64 `json:"ratio,omitempty"`
}

/* Sampler retains previous CPU counters so subsequent samples can report percent. */
type Sampler struct {
	DiskPath       string
	previousProc   uint64
	previousSystem uint64
}

/* NewSampler creates a process sampler for the filesystem containing diskPath. */
func NewSampler(diskPath string) *Sampler {
	if strings.TrimSpace(diskPath) == "" {
		diskPath = "."
	}
	return &Sampler{DiskPath: diskPath}
}

/* Sample returns current process and disk usage. */
func (s *Sampler) Sample() (ResourceSnapshot, error) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	disk, err := StatDisk(s.DiskPath)
	if err != nil {
		return ResourceSnapshot{}, err
	}

	sample := ResourceSnapshot{
		Time: time.Now().UTC(),
		Memory: MemorySnapshot{
			AllocBytes: mem.Alloc,
			SysBytes:   mem.Sys,
			RSSBytes:   readRSSBytes(),
		},
		Disk: disk,
	}

	procTicks, procOK := readProcessTicks()
	systemTicks, systemOK := readSystemTicks()
	if procOK && systemOK && s.previousProc > 0 && s.previousSystem > 0 && procTicks >= s.previousProc && systemTicks > s.previousSystem {
		percent := normalizedCPUPercent(procTicks-s.previousProc, systemTicks-s.previousSystem)
		sample.CPUPercent = &percent
	}
	if procOK && systemOK {
		s.previousProc = procTicks
		s.previousSystem = systemTicks
	}
	return sample, nil
}

/* StatDisk returns capacity for path, walking up to an existing parent if needed. */
func StatDisk(path string) (DiskSnapshot, error) {
	clean := strings.TrimSpace(path)
	if clean == "" {
		clean = "."
	}
	statPath := clean
	for {
		if _, err := os.Stat(statPath); err == nil {
			break
		}
		parent := strings.TrimSpace(filepath.Dir(statPath))
		if parent == "" || parent == statPath {
			statPath = "."
			break
		}
		statPath = parent
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(statPath, &stat); err != nil {
		return DiskSnapshot{}, fmt.Errorf("stat filesystem %q: %w", statPath, err)
	}
	blockSize := uint64(stat.Bsize)
	total := stat.Blocks * blockSize
	free := stat.Bfree * blockSize
	available := stat.Bavail * blockSize
	used := total - free
	usedPercent := 0.0
	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}
	return DiskSnapshot{
		Path:           statPath,
		TotalBytes:     total,
		FreeBytes:      free,
		AvailableBytes: available,
		UsedBytes:      used,
		UsedPercent:    usedPercent,
	}, nil
}

func readRSSBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

func readProcessTicks() (uint64, bool) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}
	text := string(data)
	end := strings.LastIndex(text, ")")
	if end < 0 || end+2 >= len(text) {
		return 0, false
	}
	fields := strings.Fields(text[end+2:])
	if len(fields) <= 12 {
		return 0, false
	}
	userTicks, err1 := strconv.ParseUint(fields[11], 10, 64)
	systemTicks, err2 := strconv.ParseUint(fields[12], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	return userTicks + systemTicks, true
}

func readSystemTicks() (uint64, bool) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0, false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return 0, false
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 2 || fields[0] != "cpu" {
		return 0, false
	}
	var total uint64
	for _, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return 0, false
		}
		total += value
	}
	return total, true
}

func normalizedCPUPercent(processDelta, systemDelta uint64) float64 {
	if systemDelta == 0 {
		return 0
	}
	percent := float64(processDelta) / float64(systemDelta) * 100
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}
