//go:build linux

package pulse

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type hardwareCollector struct {
	mu sync.Mutex

	prevIdle  uint64
	prevTotal uint64
	havePrev  bool
}

func newHardwareCollector() *hardwareCollector {
	return &hardwareCollector{}
}

func (h *hardwareCollector) Collect() map[string]float64 {
	out := make(map[string]float64)

	cpuPercent := h.readCPUPercent()
	if cpuPercent >= 0 {
		out["hw.cpu_percent"] = cpuPercent
	}

	totalMem, availMem, okMem := readMemInfo()
	if okMem {
		out["hw.mem_total_bytes"] = float64(totalMem)
		out["hw.mem_available_bytes"] = float64(availMem)
		if totalMem > 0 {
			used := totalMem - availMem
			out["hw.mem_used_percent"] = (float64(used) / float64(totalMem)) * 100.0
		}
	}

	totalDisk, usedDisk, okDisk := readDiskUsage("/")
	if okDisk {
		out["hw.disk_total_bytes"] = float64(totalDisk)
		out["hw.disk_used_bytes"] = float64(usedDisk)
		if totalDisk > 0 {
			out["hw.disk_used_percent"] = (float64(usedDisk) / float64(totalDisk)) * 100.0
		}
	}

	l1, l5, l15, okLoad := readLoadAvg()
	if okLoad {
		out["hw.load1"] = l1
		out["hw.load5"] = l5
		out["hw.load15"] = l15
	}

	return out
}

func (h *hardwareCollector) readCPUPercent() float64 {
	idle, total, ok := readProcStatCPU()
	if !ok {
		return -1
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.havePrev {
		h.prevIdle = idle
		h.prevTotal = total
		h.havePrev = true
		return 0
	}

	deltaIdle := idle - h.prevIdle
	deltaTotal := total - h.prevTotal
	h.prevIdle = idle
	h.prevTotal = total

	if deltaTotal == 0 {
		return 0
	}
	usage := (1.0 - float64(deltaIdle)/float64(deltaTotal)) * 100.0
	if usage < 0 {
		return 0
	}
	if usage > 100 {
		return 100
	}
	return usage
}

func readProcStatCPU() (idle uint64, total uint64, ok bool) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	if !s.Scan() {
		return 0, 0, false
	}
	line := s.Text()
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0, false
	}

	vals := make([]uint64, 0, len(fields)-1)
	for _, f := range fields[1:] {
		v, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		vals = append(vals, v)
	}

	var sum uint64
	for _, v := range vals {
		sum += v
	}
	idleVal := vals[3]
	if len(vals) > 4 {
		idleVal += vals[4] // include iowait
	}
	return idleVal, sum, true
}

func readMemInfo() (total uint64, available uint64, ok bool) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			v, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				total = v * 1024
			}
		case "MemAvailable:":
			v, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				available = v * 1024
			}
		}
	}
	if total == 0 {
		return 0, 0, false
	}
	if available > total {
		available = total
	}
	return total, available, true
}

func readDiskUsage(path string) (total uint64, used uint64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, false
	}
	totalBytes := st.Blocks * uint64(st.Bsize)
	usedBytes := (st.Blocks - st.Bfree) * uint64(st.Bsize)
	return totalBytes, usedBytes, true
}

func readLoadAvg() (l1 float64, l5 float64, l15 float64, ok bool) {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0, false
	}
	fields := strings.Fields(string(b))
	if len(fields) < 3 {
		return 0, 0, 0, false
	}
	v1, err1 := strconv.ParseFloat(fields[0], 64)
	v5, err2 := strconv.ParseFloat(fields[1], 64)
	v15, err3 := strconv.ParseFloat(fields[2], 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, 0, 0, false
	}
	return v1, v5, v15, true
}
