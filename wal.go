package pulse

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultWALSegmentSize int64 = 64 * 1024 * 1024
	defaultWALMaxTotal    int64 = 512 * 1024 * 1024
	defaultWALSyncEvery         = 100
	maxWALRecordSize      int64 = 8 * 1024 * 1024
)

var (
	ErrWALCapacityExceeded = errors.New("pulse: wal capacity exceeded")
)

// WALConfig controls write-ahead log persistence.
type WALConfig struct {
	Dir          string
	SegmentSize  int64
	MaxTotalSize int64
	SyncEvery    int
	SyncInterval time.Duration
	StrictSync   bool
}

type WALReplayStats struct {
	Segments       int
	Records        int
	CorruptRecords int
	TruncatedTail  bool
}

type WALReplayedSample struct {
	Sample  Sample
	Segment int
}

// WAL persists records to disk in segmented files.
type WAL struct {
	mu sync.Mutex

	cfg WALConfig

	currentSegment int
	currentFile    *os.File
	currentSize    int64

	pendingSinceSync int
	lastSync         time.Time

	acknowledgedThrough int
}

func NewWAL(cfg WALConfig) (*WAL, error) {
	resolved := cfg
	if resolved.Dir == "" {
		return nil, fmt.Errorf("pulse: wal dir is required")
	}
	if resolved.SegmentSize <= 0 {
		resolved.SegmentSize = defaultWALSegmentSize
	}
	if resolved.MaxTotalSize <= 0 {
		resolved.MaxTotalSize = defaultWALMaxTotal
	}
	if resolved.SyncEvery <= 0 {
		resolved.SyncEvery = defaultWALSyncEvery
	}
	if resolved.SyncInterval <= 0 {
		resolved.SyncInterval = time.Second
	}

	if err := os.MkdirAll(resolved.Dir, 0o755); err != nil {
		return nil, err
	}

	segments, err := listSegments(resolved.Dir)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		cfg:                 resolved,
		acknowledgedThrough: -1,
		lastSync:            time.Now(),
	}
	if len(segments) == 0 {
		w.currentSegment = 0
	} else {
		w.currentSegment = segmentIndex(segments[len(segments)-1])
	}

	if err := w.openCurrentForAppend(); err != nil {
		return nil, err
	}
	if err := w.ensureCapacity(); err != nil {
		_ = w.currentFile.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentFile == nil {
		return nil
	}
	if err := w.currentFile.Sync(); err != nil {
		_ = w.currentFile.Close()
		w.currentFile = nil
		return err
	}
	err := w.currentFile.Close()
	w.currentFile = nil
	return err
}

func (w *WAL) WriteSample(sample Sample) error {
	_, err := w.WriteSampleWithSegment(sample)
	return err
}

func (w *WAL) WriteSampleWithSegment(sample Sample) (int, error) {
	payload, err := json.Marshal(sample)
	if err != nil {
		return -1, err
	}
	return w.WriteRecordWithSegment(payload)
}

func (w *WAL) WriteRecord(payload []byte) error {
	_, err := w.WriteRecordWithSegment(payload)
	return err
}

func (w *WAL) WriteRecordWithSegment(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFile == nil {
		return -1, fmt.Errorf("pulse: wal is closed")
	}
	if int64(len(payload)) > maxWALRecordSize {
		return -1, fmt.Errorf("pulse: wal record too large: %d", len(payload))
	}

	recordLen := int64(8 + len(payload))
	if w.currentSize+recordLen > w.cfg.SegmentSize {
		if err := w.rotate(); err != nil {
			return -1, err
		}
	}
	segment := w.currentSegment

	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	if _, err := w.currentFile.Write(header); err != nil {
		return -1, err
	}
	if _, err := w.currentFile.Write(payload); err != nil {
		return -1, err
	}

	w.currentSize += recordLen
	w.pendingSinceSync++

	if w.cfg.StrictSync || w.pendingSinceSync >= w.cfg.SyncEvery || time.Since(w.lastSync) >= w.cfg.SyncInterval {
		if err := w.currentFile.Sync(); err != nil {
			return -1, err
		}
		w.pendingSinceSync = 0
		w.lastSync = time.Now()
	}

	if err := w.ensureCapacity(); err != nil {
		return -1, err
	}
	return segment, nil
}

// AcknowledgeThrough marks segments up to index as export-confirmed.
func (w *WAL) AcknowledgeThrough(segment int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if segment > w.acknowledgedThrough {
		w.acknowledgedThrough = segment
		w.compactAcknowledgedLocked()
	}
}

func (w *WAL) compactAcknowledgedLocked() {
	segments, err := listSegments(w.cfg.Dir)
	if err != nil {
		return
	}
	for _, seg := range segments {
		idx := segmentIndex(seg)
		if idx < 0 || idx > w.acknowledgedThrough || idx == w.currentSegment {
			continue
		}
		_ = os.Remove(filepath.Join(w.cfg.Dir, seg))
	}
}

func (w *WAL) ReplayRecords() ([][]byte, WALReplayStats, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := listSegments(w.cfg.Dir)
	if err != nil {
		return nil, WALReplayStats{}, err
	}

	stats := WALReplayStats{Segments: len(segments)}
	out := make([][]byte, 0)

	for i, seg := range segments {
		isLast := i == len(segments)-1
		fp := filepath.Join(w.cfg.Dir, seg)
		records, segStats, err := replaySegment(fp, isLast)
		if err != nil {
			return nil, stats, err
		}
		stats.Records += segStats.Records
		stats.CorruptRecords += segStats.CorruptRecords
		if segStats.TruncatedTail {
			stats.TruncatedTail = true
		}
		out = append(out, records...)
	}

	return out, stats, nil
}

func (w *WAL) ReplaySamples() ([]Sample, WALReplayStats, error) {
	items, stats, err := w.ReplaySamplesWithSegments()
	if err != nil {
		return nil, stats, err
	}
	out := make([]Sample, 0, len(items))
	for _, item := range items {
		out = append(out, item.Sample)
	}
	return out, stats, nil
}

func (w *WAL) ReplaySamplesWithSegments() ([]WALReplayedSample, WALReplayStats, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := listSegments(w.cfg.Dir)
	if err != nil {
		return nil, WALReplayStats{}, err
	}

	stats := WALReplayStats{Segments: len(segments)}
	out := make([]WALReplayedSample, 0)

	for i, seg := range segments {
		isLast := i == len(segments)-1
		segIdx := segmentIndex(seg)
		fp := filepath.Join(w.cfg.Dir, seg)
		records, segStats, err := replaySegment(fp, isLast)
		if err != nil {
			return nil, stats, err
		}
		stats.Records += segStats.Records
		stats.CorruptRecords += segStats.CorruptRecords
		if segStats.TruncatedTail {
			stats.TruncatedTail = true
		}

		for _, rec := range records {
			var s Sample
			if err := json.Unmarshal(rec, &s); err != nil {
				stats.CorruptRecords++
				continue
			}
			out = append(out, WALReplayedSample{Sample: s, Segment: segIdx})
		}
	}

	return out, stats, nil
}

func (w *WAL) openCurrentForAppend() error {
	name := filepath.Join(w.cfg.Dir, segmentFileName(w.currentSegment))
	fp, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	off, err := fp.Seek(0, io.SeekEnd)
	if err != nil {
		_ = fp.Close()
		return err
	}
	w.currentFile = fp
	w.currentSize = off
	return nil
}

func (w *WAL) rotate() error {
	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			return err
		}
		if err := w.currentFile.Close(); err != nil {
			return err
		}
	}
	w.currentSegment++
	w.currentSize = 0
	w.pendingSinceSync = 0
	return w.openCurrentForAppend()
}

func (w *WAL) ensureCapacity() error {
	segments, err := listSegments(w.cfg.Dir)
	if err != nil {
		return err
	}

	total := int64(0)
	for _, seg := range segments {
		info, err := os.Stat(filepath.Join(w.cfg.Dir, seg))
		if err != nil {
			return err
		}
		total += info.Size()
	}
	if total <= w.cfg.MaxTotalSize {
		return nil
	}

	for _, seg := range segments {
		idx := segmentIndex(seg)
		if idx > w.acknowledgedThrough {
			continue
		}
		if idx == w.currentSegment {
			continue
		}
		path := filepath.Join(w.cfg.Dir, seg)
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		total -= info.Size()
		if total <= w.cfg.MaxTotalSize {
			return nil
		}
	}

	if total > w.cfg.MaxTotalSize {
		return ErrWALCapacityExceeded
	}
	return nil
}

type segmentReplayStats struct {
	Records        int
	CorruptRecords int
	TruncatedTail  bool
}

func replaySegment(path string, isLast bool) ([][]byte, segmentReplayStats, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, segmentReplayStats{}, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	out := make([][]byte, 0)
	stats := segmentReplayStats{}
	var offset int64
	var lastGood int64

	for {
		header := make([]byte, 8)
		n, err := io.ReadFull(r, header)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if isLast {
				if truncErr := f.Truncate(lastGood); truncErr != nil {
					return nil, stats, truncErr
				}
				stats.TruncatedTail = true
			}
			break
		}
		if err != nil {
			return nil, stats, err
		}
		offset += int64(n)

		length := int64(binary.LittleEndian.Uint32(header[0:4]))
		expectedCRC := binary.LittleEndian.Uint32(header[4:8])
		if length <= 0 || length > maxWALRecordSize {
			stats.CorruptRecords++
			if isLast {
				if truncErr := f.Truncate(lastGood); truncErr != nil {
					return nil, stats, truncErr
				}
				stats.TruncatedTail = true
			}
			break
		}

		payload := make([]byte, length)
		n, err = io.ReadFull(r, payload)
		if err == io.ErrUnexpectedEOF {
			if isLast {
				if truncErr := f.Truncate(lastGood); truncErr != nil {
					return nil, stats, truncErr
				}
				stats.TruncatedTail = true
			}
			break
		}
		if err != nil {
			return nil, stats, err
		}
		offset += int64(n)

		if crc32.ChecksumIEEE(payload) != expectedCRC {
			stats.CorruptRecords++
			lastGood = offset
			continue
		}

		out = append(out, payload)
		stats.Records++
		lastGood = offset
	}

	return out, stats, nil
}

func listSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	segments := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			segments = append(segments, name)
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segmentIndex(segments[i]) < segmentIndex(segments[j])
	})
	return segments, nil
}

func segmentFileName(index int) string {
	return fmt.Sprintf("wal-%06d.log", index)
}

func segmentIndex(name string) int {
	base := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log")
	n, err := strconv.Atoi(base)
	if err != nil {
		return -1
	}
	return n
}
