package pulse

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWALWriteReplayAndTailTruncate(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(WALConfig{Dir: dir, SegmentSize: 1024, MaxTotalSize: 1024 * 1024, SyncEvery: 1})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()

	s1 := Sample{Timestamp: time.Unix(1, 0), Values: map[string]float64{"a": 1}}
	s2 := Sample{Timestamp: time.Unix(2, 0), Values: map[string]float64{"a": 2}}
	if err := w.WriteSample(s1); err != nil {
		t.Fatalf("write s1: %v", err)
	}
	if err := w.WriteSample(s2); err != nil {
		t.Fatalf("write s2: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	segments, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("expected wal segments, err=%v len=%d", err, len(segments))
	}

	f, err := os.OpenFile(segments[len(segments)-1], os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		_ = f.Close()
		t.Fatalf("append garbage: %v", err)
	}
	_ = f.Close()

	w2, err := NewWAL(WALConfig{Dir: dir, SegmentSize: 1024, MaxTotalSize: 1024 * 1024, SyncEvery: 1})
	if err != nil {
		t.Fatalf("new wal 2: %v", err)
	}
	defer w2.Close()

	samples, stats, err := w2.ReplaySamples()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !stats.TruncatedTail {
		t.Fatalf("expected truncated tail true")
	}
	if len(samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(samples))
	}
}

func TestWALCorruptRecordSkipped(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(WALConfig{Dir: dir, SegmentSize: 1024, MaxTotalSize: 1024 * 1024, SyncEvery: 1})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()

	if err := w.WriteSample(Sample{Timestamp: time.Unix(1, 0), Values: map[string]float64{"x": 1}}); err != nil {
		t.Fatalf("write sample: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	segments, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	f, err := os.OpenFile(segments[0], os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open wal file: %v", err)
	}

	if _, err := f.Seek(4, 0); err != nil {
		_ = f.Close()
		t.Fatalf("seek: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 0}); err != nil {
		_ = f.Close()
		t.Fatalf("corrupt crc: %v", err)
	}
	_ = f.Close()

	w2, err := NewWAL(WALConfig{Dir: dir, SegmentSize: 1024, MaxTotalSize: 1024 * 1024, SyncEvery: 1})
	if err != nil {
		t.Fatalf("new wal 2: %v", err)
	}
	defer w2.Close()

	samples, stats, err := w2.ReplaySamples()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if stats.CorruptRecords == 0 {
		t.Fatalf("expected at least one corrupt record")
	}
	if len(samples) != 0 {
		t.Fatalf("expected 0 valid samples after corruption, got %d", len(samples))
	}
}

func TestWALReplaySamplesWithSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(WALConfig{Dir: dir, SegmentSize: 256, MaxTotalSize: 1024 * 1024, SyncEvery: 1})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()

	for i := 0; i < 6; i++ {
		if _, err := w.WriteSampleWithSegment(Sample{Timestamp: time.Now().UTC(), Values: map[string]float64{"v": float64(i)}}); err != nil {
			t.Fatalf("write sample %d: %v", i, err)
		}
	}

	items, stats, err := w.ReplaySamplesWithSegments()
	if err != nil {
		t.Fatalf("replay with segments: %v", err)
	}
	if stats.Records == 0 || len(items) == 0 {
		t.Fatalf("expected replayed records")
	}
	for _, item := range items {
		if item.Segment < 0 {
			t.Fatalf("expected non-negative segment, got %d", item.Segment)
		}
	}
}

func TestWALAcknowledgeCompactsOldSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(WALConfig{Dir: dir, SegmentSize: 128, MaxTotalSize: 1024 * 1024, SyncEvery: 1})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()

	maxSegment := -1
	for i := 0; i < 20; i++ {
		sample := Sample{
			Timestamp: time.Now().UTC(),
			Values: map[string]float64{
				"value": float64(i),
				"pad":   float64(i * 1000),
			},
		}
		seg, err := w.WriteSampleWithSegment(sample)
		if err != nil {
			t.Fatalf("write sample %d: %v", i, err)
		}
		if seg > maxSegment {
			maxSegment = seg
		}
	}

	if maxSegment < 1 {
		t.Fatalf("expected multiple segments, got max segment %d", maxSegment)
	}

	segmentsBefore, err := listSegments(dir)
	if err != nil {
		t.Fatalf("list segments before: %v", err)
	}
	if len(segmentsBefore) < 2 {
		t.Fatalf("expected at least 2 segments before compaction, got %d", len(segmentsBefore))
	}

	w.AcknowledgeThrough(maxSegment - 1)

	segmentsAfter, err := listSegments(dir)
	if err != nil {
		t.Fatalf("list segments after: %v", err)
	}
	for _, seg := range segmentsAfter {
		idx := segmentIndex(seg)
		if idx < maxSegment {
			t.Fatalf("expected old segments compacted, found %s", seg)
		}
	}
}
