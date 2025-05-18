package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// Segment states
const (
	SegmentActive  = "active"  // Currently being written to
	SegmentSealed  = "sealed"  // Full but not checkpointed
	SegmentFlushed = "flushed" // Data has been checkpointed
)

type segment struct {
	id     int
	file   *os.File
	offset int64  // Current write offset
	state  string // Segment state
}

// WAL is a write ahead log for durably storing samples before they are written to the head block.
type WAL struct {
	mtx sync.Mutex

	// Active segment being written to
	current *segment

	// All segments by ID
	segments map[int]*segment

	dir         string
	segmentSize int64

	// Last successful checkpoint
	lastCheckpoint time.Time
}

// Options for configuring the WAL.
type Options struct {
	// Directory to store WAL files
	Dir string
	// Segment size (default 128MB)
	SegmentSize int64
}

// Record types
const (
	RecordSeries     byte = 1
	RecordSamples    byte = 2
	RecordCheckpoint byte = 3
)

// Record header format:
// | type (1b) | length (8b) | CRC32 (4b) | payload ... |

// New creates a new WAL in the given directory.
func New(opts Options) (*WAL, error) {
	if err := os.MkdirAll(opts.Dir, 0777); err != nil {
		return nil, err
	}

	if opts.SegmentSize == 0 {
		opts.SegmentSize = 128 * 1024 * 1024
	}

	w := &WAL{
		dir:         opts.Dir,
		segmentSize: opts.SegmentSize,
		segments:    make(map[int]*segment),
	}

	// Load existing segments
	if err := w.loadSegments(); err != nil {
		return nil, err
	}

	// Create initial segment if none exists
	if len(w.segments) == 0 {
		if err := w.newSegment(0); err != nil {
			return nil, err
		}
	}

	return w, nil
}

func (w *WAL) loadSegments() error {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		// Parse segment ID from filename
		name := f.Name()
		if !strings.HasPrefix(name, "segment-") {
			continue
		}
		idStr := strings.TrimPrefix(name, "segment-")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}

		// Open segment file
		file, err := os.OpenFile(filepath.Join(w.dir, name), os.O_RDWR, 0666)
		if err != nil {
			return err
		}

		// Get file size
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return err
		}

		// Create segment
		seg := &segment{
			id:     id,
			file:   file,
			offset: info.Size(),
			state:  SegmentSealed,
		}

		w.segments[id] = seg

		// Most recent segment becomes current
		if w.current == nil || seg.id > w.current.id {
			if w.current != nil {
				w.current.state = SegmentSealed
			}
			w.current = seg
			seg.state = SegmentActive
		}
	}

	return nil
}

func (w *WAL) newSegment(id int) error {
	name := fmt.Sprintf("segment-%08d", id)
	f, err := os.OpenFile(filepath.Join(w.dir, name), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	seg := &segment{
		id:     id,
		file:   f,
		state:  SegmentActive,
		offset: 0,
	}

	if w.current != nil {
		w.current.state = SegmentSealed
	}

	w.segments[id] = seg
	w.current = seg

	return nil
}

func (w *WAL) write(typ byte, data []byte) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	// Check if we need to rotate segment
	if w.current.offset >= w.segmentSize {
		if err := w.newSegment(w.current.id + 1); err != nil {
			return err
		}
	}

	// Write record header
	header := make([]byte, 13) // type(1) + length(8) + crc32(4)
	header[0] = typ
	binary.BigEndian.PutUint64(header[1:9], uint64(len(data)))
	crc := crc32.ChecksumIEEE(data)
	binary.BigEndian.PutUint32(header[9:13], crc)

	// Write header
	n, err := w.current.file.Write(header)
	if err != nil {
		return err
	}
	w.current.offset += int64(n)

	// Write data
	n, err = w.current.file.Write(data)
	if err != nil {
		return err
	}
	w.current.offset += int64(n)

	return w.current.file.Sync()
}

// Checkpoint marks all segments up to the current one as flushed
func (w *WAL) Checkpoint() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	// Write checkpoint record
	if err := w.write(RecordCheckpoint, nil); err != nil {
		return err
	}

	// Mark all segments except current as flushed
	for _, seg := range w.segments {
		if seg.id < w.current.id {
			seg.state = SegmentFlushed
		}
	}

	w.lastCheckpoint = time.Now()
	return nil
}

// Clean removes segments that have been checkpointed
func (w *WAL) Clean() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	var toDelete []int
	for id, seg := range w.segments {
		if seg.state == SegmentFlushed {
			toDelete = append(toDelete, id)
		}
	}

	// Sort segment IDs to delete oldest first
	sort.Ints(toDelete)

	// Keep at least one old segment
	if len(toDelete) > 0 {
		toDelete = toDelete[:len(toDelete)-1]
	}

	for _, id := range toDelete {
		seg := w.segments[id]
		name := filepath.Join(w.dir, fmt.Sprintf("segment-%08d", id))

		// Close and delete file
		seg.file.Close()
		if err := os.Remove(name); err != nil {
			return err
		}

		delete(w.segments, id)
	}

	return nil
}

// LogSeries writes a series record to the WAL.
func (w *WAL) LogSeries(lset labels.Labels) error {
	// Encode labels
	buf := make([]byte, 0, 1024)

	// Write labels length
	buf = binary.AppendVarint(buf, int64(len(lset)))

	// Write each label
	for _, l := range lset {
		buf = binary.AppendVarint(buf, int64(len(l.Name)))
		buf = append(buf, l.Name...)
		buf = binary.AppendVarint(buf, int64(len(l.Value)))
		buf = append(buf, l.Value...)
	}

	return w.write(RecordSeries, buf)
}

// LogSample writes a sample record to the WAL.
func (w *WAL) LogSample(lset labels.Labels, sample prompb.Sample) error {
	// First encode labels
	buf := make([]byte, 0, 1024)

	// Write labels length
	buf = binary.AppendVarint(buf, int64(len(lset)))

	// Write each label
	for _, l := range lset {
		buf = binary.AppendVarint(buf, int64(len(l.Name)))
		buf = append(buf, l.Name...)
		buf = binary.AppendVarint(buf, int64(len(l.Value)))
		buf = append(buf, l.Value...)
	}

	// Then encode sample
	tbuf := make([]byte, 16)
	binary.BigEndian.PutUint64(tbuf[:8], uint64(sample.Timestamp))
	binary.BigEndian.PutUint64(tbuf[8:], math.Float64bits(sample.Value))
	buf = append(buf, tbuf...)

	return w.write(RecordSamples, buf)
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	if w.current != nil {
		return w.current.file.Close()
	}
	return nil
}
