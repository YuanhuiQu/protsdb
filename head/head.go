package head

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

// Head represents the in-memory state of the storage engine.
// It holds the most recent data in memory and not yet compacted to disk.
type Head struct {
	// Protects concurrent access
	mtx sync.RWMutex

	// All series in memory by their ref
	series map[uint64]*memSeries

	// Reference counter for generating unique series references
	lastRef uint64

	// Time bounds and limits
	minTime   int64 // Minimum time of any sample in the head
	maxTime   int64 // Maximum time of any sample in the head
	chunkSize int   // Target size in samples of each chunk
}

// memSeries represents a single time series in memory
type memSeries struct {
	sync.RWMutex

	// Immutable fields
	ref   uint64        // unique series reference
	lset  labels.Labels // series labels
	chunk *memChunk     // current chunk being written to
}

// memChunk holds sample data for a time series in memory
type memChunk struct {
	minTime int64           // First sample timestamp
	maxTime int64           // Last sample timestamp
	samples []prompb.Sample // Actual samples
}

// Options for configuring the head block
type Options struct {
	// Maximum number of samples per chunk
	ChunkSize int
}

// NewHead creates a new head block
func NewHead(opts Options) *Head {
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 120 // Default chunk size
	}

	return &Head{
		series:    make(map[uint64]*memSeries),
		chunkSize: opts.ChunkSize,
		minTime:   math.MaxInt64,
		maxTime:   math.MinInt64,
	}
}

// getOrCreate returns a series for the given labels, creating a new one if necessary
func (h *Head) getOrCreate(l labels.Labels) (*memSeries, error) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	// First try to find an existing series
	for _, s := range h.series {
		if labels.Equal(s.lset, l) {
			return s, nil
		}
	}

	// Create new series with atomic reference generation
	ref := atomic.AddUint64(&h.lastRef, 1)
	s := &memSeries{
		ref:   ref,
		lset:  l,
		chunk: &memChunk{},
	}
	h.series[ref] = s
	return s, nil
}

// Append adds a new sample to a series
func (h *Head) Append(l labels.Labels, sample prompb.Sample) error {
	s, err := h.getOrCreate(l)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()

	// Update time bounds
	if sample.Timestamp < h.minTime {
		h.minTime = sample.Timestamp
	}
	if sample.Timestamp > h.maxTime {
		h.maxTime = sample.Timestamp
	}

	// Check if we need to create a new chunk
	if len(s.chunk.samples) >= h.chunkSize {
		// Create new chunk
		s.chunk = &memChunk{
			minTime: sample.Timestamp,
			maxTime: sample.Timestamp,
		}
	}

	// Append sample
	s.chunk.samples = append(s.chunk.samples, sample)
	s.chunk.maxTime = sample.Timestamp

	return nil
}

// Series returns a series by its reference
func (h *Head) Series(ref uint64) *memSeries {
	h.mtx.RLock()
	defer h.mtx.RUnlock()
	return h.series[ref]
}
