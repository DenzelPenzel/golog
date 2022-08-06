package log

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/denisschmidt/golog/api/v1"
)

type Log struct {
	mu     sync.RWMutex
	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	// create a log instance, and set up that instance
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

/*
When a log starts, it’s responsible for setting itself up for the segments that
already exist on disk or, if the log is new and has no existing segments, for
bootstrapping the initial segment.

We fetch the list of the segments on disk, parse and sort the base offsets
(because we want our slice of segments to be in order from oldest to newest),
*/
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64

	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)

		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains dup for index and store so we skip
		// the dup
		i += 1
	}

	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

// appends a record to the log. We append the record to the active segment
func (l *Log) Append(record *api.Record) (uint64, error) {
	// wrapping this func (and subsequent funcs) with a mutex to coordinate access to this section of the code
	// optimize this further and make the locks per segment rather than across the whole log !!!
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, nil
}

// reads the record stored at the given offset
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment

	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	return s.Read(off)
}

// iterates over the segments and closes them
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// closes the log and then removes its data
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// removes the log and then creates a new log to replace it
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) UpperOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	off := l.segments[len(l.segments)-1].nextOffset

	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

/*
removes all segments whose highest offset is lower than lowest.
because we don’t have disks with infinite space, we’ll periodically call
truncate() to remove old segments whose data we (hopefully) have processed by then and don’t need anymore
*/
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment

	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}

		segments = append(segments, s)
	}

	l.segments = segments
	return nil
}

// returns an io.Reader to read the whole log.
// need this capability when we implement coordinate consensus and need to support snapshots and restoring a log
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))

	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	// concatenate the segments’ stores
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off uint64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, int64(o.off))
	o.off += uint64(n)
	return n, err
}

// creates a new segment, appends that segment to the log’s
// slice of segments, and makes the new segment the active segment so that
// subsequent append calls write to it
func (l *Log) NewSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
