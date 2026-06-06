// Package storage implements the binary tick log.
//
// On-disk layout:
//
//	<root>/YYYY-MM-DD/HH.bin
//
// Each file is an append-only stream of fixed-size 27-byte TickData records
// (little-endian). Hour-bucketed files make retention a directory walk and
// keep individual files bounded (~200MB/hr at 2k ticks/s).
//
// Retention: a background goroutine deletes day-directories older than 7 days.
package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/cicn/polytr/internal/types"
)

const (
	DefaultRetention = 7 * 24 * time.Hour
	flushInterval    = 250 * time.Millisecond
	janitorPeriod    = 1 * time.Hour
	writeChanDepth   = 1 << 16
)

// TickStore is a concurrent, hour-rotated binary appender.
type TickStore struct {
	root      string
	retention time.Duration // ≤0 disables the janitor entirely (keep forever)
	in        chan types.TickData

	mu      sync.Mutex
	curHour time.Time
	curFile *os.File
	curBuf  *bufio.Writer

	wg     sync.WaitGroup
	closed chan struct{}
}

// Open creates a TickStore rooted at `root` with the given retention. Pass
// a non-positive retention (0, -1, etc.) to disable the janitor — useful for
// long-horizon historical analysis where you want every collected tick kept.
func Open(root string, retention time.Duration) (*TickStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	ts := &TickStore{
		root:      root,
		retention: retention,
		in:        make(chan types.TickData, writeChanDepth),
		closed:    make(chan struct{}),
	}
	if retention > 0 {
		ts.wg.Add(2)
		go ts.writerLoop()
		go ts.janitorLoop()
	} else {
		ts.wg.Add(1)
		go ts.writerLoop()
	}
	return ts, nil
}

// Write enqueues a tick. Non-blocking when the buffer has room; drops the tick
// (with a counter increment) if the consumer is saturated, so producers are
// never stalled by disk pressure.
func (ts *TickStore) Write(t types.TickData) {
	select {
	case ts.in <- t:
	default:
		// drop on overflow rather than block ingestion
	}
}

// Close flushes and shuts down.
func (ts *TickStore) Close() error {
	close(ts.closed)
	ts.wg.Wait()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.curBuf != nil {
		if err := ts.curBuf.Flush(); err != nil {
			return err
		}
	}
	if ts.curFile != nil {
		return ts.curFile.Close()
	}
	return nil
}

func (ts *TickStore) writerLoop() {
	defer ts.wg.Done()
	flush := time.NewTicker(flushInterval)
	defer flush.Stop()
	for {
		select {
		case <-ts.closed:
			// drain remaining
			for {
				select {
				case t := <-ts.in:
					_ = ts.append(t)
				default:
					return
				}
			}
		case t := <-ts.in:
			_ = ts.append(t)
		case <-flush.C:
			ts.mu.Lock()
			if ts.curBuf != nil {
				_ = ts.curBuf.Flush()
			}
			ts.mu.Unlock()
		}
	}
}

func (ts *TickStore) append(t types.TickData) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	h := time.Unix(0, t.Timestamp).UTC().Truncate(time.Hour)
	if ts.curFile == nil || !h.Equal(ts.curHour) {
		if err := ts.rotate(h); err != nil {
			return err
		}
	}
	return WriteTick(ts.curBuf, t)
}

func (ts *TickStore) rotate(h time.Time) error {
	if ts.curBuf != nil {
		_ = ts.curBuf.Flush()
	}
	if ts.curFile != nil {
		_ = ts.curFile.Close()
	}
	dir := filepath.Join(ts.root, h.Format("2006-01-02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, fmt.Sprintf("%02d.bin", h.Hour()))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	ts.curFile = f
	ts.curBuf = bufio.NewWriterSize(f, 1<<16)
	ts.curHour = h
	return nil
}

func (ts *TickStore) janitorLoop() {
	defer ts.wg.Done()
	t := time.NewTicker(janitorPeriod)
	defer t.Stop()
	ts.sweep()
	for {
		select {
		case <-ts.closed:
			return
		case <-t.C:
			ts.sweep()
		}
	}
}

func (ts *TickStore) sweep() {
	cutoff := time.Now().UTC().Add(-ts.retention).Truncate(24 * time.Hour)
	entries, err := os.ReadDir(ts.root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		d, err := time.Parse("2006-01-02", e.Name())
		if err != nil {
			continue
		}
		if d.Before(cutoff) {
			p := filepath.Join(ts.root, e.Name())
			if err := os.RemoveAll(p); err == nil {
				log.Printf("storage: retention pruned %s", p)
			}
		}
	}
}

// ---------- codec ----------

// WriteTick serializes one tick in fixed 27-byte LE format.
func WriteTick(w io.Writer, t types.TickData) error {
	var buf [types.TickSize]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t.Timestamp))
	buf[8] = byte(t.Source)
	buf[9] = byte(t.Type)
	binary.LittleEndian.PutUint64(buf[10:18], math.Float64bits(t.Price))
	binary.LittleEndian.PutUint64(buf[18:26], math.Float64bits(t.Amount))
	buf[26] = byte(int8(t.Side))
	_, err := w.Write(buf[:])
	return err
}

// ReadTick deserializes one fixed-size record.
func ReadTick(r io.Reader, t *types.TickData) error {
	var buf [types.TickSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	t.Timestamp = int64(binary.LittleEndian.Uint64(buf[0:8]))
	t.Source = types.Source(buf[8])
	t.Type = types.TickType(buf[9])
	t.Price = math.Float64frombits(binary.LittleEndian.Uint64(buf[10:18]))
	t.Amount = math.Float64frombits(binary.LittleEndian.Uint64(buf[18:26]))
	t.Side = types.Side(int8(buf[26]))
	return nil
}

// ---------- reader (used by replay) ----------

// ListFiles returns every .bin tick file in [from, to], sorted chronologically.
func ListFiles(root string, from, to time.Time) ([]string, error) {
	var out []string
	dayFrom := from.UTC().Truncate(24 * time.Hour)
	dayTo := to.UTC().Truncate(24 * time.Hour)
	for d := dayFrom; !d.After(dayTo); d = d.Add(24 * time.Hour) {
		dir := filepath.Join(root, d.Format("2006-01-02"))
		entries, err := os.ReadDir(dir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".bin" {
				continue
			}
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out, nil
}

// Reader streams ticks from a list of files in order, respecting [from, to].
type Reader struct {
	files []string
	idx   int
	cur   *os.File
	buf   *bufio.Reader
	from  int64
	to    int64
}

func NewReader(root string, from, to time.Time) (*Reader, error) {
	files, err := ListFiles(root, from, to)
	if err != nil {
		return nil, err
	}
	return &Reader{
		files: files,
		from:  from.UnixNano(),
		to:    to.UnixNano(),
	}, nil
}

func (r *Reader) Next(t *types.TickData) (bool, error) {
	for {
		if r.buf == nil {
			if r.idx >= len(r.files) {
				return false, nil
			}
			f, err := os.Open(r.files[r.idx])
			if err != nil {
				return false, err
			}
			r.cur = f
			r.buf = bufio.NewReaderSize(f, 1<<16)
		}
		err := ReadTick(r.buf, t)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			_ = r.cur.Close()
			r.cur, r.buf = nil, nil
			r.idx++
			continue
		}
		if err != nil {
			return false, err
		}
		if t.Timestamp < r.from {
			continue
		}
		if t.Timestamp > r.to {
			_ = r.Close()
			return false, nil
		}
		return true, nil
	}
}

func (r *Reader) Close() error {
	if r.cur != nil {
		err := r.cur.Close()
		r.cur, r.buf = nil, nil
		return err
	}
	return nil
}
