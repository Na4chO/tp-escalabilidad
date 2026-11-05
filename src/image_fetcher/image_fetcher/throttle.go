package image_fetcher

import (
	"io"
	"time"
)

// throttledReader caps how fast a body is read, so the downloads stay under a bandwidth limit
type throttledReader struct {
	reader      io.Reader
	bytesPerSec int
	start       time.Time
	read        int
}

func newThrottledReader(reader io.Reader, mbps float64) *throttledReader {
	return &throttledReader{reader: reader, bytesPerSec: max(int(mbps*1_000_000/8), 1), start: time.Now()}
}

func (t *throttledReader) Read(p []byte) (int, error) {
	// Small reads keep the rate even instead of bursting a whole buffer at once
	if chunk := max(t.bytesPerSec/10, 1); len(p) > chunk {
		p = p[:chunk]
	}
	n, err := t.reader.Read(p)
	t.read += n
	// Waits against the total read so far, so oversleeping on one read is made up on the next ones
	if wait := time.Duration(t.read)*time.Second/time.Duration(t.bytesPerSec) - time.Since(t.start); wait > 0 {
		time.Sleep(wait)
	}
	return n, err
}
