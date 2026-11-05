package image_fetcher

import (
	"io"
	"time"
)

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
	if chunk := max(t.bytesPerSec/10, 1); len(p) > chunk {
		p = p[:chunk]
	}
	n, err := t.reader.Read(p)
	t.read += n
	if wait := time.Duration(t.read)*time.Second/time.Duration(t.bytesPerSec) - time.Since(t.start); wait > 0 {
		time.Sleep(wait)
	}
	return n, err
}
