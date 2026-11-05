package imagefetcher

import (
	"io"
	"time"
)

type rateLimitedReader struct {
	reader      io.Reader
	bytesPerSec int
	start       time.Time
	read        int
}

func newRateLimitedReader(reader io.Reader, mbps float64) *rateLimitedReader {
	return &rateLimitedReader{reader: reader, bytesPerSec: max(int(mbps*1_000_000/8), 1), start: time.Now()}
}

func (t *rateLimitedReader) Read(p []byte) (int, error) {
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
