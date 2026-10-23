package transport

import (
	"bufio"
	"bytes"
	"io"
)

type sseLineReader struct {
	reader io.Reader
	skipLF bool
}

// newSSELineReader accepts the CR, LF and CRLF line endings used by SSE.
func newSSELineReader(reader io.Reader) *bufio.Reader {
	return bufio.NewReader(&sseLineReader{reader: reader})
}

// Read normalizes CR and CRLF to LF, emitting CR immediately and skipping its
// optional LF even when that byte arrives in a later read.
func (r *sseLineReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	data, out := p[:n], p[:0]
	for len(data) > 0 {
		if r.skipLF {
			r.skipLF = false
			if data[0] == '\n' {
				data = data[1:]
				continue
			}
		}
		i := bytes.IndexByte(data, '\r')
		if i < 0 {
			out = append(out, data...)
			break
		}
		out = append(out, data[:i]...)
		out = append(out, '\n')
		data = data[i+1:]
		r.skipLF = true
	}
	return len(out), err
}
