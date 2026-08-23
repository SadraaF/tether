package session

import (
	"crypto/rand"
	"io"
	"sync"
)

// replyWriter collects terminal-initiated responses (DSR, DA, CPR) that the
// vendored emulator writes back; they are looped into the PTY input side.
type replyWriter struct {
	mu  sync.Mutex
	buf []byte
}

func (w *replyWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	if len(w.buf) > 4<<10 { // runaway guard
		w.buf = w.buf[:0]
	}
	return len(p), nil
}

// take drains buffered replies for injection into the PTY.
func (w *replyWriter) take() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) == 0 {
		return nil
	}
	out := w.buf
	w.buf = nil
	return out
}

// newSID returns a short URL-safe random session id.
func newSID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b[:])
}

var _ io.Writer = (*replyWriter)(nil)
