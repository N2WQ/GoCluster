package cluster

import "sync"

// stalledConsoleWriter permits tests to hold an actual output operation while
// checking that other destinations and shutdown can continue.
type stalledConsoleWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *stalledConsoleWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

type summaryCaptureWriter struct{ lines chan string }

func (w *summaryCaptureWriter) Write(p []byte) (int, error) {
	w.lines <- string(p)
	return len(p), nil
}
