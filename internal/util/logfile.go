package util

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// SetupFileLogging tees the standard logger to `path` in addition to stderr.
// Pass an empty path to no-op. The returned closer flushes/closes the file
// on shutdown; callers should `defer` it.
//
// The file is opened in O_APPEND mode so multiple processes (or container
// restarts) safely add to the same log; the host can then rotate it
// externally via logrotate.
func SetupFileLogging(path string) (io.Closer, error) {
	if path == "" {
		return io.NopCloser(nil), nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("log dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	log.SetOutput(io.MultiWriter(os.Stderr, f))
	log.Printf("logging: writing to stderr + %s", path)
	return f, nil
}
