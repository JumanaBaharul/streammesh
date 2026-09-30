package sink

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/JumanaBaharul/streammesh/internal/event"
)

// fileSink appends newline-delimited JSON and rotates by size, keeping a fixed
// number of generations. The write is buffered and flushed on every batch, so
// the file on disk is always complete up to the last acknowledged batch.
type fileSink struct {
	*Base
	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	written int64
}

func newFileSink(base *Base) (Sink, error) {
	if base.cfg.File == "" {
		return nil, fmt.Errorf("sink %q: file path is required", base.cfg.ID)
	}
	if err := os.MkdirAll(filepath.Dir(base.cfg.File), 0o755); err != nil && filepath.Dir(base.cfg.File) != "." {
		return nil, fmt.Errorf("sink %q: create directory: %w", base.cfg.ID, err)
	}
	return &fileSink{Base: base}, nil
}

func (s *fileSink) Write(_ context.Context, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}
	payload, err := encodeBatch(events)
	if err != nil {
		s.noteFailure(err)
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		s.noteFailure(err)
		return err
	}
	rotateBytes := s.cfg.RotateBytes.Int64()
	if rotateBytes > 0 && s.written+int64(len(payload)) > rotateBytes {
		if err := s.rotate(); err != nil {
			s.noteFailure(err)
			return err
		}
	}
	if _, err := s.writer.Write(payload); err != nil {
		s.noteFailure(err)
		return fmt.Errorf("sink %q: write: %w", s.cfg.ID, err)
	}
	if err := s.writer.Flush(); err != nil {
		s.noteFailure(err)
		return fmt.Errorf("sink %q: flush: %w", s.cfg.ID, err)
	}
	s.written += int64(len(payload))

	s.noteSuccess(events, len(payload))
	return nil
}

func (s *fileSink) ensureOpen() error {
	if s.file != nil {
		return nil
	}
	file, err := os.OpenFile(s.cfg.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("sink %q: open: %w", s.cfg.ID, err)
	}
	info, err := file.Stat()
	if err == nil {
		s.written = info.Size()
	}
	s.file = file
	s.writer = bufio.NewWriterSize(file, 1<<20)
	return nil
}

// rotate moves the active file aside and expires the oldest generation.
func (s *fileSink) rotate() error {
	if s.file != nil {
		if err := s.writer.Flush(); err != nil {
			return err
		}
		if err := s.file.Close(); err != nil {
			return err
		}
		s.file = nil
		s.writer = nil
	}

	keep := s.cfg.RotateKeep
	if keep <= 0 {
		keep = 5
	}
	// Drop the oldest generation, then shift the rest up so the highest number
	// is always the oldest file. Targets are removed first because renaming onto
	// an existing file fails on Windows.
	_ = os.Remove(fmt.Sprintf("%s.%d", s.cfg.File, keep))
	for index := keep - 1; index >= 1; index-- {
		from := fmt.Sprintf("%s.%d", s.cfg.File, index)
		to := fmt.Sprintf("%s.%d", s.cfg.File, index+1)
		if _, err := os.Stat(from); err != nil {
			continue
		}
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("sink %q: rotate %s: %w", s.cfg.ID, from, err)
		}
	}
	if _, err := os.Stat(s.cfg.File); err == nil {
		if err := os.Rename(s.cfg.File, s.cfg.File+".1"); err != nil {
			return fmt.Errorf("sink %q: rotate active file: %w", s.cfg.ID, err)
		}
	}
	s.written = 0
	return s.ensureOpen()
}

func (s *fileSink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writer != nil {
		if err := s.writer.Flush(); err != nil {
			return err
		}
	}
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		s.writer = nil
		return err
	}
	return nil
}

// Path reports the active file, used by tests and the compose demo.
func (s *fileSink) Path() string { return s.cfg.File }
