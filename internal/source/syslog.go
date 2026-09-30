package source

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
)

// syslogSource listens for RFC5424/RFC3164 messages over UDP or TCP.
//
// Syslog has no acknowledgement channel, so a datagram that arrives while the
// pipeline is saturated is counted as dropped rather than blocking the read
// loop. That is a deliberate trade-off, and streammesh_source_dropped_total is
// what makes it visible.
type syslogSource struct {
	Base
	conn     net.PacketConn
	listener net.Listener
	addr     string
	mu       sync.Mutex
}

// Addr implements Addressed, reporting the bound listen address.
func (s *syslogSource) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

func newSyslogSource(base Base) (Source, error) {
	return &syslogSource{Base: base}, nil
}

func (s *syslogSource) Start(ctx context.Context) error {
	if s.cfg.Network == "tcp" {
		return s.startTCP(ctx)
	}
	return s.startUDP(ctx)
}

func (s *syslogSource) startUDP(ctx context.Context) error {
	conn, err := net.ListenPacket("udp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("source %q: listen udp %s: %w", s.cfg.ID, s.cfg.Listen, err)
	}
	s.mu.Lock()
	s.conn = conn
	s.addr = conn.LocalAddr().String()
	s.mu.Unlock()
	s.logInfo("syslog listener started", "network", "udp", "addr", conn.LocalAddr().String())

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	buffer := make([]byte, 64*1024)
	for {
		read, _, err := conn.ReadFrom(buffer)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.logError(err, "udp read failed")
			continue
		}
		payload := make([]byte, read)
		copy(payload, buffer[:read])
		s.ingest(payload)
	}
}

func (s *syslogSource) startTCP(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("source %q: listen tcp %s: %w", s.cfg.ID, s.cfg.Listen, err)
	}
	s.mu.Lock()
	s.listener = listener
	s.addr = listener.Addr().String()
	s.mu.Unlock()
	s.logInfo("syslog listener started", "network", "tcp", "addr", listener.Addr().String())

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			s.logError(err, "tcp accept failed")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.serveConn(ctx, conn)
		}()
	}
	wg.Wait()
	return nil
}

func (s *syslogSource) serveConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), s.cfg.MaxLineBytes)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		s.ingest([]byte(line))
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		s.logError(err, "tcp read failed")
	}
}

// ingest parses and forwards a single datagram or line.
func (s *syslogSource) ingest(payload []byte) {
	events, err := s.decode(payload)
	if err != nil {
		s.logError(err, "partially rejected syslog payload")
		if len(events) == 0 {
			return
		}
	}
	if err := s.deliver(events); err != nil {
		// No way to push back on a syslog sender, so the drop is counted and
		// the write-ahead log is what makes it recoverable.
		if s.logger != nil {
			s.logger.Warn("dropping syslog payload: pipeline saturated",
				"source", s.cfg.ID, "events", len(events))
		}
	}
}

// Close releases the listener.
func (s *syslogSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	if s.listener != nil {
		err := s.listener.Close()
		s.listener = nil
		return err
	}
	return nil
}
