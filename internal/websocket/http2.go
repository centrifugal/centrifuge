package websocket

import (
	"fmt"
	"io"
	"net"
	"time"
)

var (
	_ net.Conn = (*h2ServerStream)(nil)
)

// h2ServerStream is a wrapper for HTTP/2 extended CONNECT tunnel.
type h2ServerStream struct {
	io.ReadCloser
	io.Writer
	flush func() error
}

func (s *h2ServerStream) Read(p []byte) (int, error) {
	return s.ReadCloser.Read(p)
}

func (s *h2ServerStream) Write(p []byte) (int, error) {
	n, err := s.Writer.Write(p)
	if err != nil {
		return n, err
	}
	err = s.Flush()
	return n, err
}

func (s *h2ServerStream) Flush() error {
	if err := s.flush(); err != nil {
		return fmt.Errorf("h2ServerStream: failed to flush: %w", err)
	}
	return nil
}

func (s *h2ServerStream) Close() error {
	return s.ReadCloser.Close()
}

// LocalAddr returns a dummy local address. HTTP/2 streams don't have
// a meaningful local address separate from the underlying connection.
func (s *h2ServerStream) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

// RemoteAddr returns a dummy remote address. HTTP/2 streams don't have
// a meaningful remote address separate from the underlying connection.
func (s *h2ServerStream) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

// SetDeadline is a no-op for HTTP/2 streams. Deadline management is handled
// by the underlying HTTP/2 transport.
func (s *h2ServerStream) SetDeadline(t time.Time) error {
	return nil
}

// SetReadDeadline is a no-op for HTTP/2 streams. Deadline management is handled
// by the underlying HTTP/2 transport.
func (s *h2ServerStream) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline is a no-op for HTTP/2 streams. Deadline management is handled
// by the underlying HTTP/2 transport.
func (s *h2ServerStream) SetWriteDeadline(t time.Time) error {
	return nil
}
