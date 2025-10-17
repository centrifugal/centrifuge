// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package websocket

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

var (
	_ net.Conn = (*h2ServerStream)(nil)
)

// h2ServerStream is a minimal io.ReadWriteCloser for an HTTP/2 extended CONNECT
// tunnel. Read reads from the request body and Write writes to the response
// writer. To ensure data transmission, the stream is flushed on write.
type h2ServerStream struct {
	io.ReadCloser              // http.Request.Body
	io.Writer                  // http.ResponseWriter
	flush         func() error // http.ResponseWriter flush function
}

func (s *h2ServerStream) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	if err != nil {
		log.Printf("[DEBUG] h2ServerStream.Read error: %v (bytes read: %d)", err, n)
	}
	return n, err
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
	// Flush before closing the reader to ensure all data is sent.
	err := s.Flush()
	return errors.Join(err, s.ReadCloser.Close())
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
