package network

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestWebSocketEcho(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "unauthorized", 401)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"echo"}})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			kind, data, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), kind, data); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	for _, base := range []string{server.URL, "ws" + strings.TrimPrefix(server.URL, "http")} {
		c := mustClient(t, base)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		conn, err := c.DialWebSocket(ctx, "", WebSocketOptions{Header: http.Header{"Authorization": {"Bearer secret"}}, Subprotocols: []string{"echo"}, HandshakeTimeout: time.Second, ReadLimit: 1024})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if conn.Subprotocol() != "echo" {
			t.Fatal(conn.Subprotocol())
		}
		for _, kind := range []MessageType{MessageText, MessageBinary} {
			if err := conn.Write(ctx, kind, []byte("hello")); err != nil {
				t.Fatal(err)
			}
			got, data, err := conn.Read(ctx)
			if err != nil || got != kind || string(data) != "hello" {
				t.Fatal(got, string(data), err)
			}
		}
		if err := conn.Close(StatusNormalClosure, "done"); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}
func TestWebSocketErrorsAndTLS(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", 403) }))
	defer server.Close()
	var event LogEvent
	c := mustClient(t, server.URL, WithLogHandler(func(_ context.Context, e LogEvent) { event = e }))
	if _, err := c.DialWebSocket(context.Background(), "", WebSocketOptions{}); err == nil || event.StatusCode != 403 || event.Kind != "websocket" {
		t.Fatal(err, event)
	}
	for _, tc := range []struct {
		ctx      context.Context
		endpoint string
		opts     WebSocketOptions
	}{
		{nil, "", WebSocketOptions{}}, {context.Background(), "%xx", WebSocketOptions{}}, {context.Background(), "", WebSocketOptions{HandshakeTimeout: -1}}, {context.Background(), "", WebSocketOptions{ReadLimit: -1}},
	} {
		if _, err := c.DialWebSocket(tc.ctx, tc.endpoint, tc.opts); !errors.Is(err, ErrConfiguration) {
			t.Fatal(err)
		}
	}
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err == nil {
			defer conn.CloseNow()
			conn.Read(r.Context())
		}
	}))
	defer tlsServer.Close()
	c = mustClient(t, tlsServer.URL, WithHTTPClient(tlsServer.Client()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := c.DialWebSocket(ctx, "", WebSocketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	conn.CloseNow()
}

func TestWebSocketLifetimePingAndLimits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer default" {
			http.Error(w, "unauthorized", 401)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			kind, data, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), kind, data); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	c := mustClient(t, server.URL, WithDefaultHeaders(http.Header{"Authorization": {"Bearer default"}}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := c.DialWebSocket(ctx, "", WebSocketOptions{HandshakeTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	time.Sleep(120 * time.Millisecond)
	read := make(chan error, 1)
	go func() { _, _, err := conn.Read(ctx); read <- err }()
	if err := conn.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, MessageText, []byte("alive")); err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	conn.Close(StatusNormalClosure, "done")
	conn, err = c.DialWebSocket(ctx, "", WebSocketOptions{ReadLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if err := conn.Write(ctx, MessageText, []byte("too large")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("read limit not enforced")
	}
}
func TestWebSocketHandshakeCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	c := mustClient(t, server.URL)
	if _, err := c.DialWebSocket(context.Background(), "", WebSocketOptions{HandshakeTimeout: 20 * time.Millisecond}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
