package network

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// WebSocket uses context-aware message operations. Keep a reader active to
// process control frames. Call Close or CloseNow when finished. Connections do
// not reconnect or replay messages automatically. See coder/websocket for its
// concurrency and close semantics.
type WebSocket = websocket.Conn
type MessageType = websocket.MessageType

const (
	MessageText         = websocket.MessageText
	MessageBinary       = websocket.MessageBinary
	StatusNormalClosure = websocket.StatusNormalClosure
)

type WebSocketOptions struct {
	Header           http.Header
	Subprotocols     []string
	HandshakeTimeout time.Duration
	ReadLimit        int64
}

// DialWebSocket shares the client's HTTP transport and logging policy. The
// handshake timeout does not impose a deadline on the established connection.
// HTTP response consumers and HTTP retry policies do not apply to WebSockets.
func (c *Client) DialWebSocket(ctx context.Context, endpoint string, opts WebSocketOptions) (conn *WebSocket, err error) {
	start := time.Now()
	tr := &transaction{ctx: ctx}
	defer func() { c.log(tr, requestConfig{logging: c.config.logging}, "websocket", start, err) }()
	if ctx == nil || opts.HandshakeTimeout < 0 || opts.ReadLimit < 0 {
		return nil, tr.failure(ErrConfiguration, ErrConfiguration)
	}
	u, e := c.resolve(endpoint)
	if e != nil {
		return nil, tr.failure(ErrConfiguration, e)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	headers := make(http.Header)
	if sameOrigin(c.base, u) {
		headers = c.config.headers.Clone()
		if headers == nil {
			headers = make(http.Header)
		}
	}
	for key, values := range opts.Header {
		headers[key] = append([]string(nil), values...)
	}
	tr.request = &http.Request{Method: http.MethodGet, URL: u, Header: headers}
	timeout := opts.HandshakeTimeout
	if timeout == 0 {
		timeout = c.config.timeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	client := c.config.httpClient
	client.Timeout = 0
	conn, tr.response, e = websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPClient: &client, HTTPHeader: headers, Subprotocols: append([]string(nil), opts.Subprotocols...)})
	if e != nil {
		kind := ErrTransport
		if tr.response != nil && tr.response.StatusCode != http.StatusSwitchingProtocols {
			kind = ErrStatus
		}
		return nil, tr.failure(kind, e)
	}
	limit := opts.ReadLimit
	if limit == 0 {
		limit = 1 << 20
	}
	conn.SetReadLimit(limit)
	return conn, nil
}
