// Package network provides HTTP requests with explicit response consumers,
// replay-aware retries, optional structured logging, and WebSocket dialing.
//
// A Client is safe for concurrent use. Request binding targets, single-use body
// readers, and mutable data captured by callbacks remain the caller's responsibility.
// HTTP responses are owned and closed by the client. WebSocket connections are
// owned and closed by the caller.
package network
