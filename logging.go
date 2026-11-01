package network

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Behaviours controls logging fields. Error-body logging is enabled by default; captures
// are bounded by WithLogBodyLimit. OnError applies to failed attempts as well as
// final failures. Sensitive application body fields require caller redaction.
type Behaviours struct {
	LogRequestHeaders         bool
	LogRequestHeadersOnError  bool
	LogRequestBody            bool
	LogRequestBodyOnError     bool
	LogResponseHeaders        bool
	LogResponseHeadersOnError bool
	LogResponseBody           bool
	LogResponseBodyOnError    bool
	LogSuccessfulTransactions bool
}

// LogHandler is called synchronously and must be concurrency-safe and not panic.
// Events contain detached headers/body snapshots and may be retained.
type LogHandler func(context.Context, LogEvent)
type LogEvent struct {
	ExchangeID            uint64
	RequestBodyEvidence   BodyEvidence
	ResponseBodyEvidence  BodyEvidence
	Kind                  string // "retry", "complete", or "websocket"
	Method                string
	URL                   string
	StatusCode            int
	Attempts              int
	Duration              time.Duration
	Error                 error
	RequestHeaders        http.Header
	ResponseHeaders       http.Header
	RequestBody           []byte
	ResponseBody          []byte
	RequestBodyTruncated  bool
	ResponseBodyTruncated bool
}

func safeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	clone := *u
	clone.User = nil
	clone.RawQuery = ""
	clone.ForceQuery = false
	clone.Fragment = ""
	clone.RawFragment = ""
	return clone.String()
}

// DefaultLogging returns an independent policy with both error-body flags enabled.
// WithLogging and WithRequestLogging replace the entire policy.
func DefaultLogging() Behaviours {
	return Behaviours{LogRequestBodyOnError: true, LogResponseBodyOnError: true}
}
func sensitiveHeader(key string) bool {
	switch strings.ToLower(key) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key":
		return true
	}
	return false
}
func redactedHeaders(h http.Header) http.Header {
	copy := h.Clone()
	for key := range copy {
		if sensitiveHeader(key) {
			copy[key] = []string{"[REDACTED]"}
		}
	}
	return copy
}

func (c *Client) log(t *transaction, cfg requestConfig, kind string, start time.Time, err error) {
	b := cfg.logging
	if c.config.logger == nil || (err == nil && !b.LogSuccessfulTransactions && !b.LogRequestHeaders && !b.LogResponseHeaders && !b.LogRequestBody && !b.LogResponseBody) {
		return
	}
	event := LogEvent{ExchangeID: t.id, Kind: kind, Error: err, Duration: time.Since(start), Attempts: t.retry + 1}
	failed := err != nil
	if t.request != nil {
		event.Method = t.request.Method
		event.URL = safeURL(t.request.URL)
		if b.LogRequestHeaders || (failed && b.LogRequestHeadersOnError) {
			event.RequestHeaders = redactedHeaders(t.request.Header)
		}
	}
	if t.response != nil {
		event.StatusCode = t.response.StatusCode
		if b.LogResponseHeaders || (failed && b.LogResponseHeadersOnError) {
			event.ResponseHeaders = redactedHeaders(t.response.Header)
		}
	}
	if b.LogRequestBody || (failed && b.LogRequestBodyOnError) {
		event.RequestBody, event.RequestBodyTruncated = t.requestCapture.snapshot()
		event.RequestBodyEvidence = t.requestCapture.evidence()
	}
	if b.LogResponseBody || (failed && b.LogResponseBodyOnError) {
		event.ResponseBody, event.ResponseBodyTruncated = t.responseCapture.snapshot()
		event.ResponseBodyEvidence = t.responseCapture.evidence()
	}
	ctx := t.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	c.config.logger(ctx, event)
}

type capture struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	limit     int
	truncated bool
	complete  bool
}

func newCapture(limit int) *capture { return &capture{limit: limit} }
func (c *capture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(p)
	remaining := c.limit - c.buf.Len()
	if len(p) > remaining {
		p = p[:remaining]
		c.truncated = true
	}
	c.buf.Write(p)
	return n, nil
}
func (c *capture) snapshot() ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.buf.Bytes()), c.truncated
}

type capturedBody struct {
	io.ReadCloser
	capture *capture
}

func (b *capturedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.capture.Write(p[:n])
	if err == io.EOF {
		b.capture.markComplete()
	}
	return n, err
}
