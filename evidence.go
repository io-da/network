package network

import (
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// BodyEvidence describes captured bytes, not proof of transmission to a peer.
type BodyEvidence string

const (
	EvidenceUnavailable BodyEvidence = "unavailable"
	EvidencePartial     BodyEvidence = "partial"
	EvidenceComplete    BodyEvidence = "complete"
)

var nextExchangeID atomic.Uint64

func (c *capture) markComplete() { c.mu.Lock(); c.complete = true; c.mu.Unlock() }
func (c *capture) evidence() BodyEvidence {
	if c == nil {
		return EvidenceUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.complete && !c.truncated {
		return EvidenceComplete
	}
	if c.buf.Len() > 0 || c.truncated {
		return EvidencePartial
	}
	return EvidenceUnavailable
}
func (c *Client) captureRequest(t *transaction, cfg requestConfig) {
	if c.config.logger == nil || !(cfg.logging.LogRequestBody || cfg.logging.LogRequestBodyOnError) {
		return
	}
	t.requestCapture = newCapture(c.config.logLimit)
	r := t.request
	if r.Body == nil || r.Body == http.NoBody {
		t.requestCapture.markComplete()
		return
	}
	if r.GetBody != nil {
		// Diagnostic capture never consumes the transmission reader. A failed
		// snapshot is best-effort and must not prevent the real request.
		body, err := r.GetBody()
		if body != nil {
			defer body.Close()
		}
		if err == nil && body != nil {
			_, err = io.Copy(t.requestCapture, io.LimitReader(body, int64(c.config.logLimit)+1))
			if err == nil {
				t.requestCapture.markComplete()
			}
			return
		}
	}
	r.Body = &capturedBody{ReadCloser: r.Body, capture: t.requestCapture}
}

// PrepareAttemptFunc runs after backoff/body recreation and before capture/send.
// attempt is zero-based. Keep GetBody consistent if replacing the body. Hooks
// must respect ctx and must not mutate client-wide state without synchronization.
type PrepareAttemptFunc func(ctx context.Context, request *http.Request, attempt int) error

func WithPrepareAttempt(fn PrepareAttemptFunc) ClientOption {
	return func(c *clientConfig) error { c.prepare = fn; return nil }
}
func PrepareAttempt(fn PrepareAttemptFunc) RequestOption {
	return func(c *requestConfig, _ *http.Request) error { c.prepare = fn; return nil }
}

// FailureReport retains bounded evidence for one exchange. The zero value is
// ready to use. ReportFailure is available after Do returns, or after an adapter
// response reaches EOF/is closed. Call Release when validation succeeds.
// Handles are single-use and safe for concurrent reporting/releasing.
type FailureReport struct {
	mu       sync.Mutex
	claimed  bool
	released bool
	report   func(error)
}

func (r *FailureReport) claim() error {
	if r == nil {
		return ErrConfiguration
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claimed || r.released {
		return ErrConfiguration
	}
	r.claimed = true
	return nil
}
func CaptureExchange(report *FailureReport) RequestOption {
	return func(c *requestConfig, _ *http.Request) error {
		if c.report != nil {
			return ErrConfiguration
		}
		if err := report.claim(); err != nil {
			return err
		}
		c.report = report
		return nil
	}
}

type reportContextKey struct{}

// ContextWithFailureReport attaches a single-exchange handle to an SDK request.
// Redirects/retries should use separate handles per RoundTrip; use native
// CaptureExchange to retain the final result of the native retry loop.
func ContextWithFailureReport(ctx context.Context, report *FailureReport) context.Context {
	return context.WithValue(ctx, reportContextKey{}, report)
}
func frozenCapture(c *capture) *capture {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := newCapture(c.limit)
	out.buf.Write(c.buf.Bytes())
	out.truncated = c.truncated
	out.complete = c.complete
	return out
}
func (r *FailureReport) finish(c *Client, t *transaction, cfg requestConfig, start time.Time) {
	snapshot := *t
	if t.ctx != nil {
		snapshot.ctx = context.WithoutCancel(t.ctx)
	}
	if t.request != nil {
		snapshot.request = t.request.Clone(snapshot.ctx)
		snapshot.request.Body = nil
		snapshot.request.GetBody = nil
	}
	if t.response != nil {
		res := *t.response
		res.Header = res.Header.Clone()
		res.Body = nil
		res.Request = nil
		snapshot.response = &res
	}
	snapshot.requestCapture = frozenCapture(t.requestCapture)
	snapshot.responseCapture = frozenCapture(t.responseCapture)
	cfg.report = nil
	cfg.callback = nil
	cfg.consume = nil
	cfg.consumeError = nil
	cfg.metadata = nil
	cfg.prepare = nil
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return
	}
	r.report = func(err error) { c.log(&snapshot, cfg, "validation_failure", start, err) }
}

// ReportFailure emits one correlated failure event. False means nil error,
// unfinished exchange, released handle, or a report already emitted.
func (r *FailureReport) ReportFailure(err error) bool {
	if err == nil {
		return false
	}
	r.mu.Lock()
	fn := r.report
	if fn == nil {
		r.mu.Unlock()
		return false
	}
	r.report = nil
	r.released = true
	r.mu.Unlock()
	fn(err)
	return true
}
func (r *FailureReport) Release() { r.mu.Lock(); r.released = true; r.report = nil; r.mu.Unlock() }
