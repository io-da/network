package network

import (
	"context"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RetryPolicy only retries replayable requests. By default, retries apply to
// idempotent methods on transport failures or 408/429/500/502/503/504 responses.
// Condition may explicitly permit other methods/statuses, but cannot retry
// decoding failures, canceled contexts, or non-replayable bodies.
type RetryPolicy struct {
	MaxRetries   int
	FirstBackoff time.Duration
	MaxBackoff   time.Duration
	Jitter       bool
	Condition    func(error, TransactionContract) bool
}

func (p RetryPolicy) validate() error {
	if p.MaxRetries < 0 || p.FirstBackoff < 0 || p.MaxBackoff < 0 || p.MaxBackoff < p.FirstBackoff {
		return ErrConfiguration
	}
	return nil
}
func (p RetryPolicy) delay(attempt int, res *http.Response) time.Duration {
	delay := p.FirstBackoff
	for i := 0; i < attempt && delay < p.MaxBackoff; i++ {
		if delay > p.MaxBackoff/2 {
			delay = p.MaxBackoff
			break
		}
		delay *= 2
	}
	if p.Jitter && delay > 0 {
		delay = time.Duration(rand.Int64N(int64(delay)))
	}
	if delay > p.MaxBackoff {
		delay = p.MaxBackoff
	}
	if res != nil {
		value := strings.TrimSpace(res.Header.Get("Retry-After"))
		// Saturate large valid delta-seconds rather than overflowing or shortening
		// the server's minimum. Context cancellation still bounds the wait.
		digits := value != ""
		for _, ch := range value {
			if ch < '0' || ch > '9' {
				digits = false
				break
			}
		}
		if digits {
			secs, err := strconv.ParseUint(value, 10, 64)
			minimum := time.Duration(math.MaxInt64)
			if err == nil && secs <= uint64(math.MaxInt64/int64(time.Second)) {
				minimum = time.Duration(secs) * time.Second
			}
			if minimum > delay {
				delay = minimum
			}
		} else if date, err := http.ParseTime(value); err == nil {
			if minimum := time.Until(date); minimum > delay {
				delay = minimum
			}
		}
	}

	return delay
}

type transaction struct {
	id              uint64
	ctx             context.Context
	request         *http.Request
	response        *http.Response
	retry           int
	requestCapture  *capture
	responseCapture *capture
}

func (t *transaction) Context() context.Context { return t.ctx }
func (t *transaction) Request() *http.Request   { return t.request }
func (t *transaction) Response() *http.Response { return t.response }
func (t *transaction) Retry() int               { return t.retry }
func (t *transaction) failure(kind, cause error) error {
	e := &Error{Kind: kind, Cause: cause}
	if t.request != nil {
		e.Method = t.request.Method
		e.URL = safeURL(t.request.URL)
	}
	if t.response != nil {
		e.Status = t.response.StatusCode
	}
	return e
}
func (p RetryPolicy) allows(t *transaction, err error) bool {
	if err == nil || t.retry >= p.MaxRetries || t.ctx.Err() != nil || errors.Is(err, ErrConfiguration) || errors.Is(err, ErrDecode) || errors.Is(err, ErrRedirectLimit) || errors.Is(err, ErrRedirectPolicy) {
		return false
	}
	if t.request.Body != nil && t.request.Body != http.NoBody && t.request.GetBody == nil {
		return false
	}
	if p.Condition != nil {
		return p.Condition(err, t)
	}
	switch t.request.Method {
	case "GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE":
	default:
		return false
	}
	if errors.Is(err, ErrTransport) {
		return true
	}
	if t.response != nil {
		switch t.response.StatusCode {
		case 408, 429, 500, 502, 503, 504:
			return true
		}
	}
	return false
}
func (c *Client) execute(t *transaction, cfg requestConfig, start time.Time) (err error) {
	for {
		t.response = nil
		t.responseCapture = nil
		t.requestCapture = nil
		err = c.attempt(t, cfg)
		if !cfg.retry.allows(t, err) {
			return err
		}
		delay := cfg.retry.delay(t.retry, t.response)
		c.log(t, cfg, "retry", start, err)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-t.ctx.Done():
			timer.Stop()
			return t.failure(ErrTransport, t.ctx.Err())
		}
		t.retry++
		t.response = nil
		t.responseCapture = nil
		t.requestCapture = nil
		if t.request.GetBody != nil {
			body, e := t.request.GetBody()
			if e != nil {
				t.response = nil
				return t.failure(ErrTransport, e)
			}
			if body == nil {
				t.response = nil
				return t.failure(ErrTransport, ErrConfiguration)
			}
			t.request.Body = body
		}
	}
}
func (c *Client) attempt(t *transaction, cfg requestConfig) error {
	if cfg.prepare != nil {
		if err := cfg.prepare(t.ctx, t.request, t.retry); err != nil {
			if t.request.Body != nil {
				t.request.Body.Close()
			}
			return t.failure(ErrConfiguration, err)
		}
	}
	c.captureRequest(t, cfg)

	client := c.requestClient(cfg.followRedirects)
	res, err := client.Do(t.request)
	if err != nil {
		return t.failure(ErrTransport, err)
	}
	t.response = res
	defer res.Body.Close()
	for _, bind := range cfg.metadata {
		bind(res)
	}
	var reader io.Reader = res.Body
	if cfg.maxBody > 0 {
		reader = &limitedReader{reader: reader, remaining: cfg.maxBody}
	}
	if c.config.logger != nil && (cfg.logging.LogResponseBody || cfg.logging.LogResponseBodyOnError) {
		t.responseCapture = newCapture(c.config.logLimit)
		reader = &capturedBody{ReadCloser: &readerCloser{Reader: reader}, capture: t.responseCapture}
	}
	// Keep closure ownership even when a handler replaces Response.Body.
	res.Body = &readerCloser{Reader: reader}
	ctx := context.WithValue(t.ctx, decoderContextKey{}, c.config.decoders)
	accepted := !IsErrorResponse(res)
	if cfg.acceptedStatuses != nil {
		accepted = cfg.acceptedStatuses[res.StatusCode]
	}
	if !accepted {
		if cfg.consumeError != nil && t.request.Method != "HEAD" {
			err = cfg.consumeError(ctx, res)
			if err != nil {
				err = errors.Join(ErrDecode, err)
			}
		} else {
			_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
		}
		return t.failure(ErrStatus, err)
	}
	if t.request.Method == "HEAD" || res.StatusCode == http.StatusNoContent || res.StatusCode == http.StatusResetContent || res.StatusCode == http.StatusNotModified {
		return nil
	}
	if cfg.consume != nil {
		err = cfg.consume(ctx, res)
	} else {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	}
	if err != nil {
		return t.failure(ErrDecode, err)
	}
	return nil
}

type readerCloser struct{ io.Reader }

func (*readerCloser) Close() error { return nil }

type limitedReader struct {
	reader    io.Reader
	remaining int64
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		var b [1]byte
		n, err := r.reader.Read(b[:])
		if n > 0 {
			return 0, ErrBodyTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}
