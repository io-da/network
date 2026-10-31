package network

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
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
	if res != nil {
		value := res.Header.Get("Retry-After")
		if secs, err := strconv.ParseInt(value, 10, 64); err == nil && secs >= 0 {
			if secs > int64(p.MaxBackoff/time.Second) {
				delay = p.MaxBackoff
			} else if d := time.Duration(secs) * time.Second; d > delay {
				delay = d
			}
		} else if date, err := http.ParseTime(value); err == nil {
			if d := time.Until(date); d > delay {
				delay = d
			}
		}
	}
	if delay > p.MaxBackoff {
		return p.MaxBackoff
	}
	return delay
}

type transaction struct {
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
	if err == nil || t.retry >= p.MaxRetries || t.ctx.Err() != nil || errors.Is(err, ErrDecode) {
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
	if c.config.logger != nil && (cfg.logging.LogRequestBody || cfg.logging.LogRequestBodyOnError) && t.request.Body != nil {
		t.requestCapture = newCapture(c.config.logLimit)
		t.request.Body = &capturedBody{ReadCloser: t.request.Body, capture: t.requestCapture}
	}
	res, err := c.config.httpClient.Do(t.request)
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
		reader = io.TeeReader(reader, t.responseCapture)
	}
	// Keep closure ownership even when a handler replaces Response.Body.
	res.Body = &readerCloser{Reader: reader}
	ctx := context.WithValue(t.ctx, decoderContextKey{}, c.config.decoders)
	if IsErrorResponse(res) {
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
	if t.request.Method == "HEAD" || res.StatusCode == http.StatusNoContent || res.StatusCode == http.StatusResetContent {
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
