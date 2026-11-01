package network

import (
	"io"
	"net/http"
	"sync"
	"time"
)

// RoundTripper adapts the configured transport for SDK-owned requests. It sends
// exactly once, never decodes/drains responses, never converts HTTP statuses to
// transport errors, and preserves io.ReadWriteCloser for upgrades. SDKs own
// response closure, retries, redirects and timeouts. Default headers/base URLs
// are not applied to SDK requests. The configured preparation hook runs once
// per RoundTrip with attempt zero (the SDK owns its attempt numbering).
func (c *Client) RoundTripper() http.RoundTripper { return &sdkTransport{client: c} }

// HTTPClient returns an independent client using the adapter and this client's
// redirect/cookie settings. Timeout is zero to preserve writable upgrade
// bodies (net/http timeout wrappers hide io.Writer). Use request-context
// deadlines, or explicitly set Timeout for HTTP-only SDKs.
func (c *Client) HTTPClient() *http.Client {
	client := c.requestClient(c.config.followRedirects)
	client.Transport = c.RoundTripper()
	client.Timeout = 0
	return &client
}

type sdkTransport struct{ client *Client }

func (a *sdkTransport) CloseIdleConnections() { a.client.CloseIdleConnections() }
func (a *sdkTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c := a.client
	start := time.Now()
	r := req.Clone(req.Context())
	t := &transaction{ctx: r.Context(), request: r, id: nextExchangeID.Add(1)}
	cfg := requestConfig{logging: c.config.logging}
	if report, ok := r.Context().Value(reportContextKey{}).(*FailureReport); ok {
		if err := report.claim(); err != nil {
			if r.Body != nil {
				r.Body.Close()
			}
			return nil, err
		}
		cfg.report = report
	}
	finish := func(err error) {
		c.log(t, cfg, "complete", start, err)
		if cfg.report != nil {
			cfg.report.finish(c, t, cfg, start)
		}
	}
	if c.config.prepare != nil {
		if err := c.config.prepare(r.Context(), r, 0); err != nil {
			if r.Body != nil {
				r.Body.Close()
			}
			finish(err)
			return nil, err
		}
	}
	c.captureRequest(t, cfg)
	res, err := c.config.httpClient.Transport.RoundTrip(r)
	if err != nil {
		finish(err)
		return res, err
	}
	t.response = res
	var statusErr error
	if res.StatusCode >= 400 {
		statusErr = t.failure(ErrStatus, nil)
	}
	if res.Body == nil {
		finish(statusErr)
		return res, nil
	}
	if c.config.logger != nil && (cfg.logging.LogResponseBody || cfg.logging.LogResponseBodyOnError) {
		t.responseCapture = newCapture(c.config.logLimit)
	}
	body := &observedBody{ReadCloser: res.Body, capture: t.responseCapture, finish: func(err error) {
		if err == nil {
			err = statusErr
		}
		finish(err)
	}}
	if writer, ok := res.Body.(io.Writer); ok {
		res.Body = &observedUpgrade{observedBody: body, Writer: writer}
	} else {
		res.Body = body
	}
	return res, nil
}

type observedBody struct {
	io.ReadCloser
	capture *capture
	once    sync.Once
	finish  func(error)
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.capture != nil {
		b.capture.Write(p[:n])
		if err == io.EOF {
			b.capture.markComplete()
		}
	}
	if err != nil {
		b.once.Do(func() {
			if err == io.EOF {
				b.finish(nil)
			} else {
				b.finish(err)
			}
		})
	}
	return n, err
}
func (b *observedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.finish(err) })
	return err
}

type observedUpgrade struct {
	*observedBody
	io.Writer
}

var _ http.RoundTripper = (*sdkTransport)(nil)
var _ io.ReadWriteCloser = (*observedUpgrade)(nil)
