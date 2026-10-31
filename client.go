package network

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is safe for concurrent use. Configuration is fixed at construction.
// Injected transports, callbacks and logging handlers must be concurrency-safe.
type Client struct {
	base   *url.URL
	config clientConfig
}
type clientConfig struct {
	headers    http.Header
	httpClient http.Client
	logger     LogHandler
	logging    Behaviours
	retry      RetryPolicy
	timeout    time.Duration
	maxBody    int64
	logLimit   int
	decoders   map[string]Decoder
}
type ClientOption func(*clientConfig) error

func NewClient(baseURL string, opts ...ClientOption) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" {
		return nil, fmt.Errorf("%w: base URL must be an absolute HTTP(S) or WS(S) URL without credentials or fragment", ErrConfiguration)
	}
	cfg := clientConfig{httpClient: http.Client{Transport: http.DefaultTransport}, timeout: 30 * time.Second, maxBody: 16 << 20, logLimit: 4096, retry: RetryPolicy{FirstBackoff: time.Second, MaxBackoff: 30 * time.Second}, decoders: defaultDecoders()}
	for _, opt := range opts {
		if opt == nil {
			return nil, ErrConfiguration
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.timeout < 0 || cfg.maxBody < 0 || cfg.logLimit < 0 {
		return nil, ErrConfiguration
	}
	if err := cfg.retry.validate(); err != nil {
		return nil, err
	}
	// Own transport configuration while preserving pooling across this client's calls.
	if tr, ok := cfg.httpClient.Transport.(*http.Transport); ok {
		cfg.httpClient.Transport = tr.Clone()
	}
	return &Client{base: u, config: cfg}, nil
}
func (c *Client) CloseIdleConnections() { c.config.httpClient.CloseIdleConnections() }

// resolve appends relative paths to the base path, merging query parameters.
// Absolute endpoints replace the base URL and do not inherit its query.
func (c *Client) resolve(endpoint string) (*url.URL, error) {
	ref, err := url.Parse(endpoint)
	if err != nil {
		return nil, ErrConfiguration
	}
	if ref.User != nil || ref.Fragment != "" || (ref.Host != "" && !ref.IsAbs()) {
		return nil, ErrConfiguration
	}
	if ref.IsAbs() {
		if ref.Host == "" || (ref.Scheme != "http" && ref.Scheme != "https" && ref.Scheme != "ws" && ref.Scheme != "wss") {
			return nil, ErrConfiguration
		}
		return ref, nil
	}
	u := *c.base
	if ref.Path != "" {
		path := strings.TrimRight(u.EscapedPath(), "/") + "/" + strings.TrimLeft(ref.EscapedPath(), "/")
		u.Path, _ = url.PathUnescape(path)
		u.RawPath = path
	}
	query := u.Query()
	for key, values := range ref.Query() {
		query[key] = values
	}
	u.RawQuery = query.Encode()
	return &u, nil
}

func (c *Client) Do(ctx context.Context, method, endpoint string, opts ...RequestOption) (err error) {
	start := time.Now()
	cfg := requestConfig{retry: c.config.retry, logging: c.config.logging, maxBody: c.config.maxBody, timeout: c.config.timeout}
	tr := &transaction{ctx: ctx}
	defer func() {
		if cfg.callback != nil {
			cfg.callback(err)
		}
		c.log(tr, cfg, "complete", start, err)
	}()
	fail := func(kind, cause error) error { return tr.failure(kind, cause) }
	if ctx == nil {
		return fail(ErrConfiguration, errors.New("nil context"))
	}
	u, e := c.resolve(endpoint)
	if e != nil {
		return fail(ErrConfiguration, e)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fail(ErrConfiguration, errors.New("HTTP request requires HTTP(S) URL"))
	}
	req, e := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if e != nil {
		return fail(ErrConfiguration, e)
	}
	tr.request = req
	if sameOrigin(c.base, u) {
		req.Header = c.config.headers.Clone()
		if req.Header == nil {
			req.Header = make(http.Header)
		}
	}
	for _, opt := range opts {
		if opt == nil {
			if req.Body != nil {
				req.Body.Close()
			}
			return fail(ErrConfiguration, ErrConfiguration)
		}
		if e = opt(&cfg, req); e != nil {
			if req.Body != nil {
				req.Body.Close()
			}
			return fail(ErrConfiguration, e)
		}
	}
	if e = cfg.validate(); e != nil {
		if req.Body != nil {
			req.Body.Close()
		}
		return fail(ErrConfiguration, e)
	}
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
		req = req.WithContext(ctx)
		tr.ctx, tr.request = ctx, req
	}
	return c.execute(tr, cfg, start)
}

func (c *Client) Get(ctx context.Context, endpoint string, opts ...RequestOption) error {
	return c.Do(ctx, http.MethodGet, endpoint, opts...)
}
func (c *Client) GetAsync(ctx context.Context, endpoint string, opts ...RequestOption) *AsyncTransaction {
	return c.DoAsync(ctx, http.MethodGet, endpoint, opts...)
}

func (c *Client) Head(ctx context.Context, endpoint string, opts ...RequestOption) error {
	return c.Do(ctx, http.MethodHead, endpoint, opts...)
}
func (c *Client) HeadAsync(ctx context.Context, endpoint string, opts ...RequestOption) *AsyncTransaction {
	return c.DoAsync(ctx, http.MethodHead, endpoint, opts...)
}

func (c *Client) Post(ctx context.Context, endpoint string, opts ...RequestOption) error {
	return c.Do(ctx, http.MethodPost, endpoint, opts...)
}
func (c *Client) PostAsync(ctx context.Context, endpoint string, opts ...RequestOption) *AsyncTransaction {
	return c.DoAsync(ctx, http.MethodPost, endpoint, opts...)
}

func (c *Client) Put(ctx context.Context, endpoint string, opts ...RequestOption) error {
	return c.Do(ctx, http.MethodPut, endpoint, opts...)
}
func (c *Client) PutAsync(ctx context.Context, endpoint string, opts ...RequestOption) *AsyncTransaction {
	return c.DoAsync(ctx, http.MethodPut, endpoint, opts...)
}

func (c *Client) Patch(ctx context.Context, endpoint string, opts ...RequestOption) error {
	return c.Do(ctx, http.MethodPatch, endpoint, opts...)
}
func (c *Client) PatchAsync(ctx context.Context, endpoint string, opts ...RequestOption) *AsyncTransaction {
	return c.DoAsync(ctx, http.MethodPatch, endpoint, opts...)
}

func (c *Client) Delete(ctx context.Context, endpoint string, opts ...RequestOption) error {
	return c.Do(ctx, http.MethodDelete, endpoint, opts...)
}
func (c *Client) DeleteAsync(ctx context.Context, endpoint string, opts ...RequestOption) *AsyncTransaction {
	return c.DoAsync(ctx, http.MethodDelete, endpoint, opts...)
}

// Default credentials never cross origins through absolute endpoint overrides.
func sameOrigin(a, b *url.URL) bool {
	scheme := func(s string) string {
		if s == "ws" {
			return "http"
		}
		if s == "wss" {
			return "https"
		}
		return s
	}
	return strings.EqualFold(a.Host, b.Host) && scheme(a.Scheme) == scheme(b.Scheme)
}
