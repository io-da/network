package network

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
}
func clientFor(t *testing.T, fn roundTripFunc, opts ...ClientOption) *Client {
	t.Helper()
	opts = append([]ClientOption{WithHTTPClient(&http.Client{Transport: fn})}, opts...)
	c, err := NewClient("https://example.test/api?key=secret", opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func mustClient(t *testing.T, base string, opts ...ClientOption) *Client {
	t.Helper()
	c, err := NewClient(base, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestHTTPMethodsAndBinding(t *testing.T) {
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/users" || r.URL.Query().Get("key") != "secret" {
			t.Errorf("URL %s", r.URL)
		}
		res := response(200, `{"name":"Ada"}`)
		res.Header.Set("Set-Cookie", "session=value")
		return res, nil
	})
	for _, method := range []struct {
		name  string
		call  func(context.Context, string, ...RequestOption) error
		async func(context.Context, string, ...RequestOption) *AsyncTransaction
	}{
		{"GET", c.Get, c.GetAsync}, {"HEAD", c.Head, c.HeadAsync}, {"POST", c.Post, c.PostAsync}, {"PUT", c.Put, c.PutAsync}, {"PATCH", c.Patch, c.PatchAsync}, {"DELETE", c.Delete, c.DeleteAsync}, {"OPTIONS", c.Options, c.OptionsAsync},
	} {
		t.Run(method.name, func(t *testing.T) {
			var result struct{ Name string }
			var status int
			var headers http.Header
			var cookies []*http.Cookie
			opts := []RequestOption{BindJSON(&result), BindStatusCode(&status), BindHeader(&headers), BindCookies(&cookies)}
			if err := method.call(context.Background(), "users", opts...); err != nil {
				t.Fatal(err)
			}
			if status != 200 || headers.Get("Content-Type") == "" || len(cookies) != 1 {
				t.Fatal(status, headers, cookies)
			}
			if method.name != "HEAD" && result.Name != "Ada" {
				t.Fatal(result)
			}
			a := method.async(context.Background(), "users")
			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := a.Await(); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			if err := a.AwaitContext(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestResponseLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		method string
		opts   func(*bool) []RequestOption
		want   error
	}{
		{"head-error", 500, "HEAD", func(*bool) []RequestOption { return nil }, ErrStatus},
		{"handler-error-status", 500, "GET", func(called *bool) []RequestOption {
			return []RequestOption{HandleResponse(func(*http.Response) error { *called = true; return nil })}
		}, ErrStatus},
		{"custom", 200, "GET", func(called *bool) []RequestOption {
			return []RequestOption{HandleResponse(func(r *http.Response) error { *called = true; return nil })}
		}, nil},
		{"decode", 200, "GET", func(*bool) []RequestOption { var v any; return []RequestOption{BindJSON(&v)} }, ErrDecode},
		{"empty-head", 200, "HEAD", func(*bool) []RequestOption { var v any; return []RequestOption{BindJSON(&v)} }, nil},
		{"no-content", 204, "GET", func(*bool) []RequestOption { var v any; return []RequestOption{BindJSON(&v)} }, nil},
		{"reset-content", 205, "GET", func(*bool) []RequestOption { var v any; return []RequestOption{BindJSON(&v)} }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &trackedBody{Reader: strings.NewReader("broken")}
			status := 0
			called := false
			c := clientFor(t, func(*http.Request) (*http.Response, error) {
				res := response(tc.status, "")
				res.Body = b
				return res, nil
			})
			opts := append(tc.opts(&called), BindStatusCode(&status))
			err := c.Do(context.Background(), tc.method, "", opts...)
			if !errors.Is(err, tc.want) || !b.closed || status != tc.status {
				t.Fatalf("err=%v closed=%v status=%d", err, b.closed, status)
			}
			if called != (tc.name == "custom") {
				t.Fatal("handler invocation", called)
			}
		})
	}
}
func TestConfigurationAndURL(t *testing.T) {
	for _, base := range []string{"", "://bad", "ftp://host", "http://", "https://user:pass@host", "https://host/#fragment"} {
		if _, err := NewClient(base); !errors.Is(err, ErrConfiguration) {
			t.Fatal(base, err)
		}
	}
	for _, opt := range []ClientOption{nil, WithHTTPClient(nil), WithTimeout(-1), WithMaxResponseBody(-1), WithLogBodyLimit(-1), WithRetryPolicy(RetryPolicy{MaxRetries: -1}), WithDecoder("bad", decodeJSON), WithDecoder("application/json", nil), WithTLSConfig(nil)} {
		if _, err := NewClient("https://host", opt); err == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
	c := mustClient(t, "https://host/base%2Fpath?one=1&two=2")
	for _, tc := range []struct{ endpoint, want string }{
		{"", "https://host/base%2Fpath?one=1&two=2"}, {"/child%2Fpart?two=3", "https://host/base%2Fpath/child%2Fpart?one=1&two=3"}, {"?three=4", "https://host/base%2Fpath?one=1&three=4&two=2"}, {"https://other/path?q=2", "https://other/path?q=2"},
	} {
		got, err := c.resolve(tc.endpoint)
		if err != nil || got.String() != tc.want {
			t.Fatal(got, err, tc.want)
		}
	}
	for _, endpoint := range []string{"%xx", "//other/path", "https://u:p@host", "#frag", "ftp://host", "http:/x"} {
		if _, err := c.resolve(endpoint); err == nil {
			t.Fatal(endpoint)
		}
	}
	for _, tc := range []struct {
		ctx              context.Context
		method, endpoint string
		opts             []RequestOption
	}{
		{nil, "GET", "", nil}, {context.Background(), "bad method", "", nil}, {context.Background(), "GET", "%xx", nil}, {context.Background(), "GET", "ws://host", nil},
		{context.Background(), "GET", "", []RequestOption{nil}}, {context.Background(), "GET", "", []RequestOption{Timeout(-1)}}, {context.Background(), "GET", "", []RequestOption{MaxResponseBody(-1)}}, {context.Background(), "GET", "", []RequestOption{Retries(-1)}},
	} {
		if err := c.Do(tc.ctx, tc.method, tc.endpoint, tc.opts...); !errors.Is(err, ErrConfiguration) {
			t.Fatal(err)
		}
	}
	if err := c.GetAsync(context.Background(), "%xx").Await(); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
}
func TestRequestOptionsAndBodies(t *testing.T) {
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if string(data) != `{"n":1}` || r.Header.Get("X-Test") != "last" || r.Header.Get("Gone") != "" {
			t.Fatalf("%s %v", data, r.Header)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "user" || pass != "pass" {
			t.Fatal("auth")
		}
		return response(200, ""), nil
	})
	if err := c.Post(context.Background(), "", Body([]byte("old")), JSONBody(map[string]int{"n": 1}), WithHeader("X-Test", "first"), WithHeader("X-Test", "last"), WithHeader("Gone", "x"), RemoveHeader("Gone"), AuthorizationBearer("token"), AuthorizationBasic("user", "pass")); err != nil {
		t.Fatal(err)
	}
	for _, opt := range []RequestOption{ReaderBody(strings.NewReader("abc")), ReplayableBody(func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("abc")), nil }, 3)} {
		cc := clientFor(t, func(r *http.Request) (*http.Response, error) {
			data, _ := io.ReadAll(r.Body)
			r.Body.Close()
			if string(data) != "abc" {
				t.Fatal(string(data))
			}
			return response(200, ""), nil
		})
		if err := cc.Post(context.Background(), "", opt); err != nil {
			t.Fatal(err)
		}
	}
	data := []byte("abc")
	opt := Body(data)
	data[0] = 'z'
	req, _ := http.NewRequest("POST", "http://host", nil)
	cfg := requestConfig{}
	if err := opt(&cfg, req); err != nil {
		t.Fatal(err)
	}
	replay, _ := req.GetBody()
	got, _ := io.ReadAll(replay)
	replay.Close()
	if string(got) != "abc" {
		t.Fatal(string(got))
	}
	Body(nil)(&cfg, req)
	if req.Body != http.NoBody || req.ContentLength != 0 {
		t.Fatal("body not cleared")
	}
	old := &trackedBody{Reader: strings.NewReader("old")}
	req.Body = old
	Body([]byte("new"))(&cfg, req)
	if !old.closed {
		t.Fatal("old body leaked")
	}
	for _, opt := range []RequestOption{WithRequest(nil), JSONBody(make(chan int)), ReaderBody(nil), ReplayableBody(nil, 0), ReplayableBody(func() (io.ReadCloser, error) { return nil, io.EOF }, -2), ReplayableBody(func() (io.ReadCloser, error) { return nil, io.EOF }, 0), ReplayableBody(func() (io.ReadCloser, error) { return nil, nil }, 0), Callback(nil), BindJSON(nil), BindXML(2), BindBody((*int)(nil)), BindHeader(nil), BindBytes(nil), HandleResponse(nil)} {
		if err := c.Get(context.Background(), "", opt); !errors.Is(err, ErrConfiguration) {
			t.Fatal(err)
		}
	}
	var b []byte
	if err := c.Get(context.Background(), "", BindBytes(&b), BindJSON(new(any))); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), "", BindError(new(any)), BindErrorJSON(new(any))); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
}
func TestConsumersAndLimits(t *testing.T) {
	for _, tc := range []struct {
		name, body, ct string
		status         int
		option         func() RequestOption
		want           error
	}{
		{"json", `{"n":1}`, "application/problem+json", 200, func() RequestOption { return BindBody(new(map[string]int)) }, nil},
		{"xml", `<value>abc</value>`, "application/vendor+xml", 200, func() RequestOption { return BindBody(new(string)) }, nil},
		{"explicit-xml", `<value>abc</value>`, "text/plain", 200, func() RequestOption { return BindXML(new(string)) }, nil},
		{"unsupported", "abc", "text/plain", 200, func() RequestOption { return BindBody(new(string)) }, ErrUnsupportedContentType},
		{"json-extra", "{} {}", "application/json", 200, func() RequestOption { return BindJSON(new(any)) }, ErrDecode},
		{"json-trailing", "{} x", "application/json", 200, func() RequestOption { return BindJSON(new(any)) }, ErrDecode},
		{"xml-extra", "<v/> <v/>", "text/xml", 200, func() RequestOption { return BindXML(new(any)) }, ErrDecode},
		{"xml-text", "<v/> extra", "text/xml", 200, func() RequestOption { return BindXML(new(any)) }, ErrDecode},
		{"xml-bad", "<v>", "text/xml", 200, func() RequestOption { return BindXML(new(any)) }, ErrDecode},
		{"xml-trailing-bad", "<v/> <", "text/xml", 200, func() RequestOption { return BindXML(new(any)) }, ErrDecode},
		{"xml-comment", "<v/> <!-- fine --> <?pi ok?>", "text/xml", 200, func() RequestOption { return BindXML(new(any)) }, nil},
		{"error-json", `{"code":42}`, "application/json", 400, func() RequestOption { return BindErrorJSON(new(any)) }, ErrStatus},
		{"error-auto", `<error/>`, "text/xml", 400, func() RequestOption { return BindError(new(any)) }, ErrStatus},
		{"nil-writer", "abc", "text/plain", 200, func() RequestOption { return WriteBody(nil) }, ErrConfiguration},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := clientFor(t, func(*http.Request) (*http.Response, error) {
				r := response(tc.status, tc.body)
				r.Header.Set("Content-Type", tc.ct)
				return r, nil
			})
			err := c.Get(context.Background(), "", tc.option())
			if !errors.Is(err, tc.want) {
				t.Fatal(err, tc.want)
			}
		})
	}
	c := clientFor(t, func(*http.Request) (*http.Response, error) { return response(200, "12345"), nil })
	var body []byte
	if err := c.Get(context.Background(), "", BindBytes(&body), MaxResponseBody(4)); !errors.Is(err, ErrBodyTooLarge) || body != nil {
		t.Fatal(err, string(body))
	}
	if err := c.Get(context.Background(), "", BindBytes(&body), MaxResponseBody(5)); err != nil || string(body) != "12345" {
		t.Fatal(err, string(body))
	}
	var out bytes.Buffer
	if err := c.Get(context.Background(), "", WriteBody(&out), MaxResponseBody(0)); err != nil || out.String() != "12345" {
		t.Fatal(err, out.String())
	}
	c = clientFor(t, func(*http.Request) (*http.Response, error) {
		r := response(200, "")
		r.Body = io.NopCloser(brokenReader{})
		return r, nil
	})
	if err := c.Get(context.Background(), "", BindBytes(&body)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	limited := &limitedReader{reader: strings.NewReader(""), remaining: 1}
	if n, err := limited.Read(nil); n != 0 || err != nil {
		t.Fatal(n, err)
	}
}

type validated struct {
	N    int
	fail bool
}

func (v *validated) Validate(ctx context.Context) error {
	if v.fail {
		return errors.New("validation failed")
	}
	return ctx.Err()
}
func TestValidationAndCustomDecoder(t *testing.T) {
	c := clientFor(t, func(*http.Request) (*http.Response, error) { return response(200, `{"n":2}`), nil })
	v := validated{}
	if err := c.Get(context.Background(), "", BindJSON(&v)); err != nil || v.N != 2 {
		t.Fatal(err, v)
	}
	v.fail = true
	if err := c.Get(context.Background(), "", BindJSON(&v)); !errors.Is(err, ErrDecode) {
		t.Fatal(err)
	}
	c = clientFor(t, func(*http.Request) (*http.Response, error) {
		r := response(200, "hello")
		r.Header.Set("Content-Type", "application/custom")
		return r, nil
	}, WithDecoder("application/custom", func(r io.Reader, target any) error {
		data, err := io.ReadAll(r)
		*target.(*string) = string(data)
		return err
	}))
	var text string
	if err := c.Get(context.Background(), "", BindBody(&text)); err != nil || text != "hello" {
		t.Fatal(err, text)
	}
}
func TestTLSIsolation(t *testing.T) {
	original := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	provided := &http.Client{Transport: original}
	c := mustClient(t, "https://host", WithHTTPClient(provided), WithTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	if original.TLSClientConfig.InsecureSkipVerify || !c.config.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify {
		t.Fatal("TLS configuration shared")
	}
	if _, err := NewClient("https://host", WithHTTPClient(&http.Client{Transport: roundTripFunc(nil)}), WithTLSConfig(&tls.Config{})); err == nil {
		t.Fatal("custom transport accepted TLS config")
	}
	c = mustClient(t, "https://host", WithHTTPClient(&http.Client{}))
	if c.config.httpClient.Transport == http.DefaultTransport {
		t.Fatal("default transport not cloned")
	}
}
func TestRedirectBodyReplayAndTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/end", http.StatusTemporaryRedirect)
			return
		}
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			return
		}
		data, _ := io.ReadAll(r.Body)
		w.Write(data)
	}))
	defer server.Close()
	c := mustClient(t, server.URL)
	var got []byte
	if err := c.Post(context.Background(), "start", FollowRedirects(true), Body([]byte("old")), Body([]byte("new")), BindBytes(&got)); err != nil || string(got) != "new" {
		t.Fatal(err, string(got))
	}
	if err := c.Get(context.Background(), "slow", Timeout(20*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
func TestErrors(t *testing.T) {
	cause := io.EOF
	e := &Error{Kind: ErrTransport, Cause: cause, Status: 503, Method: "GET", URL: "https://host"}
	if !errors.Is(e, ErrTransport) || !errors.Is(e, io.EOF) || e.ErrorCode() != "503" || e.StatusCode() != 503 || !strings.Contains(e.Error(), "EOF") {
		t.Fatal(e)
	}
	e.Cause = nil
	if strings.Contains(e.Error(), "EOF") {
		t.Fatal(e)
	}
	if got := ExtractErrorStatusCode(fmt.Errorf("wrapped: %w", errors.Join(io.EOF, e)), 0); got != 503 {
		t.Fatal(got)
	}
	if got := ExtractErrorStatusCode(io.EOF, 42); got != 42 {
		t.Fatal(got)
	}
	u, _ := url.Parse("https://u:p@host/path?q=secret#fragment")
	if safeURL(u) != "https://host/path" || safeURL(nil) != "" {
		t.Fatal(safeURL(u))
	}
}
func TestRetryPolicyAndState(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		status       int
		payload      string
		opts         []RequestOption
		calls        int
	}{
		{"get", "GET", 503, "", nil, 3}, {"post", "POST", 503, "", nil, 1}, {"permanent", "GET", 400, "", nil, 1},
		{"decode", "GET", 200, "broken", []RequestOption{BindJSON(new(any))}, 1},
		{"stream", "PUT", 503, "", []RequestOption{ReaderBody(strings.NewReader("data"))}, 1},
		{"replay", "PUT", 503, "", []RequestOption{Body([]byte("data"))}, 3},
		{"custom", "POST", 400, "", []RequestOption{WithRequestRetryPolicy(RetryPolicy{MaxRetries: 2, Condition: func(error, TransactionContract) bool { return true }})}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c := clientFor(t, func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Body != nil {
					data, _ := io.ReadAll(r.Body)
					r.Body.Close()
					if string(data) != "data" {
						t.Fatal(string(data))
					}
				}
				return response(tc.status, tc.payload), nil
			}, WithRetryPolicy(RetryPolicy{MaxRetries: 2}))
			if err := c.Do(context.Background(), tc.method, "", tc.opts...); err == nil {
				t.Fatal("expected failure")
			}
			if calls != tc.calls {
				t.Fatal(calls, tc.calls)
			}
		})
	}
	calls := 0
	c := clientFor(t, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return response(503, "old"), nil
		}
		return nil, io.ErrUnexpectedEOF
	}, WithRetryPolicy(RetryPolicy{MaxRetries: 1}))
	err := c.Get(context.Background(), "")
	if !errors.Is(err, io.ErrUnexpectedEOF) || ExtractErrorStatusCode(err, -1) != 0 {
		t.Fatal(err)
	}
	p := RetryPolicy{FirstBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond}
	if p.delay(0, nil) != time.Millisecond || p.delay(1, nil) != 2*time.Millisecond || p.delay(5, nil) != 3*time.Millisecond {
		t.Fatal("backoff")
	}
	p.Jitter = true
	for i := 0; i < 10; i++ {
		if d := p.delay(0, nil); d < 0 || d >= time.Millisecond {
			t.Fatal(d)
		}
	}
	p = RetryPolicy{MaxBackoff: 2 * time.Second}
	r := response(429, "")
	r.Header.Set("Retry-After", "1")
	if p.delay(0, r) != time.Second {
		t.Fatal("retry-after")
	}
	r.Header.Set("Retry-After", "9999999999")
	if p.delay(0, r) <= 2*time.Second {
		t.Fatal("cap")
	}
	r.Header.Set("Retry-After", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))
	if p.delay(0, r) <= 2*time.Second {
		t.Fatal("date")
	}
	for _, p := range []RetryPolicy{{FirstBackoff: -1}, {MaxBackoff: -1}, {FirstBackoff: 2, MaxBackoff: 1}} {
		if p.validate() == nil {
			t.Fatal(p)
		}
	}
}
func TestRetryCancellationFactoryAndCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var callbacks int
	c := clientFor(t, func(*http.Request) (*http.Response, error) { return response(503, ""), nil }, WithRetryPolicy(RetryPolicy{MaxRetries: 2, FirstBackoff: time.Hour, MaxBackoff: time.Hour}), WithLogHandler(func(_ context.Context, event LogEvent) {
		if event.Kind == "retry" {
			cancel()
		}
	}))
	if err := c.Get(ctx, "", Callback(func(err error) {
		callbacks++
		if err == nil {
			t.Error("callback nil")
		}
	})); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if callbacks != 1 {
		t.Fatal(callbacks)
	}
	calls := 0
	c = clientFor(t, func(r *http.Request) (*http.Response, error) { r.Body.Close(); return nil, io.EOF }, WithRetryPolicy(RetryPolicy{MaxRetries: 1}))
	err := c.Put(context.Background(), "", ReplayableBody(func() (io.ReadCloser, error) {
		calls++
		if calls > 1 {
			return nil, io.ErrClosedPipe
		}
		return io.NopCloser(strings.NewReader("body")), nil
	}, 4))
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	calls = 0
	err = c.Put(context.Background(), "", ReplayableBody(func() (io.ReadCloser, error) {
		calls++
		if calls > 1 {
			return nil, nil
		}
		return io.NopCloser(strings.NewReader("body")), nil
	}, 4))
	if !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	c = clientFor(t, func(*http.Request) (*http.Response, error) { close(started); <-release; return response(200, ""), nil })
	a := c.GetAsync(context.Background(), "")
	<-started
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if err := a.AwaitContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	if err := a.Await(); err != nil {
		t.Fatal(err)
	}
}
func TestConcurrentRequests(t *testing.T) {
	var count atomic.Int32
	c := clientFor(t, func(*http.Request) (*http.Response, error) { count.Add(1); return response(200, `{"ok":true}`), nil })
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var result map[string]bool
			if err := c.Get(context.Background(), "", BindJSON(&result)); err != nil || !result["ok"] {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 50 {
		t.Fatal(count.Load())
	}
}

// Ensure ordinary JSON bindings remain compatible with custom decoding helpers.
var _ Decoder = func(r io.Reader, target any) error { return json.NewDecoder(r).Decode(target) }

func TestOptionFailureClosesBodyAndCallsCallback(t *testing.T) {
	c := clientFor(t, func(*http.Request) (*http.Response, error) { t.Fatal("unexpected request"); return nil, nil })
	for _, opt := range []RequestOption{nil, Timeout(-1), WithRequest(func(*http.Request) error { return io.EOF })} {
		body := &trackedBody{Reader: strings.NewReader("body")}
		callbacks := 0
		err := c.Post(context.Background(), "", Callback(func(error) { callbacks++ }), ReaderBody(body), opt)
		if err == nil || !body.closed || callbacks != 1 {
			t.Fatal(err, body.closed, callbacks)
		}
	}
}
func TestRetryInspectionAndErrorDecoding(t *testing.T) {
	calls := 0
	policy := RetryPolicy{MaxRetries: 1, Condition: func(err error, tr TransactionContract) bool {
		if tr.Context() == nil || tr.Request().Method != "GET" || tr.Response().StatusCode != 503 || tr.Retry() != 0 || err == nil {
			t.Error("incorrect transaction")
		}
		return true
	}}
	c := clientFor(t, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return response(503, ""), nil
		}
		return response(200, ""), nil
	}, WithRetryPolicy(policy))
	if err := c.Get(context.Background(), ""); err != nil || calls != 2 {
		t.Fatal(err, calls)
	}
	calls = 0
	c = clientFor(t, func(*http.Request) (*http.Response, error) { calls++; return response(503, "bad JSON"), nil }, WithRetryPolicy(RetryPolicy{MaxRetries: 2}))
	err := c.Get(context.Background(), "", BindErrorJSON(new(any)))
	if !errors.Is(err, ErrStatus) || !errors.Is(err, ErrDecode) || calls != 1 {
		t.Fatal(err, calls)
	}
	c = clientFor(t, func(*http.Request) (*http.Response, error) { return response(200, "abc"), nil })
	if err := c.Get(context.Background(), "", HandleResponse(func(res *http.Response) error { return res.Body.Close() })); err != nil {
		t.Fatal(err)
	}
}
func TestErrorMessagesRedactURLQueries(t *testing.T) {
	for _, value := range []string{"https://user:password@host/path?secret=value#fragment", "%xx"} {
		e := &Error{Kind: ErrTransport, Cause: fmt.Errorf("wrapped: %w", &url.Error{Op: "Get", URL: value, Err: io.EOF})}
		if message := e.Error(); strings.Contains(message, "password") || strings.Contains(message, "secret") || strings.Contains(message, "%xx") {
			t.Fatal(message)
		}
		if !errors.Is(e, io.EOF) {
			t.Fatal("lost cause")
		}
	}
}

func TestDefaultHeadersAndOriginIsolation(t *testing.T) {
	headers := http.Header{"Authorization": {"Bearer original"}, "X-Default": {"yes"}}
	option := WithDefaultHeaders(headers)
	headers.Set("Authorization", "Bearer changed")
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "other.test" {
			if r.Header.Get("Authorization") != "" || r.Header.Get("X-Default") != "" {
				t.Error("default credentials escaped origin")
			}
		} else {
			want := "Bearer original"
			if r.URL.Path == "/api/override" {
				want = "Bearer request"
			}
			if r.Header.Get("Authorization") != want || r.Header.Get("X-Default") != "yes" {
				t.Fatal(r.Header)
			}
		}
		return response(200, ""), nil
	}, option)
	for _, endpoint := range []string{"", "https://other.test/path"} {
		if err := c.Get(context.Background(), endpoint); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(context.Background(), "override", AuthorizationBearer("request")); err != nil {
		t.Fatal(err)
	}
	ws, _ := url.Parse("wss://example.test")
	https, _ := url.Parse("https://example.test")
	if !sameOrigin(ws, https) {
		t.Fatal("scheme equivalence")
	}
}
func TestStandaloneConversions(t *testing.T) {
	value, err := ParseJSON[map[string]int]([]byte(`{"n":1}`))
	if err != nil || (*value)["n"] != 1 {
		t.Fatal(value, err)
	}
	if value, err := ParseJSON[any]("broken"); value != nil || err == nil {
		t.Fatal(value, err)
	}
	text, err := ParseXML[string]("<value>hello</value>")
	if err != nil || *text != "hello" {
		t.Fatal(text, err)
	}
	if value, err := ParseXML[string]("<value>"); value != nil || err == nil {
		t.Fatal(value, err)
	}
}
