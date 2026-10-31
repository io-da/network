package network

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
)

func TestRedirectDefaultsAndOverrides(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/end", 302)
			return
		}
		hits.Add(1)
		io.WriteString(w, "done")
	}))
	defer server.Close()
	c := mustClient(t, server.URL)
	var status int
	var headers http.Header
	err := c.Get(context.Background(), "start", BindStatusCode(&status), BindHeader(&headers))
	if !errors.Is(err, ErrStatus) || status != 302 || headers.Get("Location") != "/end" || hits.Load() != 0 {
		t.Fatal(err, status, headers, hits.Load())
	}
	var body string
	if err := c.Get(context.Background(), "start", FollowRedirects(true), BindString(&body)); err != nil || body != "done" {
		t.Fatal(err, body)
	}
	c = mustClient(t, server.URL, WithFollowRedirects(true))
	if err := c.Get(context.Background(), "start"); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), "start", FollowRedirects(false)); !errors.Is(err, ErrStatus) {
		t.Fatal(err)
	}
	// Per-request policies do not mutate a shared client.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(follow bool) {
			defer wg.Done()
			err := c.Get(context.Background(), "start", FollowRedirects(follow))
			if follow && err != nil || !follow && !errors.Is(err, ErrStatus) {
				t.Error(err)
			}
		}(i%2 == 0)
	}
	wg.Wait()
}
func TestRedirectMethodAndBodySemantics(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/start" {
					http.Redirect(w, r, "/end", code)
					return
				}
				data, _ := io.ReadAll(r.Body)
				io.WriteString(w, r.Method+":"+string(data))
			}))
			defer server.Close()
			c := mustClient(t, server.URL, WithFollowRedirects(true))
			var body string
			if err := c.Post(context.Background(), "start", Body([]byte("payload")), BindString(&body)); err != nil {
				t.Fatal(err)
			}
			want := "GET:"
			if code == 307 || code == 308 {
				want = "POST:payload"
			}
			if body != want {
				t.Fatal(body, want)
			}
		})
	}
}
func TestRedirectLimitsAndInjectedChecker(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); http.Redirect(w, r, "/loop", 302) }))
	defer server.Close()
	c := mustClient(t, server.URL, WithFollowRedirects(true), WithMaxRedirects(2), WithRetryPolicy(RetryPolicy{MaxRetries: 2}))
	if err := c.Get(context.Background(), ""); !errors.Is(err, ErrRedirectLimit) || hits.Load() != 3 {
		t.Fatal(err, hits.Load())
	}
	if _, err := NewClient(server.URL, WithMaxRedirects(-1)); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
	var checked int
	sentinel := errors.New("custom redirect refused")
	injected := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { checked++; return sentinel }}
	c = mustClient(t, server.URL, WithHTTPClient(injected), WithRetryPolicy(RetryPolicy{MaxRetries: 2}))
	if err := c.Get(context.Background(), ""); !errors.Is(err, ErrStatus) || checked != 0 {
		t.Fatal(err, checked)
	}
	if err := c.Get(context.Background(), "", FollowRedirects(true)); !errors.Is(err, sentinel) || !errors.Is(err, ErrRedirectPolicy) || checked != 1 {
		t.Fatal(err, checked)
	}
	injected.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c = mustClient(t, server.URL, WithHTTPClient(injected), WithFollowRedirects(true))
	if err := c.Get(context.Background(), ""); !errors.Is(err, ErrStatus) {
		t.Fatal(err)
	}
}
func TestRedirectStripsCrossOriginSecrets(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, key := range []string{"Authorization", "Cookie", "X-Api-Key", "Proxy-Authorization"} {
			if r.Header.Get(key) != "" {
				t.Errorf("leaked %s", key)
			}
		}
		if r.Header.Get("X-Trace") != "trace" {
			t.Error("lost ordinary header")
		}
	}))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, 302) }))
	defer origin.Close()
	c := mustClient(t, origin.URL, WithFollowRedirects(true))
	if err := c.Get(context.Background(), "", AuthorizationBearer("secret"), WithHeader("Cookie", "session=secret"), WithHeader("X-Api-Key", "secret"), WithHeader("Proxy-Authorization", "secret"), WithHeader("X-Trace", "trace")); err != nil {
		t.Fatal(err)
	}
}
func TestWebSocketRedirectPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			http.Redirect(w, r, "/ws", 302)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err == nil {
			defer conn.CloseNow()
			conn.Read(r.Context())
		}
	}))
	defer server.Close()
	var logged LogEvent
	c := mustClient(t, server.URL, WithLogHandler(func(_ context.Context, event LogEvent) { logged = event }))
	if _, err := c.DialWebSocket(context.Background(), "", WebSocketOptions{}); !errors.Is(err, ErrStatus) || logged.StatusCode != 302 || len(logged.ResponseBody) == 0 {
		t.Fatal(err, logged)
	}
	c = mustClient(t, server.URL, WithFollowRedirects(true))
	conn, err := c.DialWebSocket(context.Background(), "", WebSocketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	conn.CloseNow()
}
