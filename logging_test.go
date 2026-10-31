package network

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestLoggingPolicies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		b       Behaviours
		events  int
		details bool
	}{
		{"silent-success", 200, Behaviours{}, 0, false},
		{"summary-success", 200, Behaviours{LogSuccessfulTransactions: true}, 1, false},
		{"always", 200, Behaviours{LogRequestHeaders: true, LogRequestBody: true, LogResponseHeaders: true, LogResponseBody: true}, 1, true},
		{"error-details", 500, Behaviours{LogRequestHeadersOnError: true, LogRequestBodyOnError: true, LogResponseHeadersOnError: true, LogResponseBodyOnError: true}, 1, true},
		{"success-no-error-details", 200, Behaviours{LogSuccessfulTransactions: true, LogRequestHeadersOnError: true, LogRequestBodyOnError: true, LogResponseHeadersOnError: true, LogResponseBodyOnError: true}, 1, false},
		{"error-summary", 500, Behaviours{}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var events []LogEvent
			c := clientFor(t, func(r *http.Request) (*http.Response, error) {
				io.Copy(io.Discard, r.Body)
				r.Body.Close()
				res := response(tc.status, "response-body")
				res.Header.Set("Set-Cookie", "secret")
				return res, nil
			}, WithLogging(tc.b), WithLogBodyLimit(4), WithLogHandler(func(_ context.Context, e LogEvent) { events = append(events, e) }))
			var data []byte
			c.Post(context.Background(), "users?token=secret", Body([]byte("request-body")), AuthorizationBearer("secret"), WithHeader("Cookie", "secret"), WithHeader("X-Api-Key", "secret"), BindBytes(&data))
			if len(events) != tc.events {
				t.Fatal(events)
			}
			if len(events) == 0 {
				return
			}
			e := events[0]
			if e.URL != "https://example.test/api/users" || e.Method != "POST" || e.StatusCode != tc.status || e.Attempts != 1 || e.Duration < 0 {
				t.Fatal(e)
			}
			if tc.details {
				if string(e.RequestBody) != "requ" || string(e.ResponseBody) != "resp" || !e.RequestBodyTruncated || !e.ResponseBodyTruncated {
					t.Fatal(e)
				}
				if e.RequestHeaders.Get("Authorization") != "[REDACTED]" || e.RequestHeaders.Get("Cookie") != "[REDACTED]" || e.RequestHeaders.Get("X-Api-Key") != "[REDACTED]" || e.ResponseHeaders.Get("Set-Cookie") != "[REDACTED]" {
					t.Fatal(e)
				}
			} else if e.RequestHeaders != nil || e.ResponseHeaders != nil || len(e.RequestBody) > 0 || len(e.ResponseBody) > 0 {
				t.Fatal(e)
			}
		})
	}
}
func TestLoggingDecodeFailureAndRetry(t *testing.T) {
	var events []LogEvent
	calls := 0
	c := clientFor(t, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return response(503, "retry"), nil
		}
		return response(200, "bad JSON"), nil
	}, WithLogging(Behaviours{LogResponseBodyOnError: true}), WithRetryPolicy(RetryPolicy{MaxRetries: 1}), WithLogHandler(func(_ context.Context, e LogEvent) { events = append(events, e) }))
	if err := c.Get(context.Background(), "", BindJSON(new(any))); !errors.Is(err, ErrDecode) {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Kind != "retry" || events[1].Kind != "complete" || string(events[1].ResponseBody) != "bad JSON" {
		t.Fatal(events)
	}
	var last LogEvent
	c = clientFor(t, func(*http.Request) (*http.Response, error) { return nil, io.EOF }, WithLogHandler(func(_ context.Context, e LogEvent) { last = e }), WithRequestIndependentLogging())
	c.Get(nil, "")
	if last.Error == nil {
		t.Fatal("missing nil-context log")
	}
	c.Get(context.Background(), "")
	if last.RequestBody != nil {
		t.Fatal(last)
	}
}

// Local test helper exercises request policy override independently of defaults.
func WithRequestIndependentLogging() ClientOption {
	return WithLogging(Behaviours{LogRequestBodyOnError: true, LogResponseBodyOnError: true})
}
func TestRequestLoggingOverrideAndCapture(t *testing.T) {
	called := false
	c := clientFor(t, func(*http.Request) (*http.Response, error) { return response(200, ""), nil }, WithLogHandler(func(context.Context, LogEvent) { called = true }))
	if err := c.Get(context.Background(), "", WithRequestLogging(Behaviours{LogSuccessfulTransactions: true})); err != nil || !called {
		t.Fatal(err, called)
	}
	cap := newCapture(3)
	if n, err := cap.Write([]byte("12")); n != 2 || err != nil {
		t.Fatal(n, err)
	}
	got, truncated := cap.snapshot()
	got[0] = 'x'
	again, _ := cap.snapshot()
	if string(again) != "12" || truncated {
		t.Fatal(string(again), truncated)
	}
	cap.Write([]byte("345"))
	got, truncated = cap.snapshot()
	if string(got) != "123" || !truncated {
		t.Fatal(string(got), truncated)
	}
	r := &capturedBody{ReadCloser: io.NopCloser(strings.NewReader("hello")), capture: newCapture(0)}
	io.ReadAll(r)
	r.Close()
	_, truncated = r.capture.snapshot()
	if !truncated {
		t.Fatal("zero limit")
	}
}
