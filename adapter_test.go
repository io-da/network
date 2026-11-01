package network

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestAdapterStreamingAndDeferredReport(t *testing.T) {
	var events []LogEvent
	body := &trackedBody{Reader: strings.NewReader("sdk response")}
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		res := response(200, "")
		res.Body = body
		return res, nil
	}, WithLogHandler(func(_ context.Context, e LogEvent) { events = append(events, e) }))
	report := &FailureReport{}
	req, _ := http.NewRequestWithContext(ContextWithFailureReport(context.Background(), report), "GET", "http://example.test", nil)
	res, err := c.RoundTripper().RoundTrip(req)
	if err != nil || body.closed || len(events) != 0 {
		t.Fatal(err, body.closed, events)
	}
	if report.ReportFailure(io.EOF) {
		t.Fatal("report before completion")
	}
	data, err := io.ReadAll(res.Body)
	if err != nil || string(data) != "sdk response" || body.closed {
		t.Fatal(err, string(data), body.closed)
	}
	if !report.ReportFailure(errors.New("application rejected 200")) {
		t.Fatal("missing report")
	}
	if len(events) != 1 || string(events[0].ResponseBody) != "sdk response" || events[0].ResponseBodyEvidence != EvidenceComplete || events[0].Kind != "validation_failure" {
		t.Fatal(events)
	}
	if report.ReportFailure(io.EOF) {
		t.Fatal("duplicate report")
	}
	res.Body.Close()
	if !body.closed {
		t.Fatal("not closed")
	}
	// Handle reuse cannot associate a second exchange with earlier evidence.
	if _, err := c.RoundTripper().RoundTrip(req); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
}
func TestAdapterUpgrade(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		kind, data, err := conn.Read(r.Context())
		if err == nil {
			conn.Write(r.Context(), kind, data)
		}
	}))
	defer server.Close()
	c := mustClient(t, server.URL, WithLogHandler(func(context.Context, LogEvent) {}))
	conn, _, err := websocket.Dial(context.Background(), server.URL, &websocket.DialOptions{HTTPClient: c.HTTPClient()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte("upgrade")); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.Read(ctx)
	if err != nil || string(data) != "upgrade" {
		t.Fatal(err, string(data))
	}
	c.RoundTripper().(interface{ CloseIdleConnections() }).CloseIdleConnections()
}
func TestPreTransmissionCapture(t *testing.T) {
	for _, replay := range []bool{true, false} {
		var event LogEvent
		c := clientFor(t, func(r *http.Request) (*http.Response, error) { r.Body.Close(); return nil, io.ErrClosedPipe }, WithLogHandler(func(_ context.Context, e LogEvent) { event = e }))
		opt := Body([]byte("not transmitted"))
		if !replay {
			opt = ReaderBody(strings.NewReader("not transmitted"))
		}
		c.Post(context.Background(), "", opt)
		if replay {
			if string(event.RequestBody) != "not transmitted" || event.RequestBodyEvidence != EvidenceComplete {
				t.Fatal(event)
			}
		} else if event.RequestBodyEvidence != EvidenceUnavailable {
			t.Fatal(event)
		}
	}
	var event LogEvent
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		buf := make([]byte, 2)
		r.Body.Read(buf)
		r.Body.Close()
		return nil, io.EOF
	}, WithLogHandler(func(_ context.Context, e LogEvent) { event = e }))
	c.Post(context.Background(), "", ReaderBody(strings.NewReader("stream")))
	if string(event.RequestBody) != "st" || event.RequestBodyEvidence != EvidencePartial {
		t.Fatal(event)
	}
	c = clientFor(t, func(r *http.Request) (*http.Response, error) { r.Body.Close(); return nil, io.EOF }, WithLogHandler(func(_ context.Context, e LogEvent) { event = e }), WithLogBodyLimit(3))
	c.Post(context.Background(), "", Body([]byte("longer")))
	if string(event.RequestBody) != "lon" || !event.RequestBodyTruncated || event.RequestBodyEvidence != EvidencePartial {
		t.Fatal(event)
	}
}
func TestAttemptPreparationAndNativeReport(t *testing.T) {
	var attempts []int
	var events []LogEvent
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Signed") == "first" {
			return response(503, "retry"), nil
		}
		return response(200, `{"ok":false}`), nil
	}, WithRetryPolicy(RetryPolicy{MaxRetries: 1}), WithLogHandler(func(_ context.Context, e LogEvent) { events = append(events, e) }))
	report := &FailureReport{}
	var data []byte
	err := c.Get(context.Background(), "", CaptureExchange(report), BindBytes(&data), PrepareAttempt(func(ctx context.Context, r *http.Request, n int) error {
		attempts = append(attempts, n)
		v := "first"
		if n > 0 {
			v = "second"
		}
		r.Header.Set("Signed", v)
		return ctx.Err()
	}))
	if err != nil || len(attempts) != 2 || attempts[1] != 1 {
		t.Fatal(err, attempts)
	}
	if !report.ReportFailure(io.ErrUnexpectedEOF) || len(events) != 2 || events[0].ExchangeID != events[1].ExchangeID || string(events[1].ResponseBody) != string(data) {
		t.Fatal(events)
	}
	// Failed preparation stops before sending and is never retried.
	c = clientFor(t, func(*http.Request) (*http.Response, error) { t.Fatal("sent after preparation failed"); return nil, nil }, WithPrepareAttempt(func(context.Context, *http.Request, int) error { return io.EOF }), WithRetryPolicy(RetryPolicy{MaxRetries: 2}))
	if err := c.Get(context.Background(), ""); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", "http://host", nil)
	if _, err := c.RoundTripper().RoundTrip(req); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}
func TestRetryAfterMinimumAndCancellation(t *testing.T) {
	p := RetryPolicy{FirstBackoff: time.Millisecond, MaxBackoff: time.Second, Jitter: true}
	for _, value := range []string{"2", "999999999999999999999999999999", "18446744073709551615"} {
		res := response(429, "")
		res.Header.Set("Retry-After", value)
		delay := p.delay(0, res)
		if value == "2" && delay != 2*time.Second || value != "2" && delay != time.Duration(math.MaxInt64) {
			t.Fatal(value, delay)
		}
	}
	res := response(503, "")
	res.Header.Set("Retry-After", time.Now().Add(time.Minute).UTC().Format(http.TimeFormat))
	if p.delay(0, res) < 58*time.Second {
		t.Fatal("date shortened")
	}
	for _, value := range []string{"-2", "nonsense", "1.5"} {
		res.Header.Set("Retry-After", value)
		if p.delay(0, res) > time.Second {
			t.Fatal(value)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	calls := 0
	c := clientFor(t, func(*http.Request) (*http.Response, error) {
		calls++
		r := response(429, "")
		r.Header.Set("Retry-After", "3600")
		return r, nil
	}, WithRetryPolicy(RetryPolicy{MaxRetries: 1}))
	if err := c.Get(ctx, ""); !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatal(err, calls)
	}
}
func TestAdapterFailuresAndReportRelease(t *testing.T) {
	for _, status := range []int{200, 500} {
		var event LogEvent
		c := clientFor(t, func(*http.Request) (*http.Response, error) { return response(status, "payload"), nil }, WithLogging(Behaviours{LogSuccessfulTransactions: true, LogResponseBodyOnError: true}), WithLogHandler(func(_ context.Context, e LogEvent) { event = e }))
		req, _ := http.NewRequest("GET", "http://host", nil)
		res, err := c.HTTPClient().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var one [1]byte
		res.Body.Read(one[:])
		res.Body.Close()
		if event.StatusCode != status || status == 500 && event.ResponseBodyEvidence != EvidencePartial {
			t.Fatal(event)
		}
	}
	report := &FailureReport{}
	report.Release()
	if report.claim() == nil || report.ReportFailure(nil) {
		t.Fatal("released")
	}
	c := clientFor(t, func(*http.Request) (*http.Response, error) { return response(200, "x"), nil })
	if err := c.Get(context.Background(), "", CaptureExchange(nil)); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
	report = &FailureReport{}
	if err := c.Get(context.Background(), "", CaptureExchange(report), CaptureExchange(&FailureReport{})); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
	report.Release()
	report = &FailureReport{}
	c.Get(context.Background(), "", CaptureExchange(report))
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); report.ReportFailure(io.EOF); report.Release() }()
	}
	wg.Wait()
}
func TestSnapshotFailureFallbackAndAdapterReadError(t *testing.T) {
	var event LogEvent
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		return nil, io.EOF
	}, WithLogHandler(func(_ context.Context, e LogEvent) { event = e }))
	req, _ := http.NewRequest("POST", "http://host", bytes.NewBufferString("body"))
	req.GetBody = func() (io.ReadCloser, error) { return nil, io.EOF }
	c.RoundTripper().RoundTrip(req)
	if string(event.RequestBody) != "body" || event.RequestBodyEvidence != EvidenceComplete {
		t.Fatal(event)
	}
	c = clientFor(t, func(*http.Request) (*http.Response, error) {
		res := response(200, "")
		res.Body = io.NopCloser(brokenReader{})
		return res, nil
	}, WithLogHandler(func(_ context.Context, e LogEvent) { event = e }))
	req, _ = http.NewRequest("GET", "http://host", nil)
	res, err := c.RoundTripper().RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(res.Body); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	res.Body.Close()
	if !errors.Is(event.Error, io.ErrUnexpectedEOF) {
		t.Fatal(event)
	}
}

type writableTestBody struct{ bytes.Buffer }

func (*writableTestBody) Close() error { return nil }
func TestSDKHTTPClientPreservesWritableUpgrade(t *testing.T) {
	c := clientFor(t, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 101, Header: make(http.Header), Body: &writableTestBody{}}, nil
	})
	client := c.HTTPClient()
	if client.Timeout != 0 {
		t.Fatal("upgrade-incompatible timeout")
	}
	req, _ := http.NewRequest("GET", "http://host", nil)
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	rw, ok := res.Body.(io.ReadWriteCloser)
	if !ok {
		t.Fatal("lost upgrade writer")
	}
	if _, err := rw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rw)
	rw.Close()
	if string(data) != "hello" {
		t.Fatal(string(data))
	}
}
