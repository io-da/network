package network

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestQueryAndForm(t *testing.T) {
	values := url.Values{"tag": {"a b", "c+d"}, "key": nil}
	opt := Query(values)
	values.Set("tag", "changed")
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Has("key") || r.URL.Query().Get("page") != "2" || strings.Join(r.URL.Query()["tag"], ",") != "a b,c+d" {
			t.Fatal(r.URL)
		}
		if r.Header.Get("Content-Type") != ContentTypeForm {
			t.Fatal(r.Header)
		}
		data, _ := io.ReadAll(r.Body)
		r.Body.Close()
		form, _ := url.ParseQuery(string(data))
		if form.Get("message") != "a & b" || len(form["tag"]) != 2 {
			t.Fatal(form)
		}
		return response(200, ""), nil
	})
	form := url.Values{"message": {"a & b"}, "tag": {"one", "two"}}
	body := FormBody(form)
	form.Set("message", "changed")
	if err := c.Post(context.Background(), "?page=1", opt, QueryParam("page", "2"), body); err != nil {
		t.Fatal(err)
	}
	req := &http.Request{URL: &url.URL{RawQuery: "invalid=%xx"}}
	if err := QueryParam("x", "y")(&requestConfig{}, req); err == nil {
		t.Fatal("malformed query accepted")
	}
	for _, value := range []string{"https://host/?bad=%xx", "https://host/?bad=1;2"} {
		if _, err := NewClient(value); !errors.Is(err, ErrConfiguration) {
			t.Fatal(err)
		}
	}
	if err := c.Get(context.Background(), "?bad=%xx"); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
}
func TestStatusAndErrorConsumers(t *testing.T) {
	c := clientFor(t, func(*http.Request) (*http.Response, error) { return response(304, ""), nil })
	if err := c.Get(context.Background(), ""); !errors.Is(err, ErrStatus) {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), "", AcceptStatus(200, 304), BindJSON(new(any))); err != nil {
		t.Fatal(err)
	}
	c = clientFor(t, func(*http.Request) (*http.Response, error) { return response(418, "teapot"), nil })
	var data []byte
	if err := c.Get(context.Background(), "", BindErrorBytes(&data)); !errors.Is(err, ErrStatus) || string(data) != "teapot" {
		t.Fatal(err, string(data))
	}
	called := false
	if err := c.Get(context.Background(), "", HandleErrorResponse(func(res *http.Response) error { called = true; return io.ErrUnexpectedEOF })); !errors.Is(err, ErrDecode) || !errors.Is(err, ErrStatus) || !called {
		t.Fatal(err, called)
	}
	var text string
	if err := c.Get(context.Background(), "", AcceptStatus(418), BindString(&text)); err != nil || text != "teapot" {
		t.Fatal(err, text)
	}
	for _, opt := range []RequestOption{AcceptStatus(), AcceptStatus(199), AcceptStatus(600), BindString(nil), BindErrorBytes(nil), HandleErrorResponse(nil)} {
		if err := c.Get(context.Background(), "", opt); !errors.Is(err, ErrConfiguration) {
			t.Fatal(err)
		}
	}
	c = clientFor(t, func(*http.Request) (*http.Response, error) {
		res := response(200, "")
		res.Body = io.NopCloser(brokenReader{})
		return res, nil
	})
	if err := c.Get(context.Background(), "", BindString(&text)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}
func TestCookieJarAndProxy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "ok", Path: "/"})
			return
		}
		cookie, err := r.Cookie("session")
		if err != nil || cookie.Value != "ok" {
			http.Error(w, "no session", 401)
		}
	}))
	defer server.Close()
	jar, _ := cookiejar.New(nil)
	c := mustClient(t, server.URL, WithCookieJar(jar))
	if err := c.Get(context.Background(), "login"); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), "private"); err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "http://destination.invalid/data" {
			t.Error(r.URL)
		}
		io.WriteString(w, "via proxy")
	}))
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)
	c = mustClient(t, "http://destination.invalid", WithProxy(http.ProxyURL(proxyURL)))
	var data string
	if err := c.Get(context.Background(), "data", BindString(&data)); err != nil || data != "via proxy" {
		t.Fatal(err, data)
	}
	if _, err := NewClient(server.URL, WithHTTPClient(&http.Client{Transport: roundTripFunc(nil)}), WithProxy(nil)); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
	var transport *http.Transport
	if _, err := NewClient(server.URL, WithHTTPClient(&http.Client{Transport: transport})); !errors.Is(err, ErrConfiguration) {
		t.Fatal(err)
	}
}
func TestGzipDownloadAndEventStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events" {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: hello\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if r.Header.Get("Accept-Encoding") != "gzip" {
			t.Error("gzip not requested")
		}
		w.Header().Set("Content-Encoding", "gzip")
		zip := gzip.NewWriter(w)
		io.WriteString(zip, "download")
		zip.Close()
	}))
	defer server.Close()
	c := mustClient(t, server.URL)
	var download bytes.Buffer
	if err := c.Get(context.Background(), "", WriteBody(&download)); err != nil || download.String() != "download" {
		t.Fatal(err, download.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Get(ctx, "events", Timeout(0), MaxResponseBody(0), HandleResponse(func(res *http.Response) error {
		line, err := bufio.NewReader(res.Body).ReadString('\n')
		if line != "data: hello\n" {
			t.Fatal(line)
		}
		cancel()
		return err
	})); err != nil {
		t.Fatal(err)
	}
}
func TestDefaultErrorBodyLoggingAndEmptyBody(t *testing.T) {
	var events []LogEvent
	c := clientFor(t, func(r *http.Request) (*http.Response, error) {
		data, _ := io.ReadAll(r.Body)
		r.Body.Close()
		return response(500, "error: "+string(data)), nil
	}, WithLogHandler(func(_ context.Context, e LogEvent) { events = append(events, e) }))
	c.Post(context.Background(), "", Body([]byte("input")))
	if len(events) != 1 || string(events[0].RequestBody) != "input" || string(events[0].ResponseBody) != "error: input" {
		t.Fatal(events)
	}
	events = nil
	c.Post(context.Background(), "", Body([]byte("input")), WithRequestLogging(Behaviours{}))
	if len(events) != 1 || events[0].RequestBody != nil || events[0].ResponseBody != nil {
		t.Fatal(events)
	}
	b := DefaultLogging()
	b.LogSuccessfulTransactions = true
	c = clientFor(t, func(r *http.Request) (*http.Response, error) {
		if r.Body != http.NoBody {
			t.Error("logging changed empty body")
		}
		return response(200, ""), nil
	}, WithLogHandler(func(context.Context, LogEvent) {}), WithLogging(b))
	if err := c.Post(context.Background(), "", Body(nil)); err != nil {
		t.Fatal(err)
	}
}
