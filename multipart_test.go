package network

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

type countingReadCloser struct {
	io.Reader
	closed *atomic.Int32
}

func (r *countingReadCloser) Close() error { r.closed.Add(1); return nil }
func TestMultipartStreamingAndReplay(t *testing.T) {
	var opened, closed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/upload", 307)
			return
		}
		reader, err := r.MultipartReader()
		if err != nil {
			t.Error(err)
			http.Error(w, "bad", 400)
			return
		}
		got := map[string][]string{}
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Error(err)
				return
			}
			data, _ := io.ReadAll(part)
			got[part.FormName()] = append(got[part.FormName()], string(data))
			if part.FormName() == "file" && part.FileName() != "report.txt" {
				t.Error(part.FileName())
			}
		}
		if strings.Join(got["tag"], ",") != "one,two" || len(got["file"]) != 1 || got["file"][0] != strings.Repeat("x", 128<<10) {
			t.Error("multipart content mismatch")
		}
	}))
	defer server.Close()
	c := mustClient(t, server.URL, WithFollowRedirects(true))
	values := url.Values{"tag": {"one", "two"}}
	option := MultipartBody(values, MultipartFile{FieldName: "file", FileName: "report.txt", Open: func() (io.ReadCloser, error) {
		opened.Add(1)
		return &countingReadCloser{Reader: strings.NewReader(strings.Repeat("x", 128<<10)), closed: &closed}, nil
	}})
	values.Set("tag", "changed")
	if opened.Load() != 0 {
		t.Fatal("file opened before request")
	}
	if err := c.Post(context.Background(), "start", option); err != nil {
		t.Fatal(err)
	}
	// The redirect may arrive before the initial upload opens its file. Every
	// opened reader must still be closed by completion of the transport.
	c.CloseIdleConnections()
	if opened.Load() < 1 {
		t.Fatal(opened.Load())
	}
	// Test deterministic full consumption and early closure independently below.
}
func TestMultipartReadErrorsAndClosure(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func() (io.ReadCloser, error)
		want error
	}{
		{"open-error", func() (io.ReadCloser, error) { return nil, io.ErrClosedPipe }, io.ErrClosedPipe},
		{"nil-reader", func() (io.ReadCloser, error) { return nil, nil }, ErrConfiguration},
		{"read-error", func() (io.ReadCloser, error) { return io.NopCloser(brokenReader{}), nil }, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("POST", "http://host", nil)
			opt := MultipartBody(nil, MultipartFile{FieldName: "file", FileName: "file.txt", ContentType: "text/plain", Open: tc.open})
			if err := opt(&requestConfig{}, req); err != nil {
				t.Fatal(err)
			}
			_, err := io.ReadAll(req.Body)
			req.Body.Close()
			if !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
	var closed atomic.Int32
	stream := &multipartStream{segments: []multipartSegment{{open: func() (io.ReadCloser, error) {
		return &countingReadCloser{Reader: strings.NewReader("abc"), closed: &closed}, nil
	}}}}
	if n, err := stream.Read(nil); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	var one [1]byte
	if n, err := stream.Read(one[:]); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	stream.Close()
	stream.Close()
	if closed.Load() != 1 {
		t.Fatal(closed.Load())
	}
	if _, err := stream.Read(one[:]); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	stream = &multipartStream{segments: []multipartSegment{{open: func() (io.ReadCloser, error) {
		return &countingReadCloser{Reader: strings.NewReader("abc"), closed: &closed}, io.ErrUnexpectedEOF
	}}}}
	if _, err := stream.Read(one[:]); !errors.Is(err, io.ErrUnexpectedEOF) || closed.Load() != 2 {
		t.Fatal(err, closed.Load())
	}
	stream.Close()
	for _, file := range []MultipartFile{{}, {FieldName: "f", FileName: "x", ContentType: "bad", Open: func() (io.ReadCloser, error) { return nil, nil }}, {FieldName: "f", FileName: "x", ContentType: "text/plain\r\nInjected: yes", Open: func() (io.ReadCloser, error) { return nil, nil }}} {
		req, _ := http.NewRequest("POST", "http://host", nil)
		if err := MultipartBody(nil, file)(&requestConfig{}, req); err == nil {
			t.Fatal("invalid multipart accepted")
		}
	}
}

// Readers may deliver their last bytes and io.EOF together.
type finalChunkReader struct{}

func (finalChunkReader) Read(p []byte) (int, error) { return copy(p, "last"), io.EOF }
func TestMultipartFinalChunkAndCompleteClose(t *testing.T) {
	var closed atomic.Int32
	stream := &multipartStream{segments: []multipartSegment{{open: func() (io.ReadCloser, error) {
		return &countingReadCloser{Reader: finalChunkReader{}, closed: &closed}, nil
	}}, {data: []byte("tail")}}}
	data, err := io.ReadAll(stream)
	stream.Close()
	if err != nil || string(data) != "lasttail" || closed.Load() != 1 {
		t.Fatal(err, string(data), closed.Load())
	}
}

type cancelUploadReader struct {
	started chan struct{}
	stopped chan struct{}
}

func (r *cancelUploadReader) Read([]byte) (int, error) {
	close(r.started)
	<-r.stopped
	return 0, io.ErrClosedPipe
}
func (r *cancelUploadReader) Close() error { close(r.stopped); return nil }
func TestMultipartConcurrentCloseInterruptsRead(t *testing.T) {
	reader := &cancelUploadReader{started: make(chan struct{}), stopped: make(chan struct{})}
	stream := &multipartStream{segments: []multipartSegment{{open: func() (io.ReadCloser, error) { return reader, nil }}}}
	done := make(chan error, 1)
	go func() { _, err := stream.Read(make([]byte, 16)); done <- err }()
	<-reader.started
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	stream.Close() // underlying reader is closed exactly once
}
func TestMultipartCloseDuringOpen(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var closed atomic.Int32
	stream := &multipartStream{segments: []multipartSegment{{open: func() (io.ReadCloser, error) {
		close(started)
		<-release
		return &countingReadCloser{Reader: strings.NewReader("x"), closed: &closed}, nil
	}}}}
	done := make(chan error, 1)
	go func() { _, err := stream.Read(make([]byte, 16)); done <- err }()
	<-started
	stream.Close()
	close(release)
	if err := <-done; !errors.Is(err, io.ErrClosedPipe) || closed.Load() != 1 {
		t.Fatal(err, closed.Load())
	}
}
