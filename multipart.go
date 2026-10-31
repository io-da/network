package network

import (
	"bytes"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"slices"
	"strings"
	"sync"
)

// MultipartFile opens an independent reader for each attempt. The client closes
// each reader. Files are streamed, never buffered in memory. Open must be safe
// to call concurrently if the option is used concurrently.
type MultipartFile struct {
	FieldName   string
	FileName    string
	ContentType string // defaults to application/octet-stream
	Open        func() (io.ReadCloser, error)
}
type multipartSegment struct {
	data []byte
	open func() (io.ReadCloser, error)
}

// MultipartBody snapshots fields and file metadata, and streams replayable file
// parts without goroutines. Files open lazily when the transport reads the body.
func MultipartBody(fields url.Values, files ...MultipartFile) RequestOption {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	contentType := writer.FormDataContentType()
	var segments []multipartSegment
	var buildErr error
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		for _, value := range fields[key] {
			header := textproto.MIMEHeader{}
			header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": key}))
			part, _ := writer.CreatePart(header)
			_, _ = io.WriteString(part, value)
		}
	}
	for _, file := range files {
		if file.Open == nil || file.FieldName == "" || file.FileName == "" || strings.ContainsAny(file.ContentType, "\r\n") {
			buildErr = ErrConfiguration
			break
		}
		ct := file.ContentType
		if ct == "" {
			ct = "application/octet-stream"
		}
		if _, _, err := mime.ParseMediaType(ct); err != nil || !strings.Contains(ct, "/") {
			buildErr = ErrConfiguration
			break
		}
		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": file.FieldName, "filename": file.FileName}))
		header.Set("Content-Type", ct)
		_, _ = writer.CreatePart(header)
		segments = append(segments, multipartSegment{data: bytes.Clone(buffer.Bytes())}, multipartSegment{open: file.Open})
		buffer.Reset()
	}
	_ = writer.Close()
	segments = append(segments, multipartSegment{data: bytes.Clone(buffer.Bytes())})
	return func(c *requestConfig, r *http.Request) error {
		if buildErr != nil {
			return buildErr
		}
		r.Header.Set("Content-Type", contentType)
		return ReplayableBody(func() (io.ReadCloser, error) { return &multipartStream{segments: segments}, nil }, -1)(c, r)
	}
}

// Read is serialized by the HTTP transport; Close may run concurrently to
// interrupt an in-flight upload. Never hold the state lock during file I/O.
type multipartStream struct {
	mu       sync.Mutex
	segments []multipartSegment
	index    int
	current  io.ReadCloser
	closed   bool
}

func (s *multipartStream) Read(p []byte) (int, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return 0, io.ErrClosedPipe
		}
		if len(p) == 0 {
			s.mu.Unlock()
			return 0, nil
		}
		if s.index == len(s.segments) {
			s.mu.Unlock()
			return 0, io.EOF
		}
		reader := s.current
		segment := s.segments[s.index]
		s.mu.Unlock()
		if reader == nil {
			if segment.open != nil {
				var err error
				reader, err = segment.open()
				if err != nil {
					if reader != nil {
						reader.Close()
					}
					return 0, err
				}
				if reader == nil {
					return 0, ErrConfiguration
				}
			} else {
				reader = io.NopCloser(bytes.NewReader(segment.data))
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				reader.Close()
				return 0, io.ErrClosedPipe
			}
			s.current = reader
			s.mu.Unlock()
		}
		n, err := reader.Read(p)
		if err != io.EOF {
			return n, err
		}
		s.mu.Lock()
		shouldClose := s.current != nil
		s.current = nil
		s.index++
		s.mu.Unlock()
		if shouldClose {
			reader.Close()
		}
		if n > 0 {
			return n, nil
		}
	}
}
func (s *multipartStream) Close() error {
	s.mu.Lock()
	s.closed = true
	reader := s.current
	s.current = nil
	s.mu.Unlock()
	if reader != nil {
		return reader.Close()
	}
	return nil
}
