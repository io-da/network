package network

import (
	"net/http"
	"testing"
)

func TestContentTypes(t *testing.T) {
	for _, tc := range []struct {
		ct    string
		check func(*http.Response) bool
	}{
		{"text/plain", IsTextResponse}, {"application/json; charset=utf-8", IsJSONResponse}, {"application/problem+json", IsJSONResponse}, {"text/xml", IsXMLResponse}, {"application/atom+xml", IsXMLResponse}, {"text/html", IsHTMLResponse}, {"application/pdf", IsPDFResponse}, {"image/png", IsPNGResponse}, {"image/jpeg", IsJPEGResponse}, {"image/gif", IsGIFResponse}, {"text/csv", IsCSVResponse}, {"application/zip", IsZIPResponse},
	} {
		r := response(200, "")
		r.Header.Set("Content-Type", tc.ct)
		if !tc.check(r) {
			t.Fatal(tc.ct)
		}
		r.Header.Set("Content-Type", "application/octet-stream")
		if tc.check(r) {
			t.Fatal("false positive", tc.ct)
		}
	}
	for _, tc := range []struct {
		ct    string
		check func(*http.Request) bool
	}{{"application/json", IsJSONRequest}, {"application/xml", IsXMLRequest}, {"multipart/form-data; boundary=x", IsFormRequest}} {
		req := &http.Request{Header: http.Header{"Content-Type": {tc.ct}}}
		if !tc.check(req) {
			t.Fatal(tc.ct)
		}
	}
	if normalizedMediaType("bad;") != "" {
		t.Fatal("invalid MIME")
	}
	if contentTypeContains(http.Header{"Content-Type": {"application/json-garbage"}}, ContentTypeJSON) {
		t.Fatal("substring match")
	}
	for _, tc := range []struct {
		accept string
		check  func(*http.Request) bool
		want   bool
	}{
		{"text/plain", AcceptsText, true}, {"application/json", AcceptsJSON, true}, {"text/html", AcceptsHTML, true}, {"application/xml", AcceptsXML, true}, {"", AcceptsJSON, true}, {"*/*", AcceptsJSON, true}, {"application/*", AcceptsJSON, true}, {"application/json;q=0,*/*;q=1", AcceptsJSON, false}, {"text/html", AcceptsJSON, false}, {"application/json;q=bad", AcceptsJSON, false}, {"application/json;q=2", AcceptsJSON, false}, {"bad;", AcceptsJSON, false},
	} {
		req := &http.Request{Header: http.Header{"Accept": {tc.accept}}}
		if got := tc.check(req); got != tc.want {
			t.Fatal(tc.accept, got)
		}
	}
}
