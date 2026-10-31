package network

import (
	"mime"
	"net/http"
	"strconv"
	"strings"
)

// --- Requests

func IsJSONRequest(r *http.Request) bool {
	return contentTypeContains(r.Header, ContentTypeJSON)
}

func IsXMLRequest(r *http.Request) bool {
	return contentTypeContains(r.Header, ContentTypeXML, ContentTypeAppXML)
}

func IsFormRequest(r *http.Request) bool {
	return contentTypeContains(r.Header, ContentTypeForm, ContentTypeFormMultipart)
}

func AcceptsText(r *http.Request) bool {
	return acceptContains(r.Header, ContentTypeText)
}

func AcceptsJSON(r *http.Request) bool {
	return acceptContains(r.Header, ContentTypeJSON)
}

func AcceptsHTML(r *http.Request) bool {
	return acceptContains(r.Header, ContentTypeHTML, ContentTypeXHTML)
}

func AcceptsXML(r *http.Request) bool {
	return acceptContains(r.Header, ContentTypeXML, ContentTypeAppXML, ContentTypeAtomXML)
}

// --- Responses

func IsTextResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypeText)
}

func IsJSONResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypeJSON)
}

func IsXMLResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypeXML, ContentTypeAppXML, ContentTypeAtomXML)
}

func IsHTMLResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypeHTML, ContentTypeXHTML)
}

func IsPDFResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypePDF)
}

func IsPNGResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypePNG)
}

func IsJPEGResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypeJPEG)
}

func IsGIFResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypeGIF)
}

func IsCSVResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypeCSV)
}

func IsZIPResponse(r *http.Response) bool {
	return contentTypeContains(r.Header, ContentTypeZIP, ContentTypeZIPCompressed, ContentType7ZIPCompressed, ContentTypeGZIP, ContentTypeXGZIP)
}

func normalizedMediaType(value string) string {
	mt, _, err := mime.ParseMediaType(value)
	if err != nil || !strings.Contains(mt, "/") {
		return ""
	}
	return mt
}
func contentTypeContains(h http.Header, types ...string) bool {
	mt := normalizedMediaType(h.Get("Content-Type"))
	for _, ct := range types {
		if mt == ct || (ct == ContentTypeJSON && strings.HasSuffix(mt, "+json")) || ((ct == ContentTypeXML || ct == ContentTypeAppXML) && strings.HasSuffix(mt, "+xml")) {
			return true
		}
	}
	return false
}
func acceptContains(h http.Header, types ...string) bool {
	// Choose the most specific matching range, so an explicit q=0 exclusion
	// overrides a wildcard. This answers acceptability, not preference ordering.
	for _, ct := range types {
		specificity, quality := -1, 0.0
		for _, entry := range strings.Split(h.Get("Accept"), ",") {
			mt, params, err := mime.ParseMediaType(strings.TrimSpace(entry))
			if err != nil {
				continue
			}
			q := 1.0
			if value, ok := params["q"]; ok {
				q, err = strconv.ParseFloat(value, 64)
				if err != nil || q < 0 || q > 1 {
					continue
				}
			}
			score := -1
			switch {
			case mt == ct:
				score = 2
			case mt == strings.SplitN(ct, "/", 2)[0]+"/*":
				score = 1
			case mt == "*/*":
				score = 0
			}
			if score > specificity {
				specificity, quality = score, q
			}
		}
		if specificity >= 0 && quality > 0 {
			return true
		}
	}
	return h.Get("Accept") == ""
}
