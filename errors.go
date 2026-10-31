package network

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

var (
	ErrConfiguration          = errors.New("network: invalid configuration")
	ErrTransport              = errors.New("network: transport failure")
	ErrStatus                 = errors.New("network: unexpected HTTP status")
	ErrDecode                 = errors.New("network: response consumption failed")
	ErrBodyTooLarge           = errors.New("network: response body exceeds limit")
	ErrUnsupportedContentType = errors.New("network: unsupported content type")
)

// Error distinguishes local failures (StatusCode()==0) from HTTP responses.
// Kind is one of the package's sentinel errors; Cause retains the original error.
// URL is sanitized and never contains credentials, query values or fragments.
type Error struct {
	Kind   error
	Cause  error
	Method string
	URL    string
	Status int
}

func (e *Error) Error() string {
	if e.Cause != nil {
		cause := e.Cause
		var urlErr *url.Error
		if errors.As(cause, &urlErr) {
			sanitized := *urlErr
			parsed, err := url.Parse(sanitized.URL)
			if err != nil {
				sanitized.URL = "[invalid URL]"
			} else {
				sanitized.URL = safeURL(parsed)
			}
			cause = &sanitized
		}
		return fmt.Sprintf("%s %s: %v (status %d): %v", e.Method, e.URL, e.Kind, e.Status, cause)
	}
	return fmt.Sprintf("%s %s: %v (status %d)", e.Method, e.URL, e.Kind, e.Status)
}
func (e *Error) StatusCode() int      { return e.Status }
func (e *Error) ErrorCode() string    { return strconv.Itoa(e.Status) }
func (e *Error) Unwrap() error        { return e.Cause }
func (e *Error) Is(target error) bool { return errors.Is(e.Kind, target) }
func ExtractErrorStatusCode(err error, fallback int) int {
	var target interface{ StatusCode() int }
	if errors.As(err, &target) {
		return target.StatusCode()
	}
	return fallback
}
func IsErrorResponse(res *http.Response) bool { return res.StatusCode < 200 || res.StatusCode >= 300 }
