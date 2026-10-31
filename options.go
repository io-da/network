package network

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strings"
	"time"
)

// RequestOption configures one request. Options run in caller order; scalar
// settings use the last value. Only one success and one error consumer are allowed.
type RequestOption func(*requestConfig, *http.Request) error

type requestConfig struct {
	retry        RetryPolicy
	timeout      time.Duration
	maxBody      int64
	logging      Behaviours
	consume      func(context.Context, *http.Response) error
	consumeError func(context.Context, *http.Response) error
	metadata     []func(*http.Response)
	callback     func(error)
}

func (r requestConfig) validate() error {
	if r.timeout < 0 || r.maxBody < 0 {
		return ErrConfiguration
	}
	return r.retry.validate()
}
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *clientConfig) error {
		if client == nil {
			return ErrConfiguration
		}
		c.httpClient = *client
		if c.httpClient.Transport == nil {
			c.httpClient.Transport = http.DefaultTransport
		}
		return nil
	}
}

// WithTLSConfig clones the configuration. Apply after WithHTTPClient.
func WithTLSConfig(cfg *tls.Config) ClientOption {
	return func(c *clientConfig) error {
		tr, ok := c.httpClient.Transport.(*http.Transport)
		if !ok || cfg == nil {
			return fmt.Errorf("%w: TLS configuration requires an HTTP transport", ErrConfiguration)
		}
		clone := tr.Clone()
		clone.TLSClientConfig = cfg.Clone()
		c.httpClient.Transport = clone
		return nil
	}
}
func WithLogHandler(handler LogHandler) ClientOption {
	return func(c *clientConfig) error { c.logger = handler; return nil }
}
func WithLogging(b Behaviours) ClientOption {
	return func(c *clientConfig) error { c.logging = b; return nil }
}
func WithTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) error { c.timeout = d; return nil }
}
func WithRetryPolicy(p RetryPolicy) ClientOption {
	return func(c *clientConfig) error { c.retry = p; return nil }
}

// WithMaxResponseBody limits decoded/buffered responses (default 16 MiB).
// Zero disables the limit. It also applies to custom handlers and streamed copies.
func WithMaxResponseBody(n int64) ClientOption {
	return func(c *clientConfig) error { c.maxBody = n; return nil }
}
func WithLogBodyLimit(n int) ClientOption {
	return func(c *clientConfig) error { c.logLimit = n; return nil }
}
func WithDecoder(mediaType string, decoder Decoder) ClientOption {
	return func(c *clientConfig) error {
		mt, _, err := mime.ParseMediaType(mediaType)
		if err != nil || decoder == nil || !strings.Contains(mt, "/") || strings.Contains(mt, "*") {
			return ErrConfiguration
		}
		c.decoders[mt] = decoder
		return nil
	}
}
func Timeout(d time.Duration) RequestOption {
	return func(c *requestConfig, _ *http.Request) error { c.timeout = d; return nil }
}
func Retries(n int) RequestOption {
	return func(c *requestConfig, _ *http.Request) error { c.retry.MaxRetries = n; return nil }
}
func WithRequestRetryPolicy(p RetryPolicy) RequestOption {
	return func(c *requestConfig, _ *http.Request) error { c.retry = p; return nil }
}
func WithRequestLogging(b Behaviours) RequestOption {
	return func(c *requestConfig, _ *http.Request) error { c.logging = b; return nil }
}
func MaxResponseBody(n int64) RequestOption {
	return func(c *requestConfig, _ *http.Request) error { c.maxBody = n; return nil }
}
func WithHeader(key, value string) RequestOption {
	return WithRequest(func(r *http.Request) error { r.Header.Set(key, value); return nil })
}
func RemoveHeader(key string) RequestOption {
	return WithRequest(func(r *http.Request) error { r.Header.Del(key); return nil })
}
func AuthorizationBearer(token string) RequestOption {
	return WithHeader("Authorization", "Bearer "+token)
}
func AuthorizationBasic(user, password string) RequestOption {
	return WithRequest(func(r *http.Request) error { r.SetBasicAuth(user, password); return nil })
}

// WithRequest is an escape hatch for request customization before execution.
// A replacement body must also set GetBody and ContentLength appropriately.
func WithRequest(fn func(*http.Request) error) RequestOption {
	return func(_ *requestConfig, r *http.Request) error {
		if fn == nil {
			return ErrConfiguration
		}
		return fn(r)
	}
}
func replaceBody(r *http.Request, body io.ReadCloser, factory func() (io.ReadCloser, error), size int64) {
	if r.Body != nil {
		r.Body.Close()
	}
	r.Body, r.GetBody, r.ContentLength = body, factory, size
}

// Body copies data when the option is constructed and supports redirects/retries.
func Body(data []byte) RequestOption {
	data = bytes.Clone(data)
	return WithRequest(func(r *http.Request) error {
		factory := func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }
		b, _ := factory()
		if len(data) == 0 {
			b = http.NoBody
		}
		replaceBody(r, b, factory, int64(len(data)))
		return nil
	})
}
func JSONBody(value any) RequestOption {
	data, err := json.Marshal(value)
	return func(c *requestConfig, r *http.Request) error {
		if err != nil {
			return err
		}
		r.Header.Set("Content-Type", ContentTypeJSON)
		return Body(data)(c, r)
	}
}

// ReaderBody transfers ownership of reader to the request. This option is single
// use; it does not permit retries or redirects that require replaying the body.
func ReaderBody(reader io.Reader) RequestOption {
	return WithRequest(func(r *http.Request) error {
		if reader == nil {
			return ErrConfiguration
		}
		body, ok := reader.(io.ReadCloser)
		if !ok {
			body = io.NopCloser(reader)
		}
		replaceBody(r, body, nil, -1)
		return nil
	})
}

// ReplayableBody calls factory for every attempt. Each reader is owned by the
// request; size -1 means unknown. Factories must return independent readers.
func ReplayableBody(factory func() (io.ReadCloser, error), size int64) RequestOption {
	return WithRequest(func(r *http.Request) error {
		if factory == nil || size < -1 {
			return ErrConfiguration
		}
		body, err := factory()
		if err != nil {
			return err
		}
		if body == nil {
			return ErrConfiguration
		}
		replaceBody(r, body, factory, size)
		return nil
	})
}
func BindHeader(target *http.Header) RequestOption {
	return metadata(target, func(r *http.Response) { *target = r.Header.Clone() })
}
func BindCookies(target *[]*http.Cookie) RequestOption {
	return metadata(target, func(r *http.Response) { *target = r.Cookies() })
}
func BindStatusCode(target *int) RequestOption {
	return metadata(target, func(r *http.Response) { *target = r.StatusCode })
}
func metadata(target any, fn func(*http.Response)) RequestOption {
	return func(c *requestConfig, _ *http.Request) error {
		if err := validateTarget(target); err != nil {
			return err
		}
		c.metadata = append(c.metadata, fn)
		return nil
	}
}
func validateTarget(target any) error {
	v := reflect.ValueOf(target)
	if !v.IsValid() || v.Kind() != reflect.Pointer || v.IsNil() {
		return fmt.Errorf("%w: binding requires a non-nil pointer", ErrConfiguration)
	}
	return nil
}
func consumer(fn func(context.Context, *http.Response) error, onError bool) RequestOption {
	return func(c *requestConfig, _ *http.Request) error {
		slot := &c.consume
		if onError {
			slot = &c.consumeError
		}
		if *slot != nil {
			return fmt.Errorf("%w: conflicting body consumers", ErrConfiguration)
		}
		*slot = fn
		return nil
	}
}
func bind(target any, format string, onError bool) RequestOption {
	return func(c *requestConfig, r *http.Request) error {
		if err := validateTarget(target); err != nil {
			return err
		}
		return consumer(func(ctx context.Context, res *http.Response) error {
			decoders := ctx.Value(decoderContextKey{}).(map[string]Decoder)
			mt := format
			if mt == "" {
				mt = normalizedMediaType(res.Header.Get("Content-Type"))
			}
			decoder := decoders[mt]
			if decoder == nil && strings.HasSuffix(mt, "+json") {
				decoder = decoders[ContentTypeJSON]
			}
			if decoder == nil && strings.HasSuffix(mt, "+xml") {
				decoder = decoders[ContentTypeAppXML]
			}
			if decoder == nil {
				return ErrUnsupportedContentType
			}
			if err := decoder(res.Body, target); err != nil {
				return err
			}
			if validator, ok := target.(SecondPassValidationContract); ok {
				return validator.Validate(ctx)
			}
			return nil
		}, onError)(c, r)
	}
}
func BindJSON(target any) RequestOption      { return bind(target, ContentTypeJSON, false) }
func BindXML(target any) RequestOption       { return bind(target, ContentTypeAppXML, false) }
func BindBody(target any) RequestOption      { return bind(target, "", false) }
func BindError(target any) RequestOption     { return bind(target, "", true) }
func BindErrorJSON(target any) RequestOption { return bind(target, ContentTypeJSON, true) }
func BindBytes(target *[]byte) RequestOption {
	return func(c *requestConfig, r *http.Request) error {
		if err := validateTarget(target); err != nil {
			return err
		}
		return consumer(func(_ context.Context, res *http.Response) error {
			data, err := io.ReadAll(res.Body)
			if err == nil {
				*target = data
			}
			return err
		}, false)(c, r)
	}
}
func WriteBody(writer io.Writer) RequestOption {
	return HandleResponse(func(res *http.Response) error {
		if writer == nil {
			return ErrConfiguration
		}
		_, err := io.Copy(writer, res.Body)
		return err
	})
}

// HandleResponse runs only for successful responses. The response and its body
// are valid only during the handler; the client always closes the body afterwards.
func HandleResponse(handler func(*http.Response) error) RequestOption {
	return func(c *requestConfig, r *http.Request) error {
		if handler == nil {
			return ErrConfiguration
		}
		return consumer(func(_ context.Context, res *http.Response) error { return handler(res) }, false)(c, r)
	}
}

// Callback runs once with the final result, before Do/Await returns. It must not
// wait for the same async transaction. Callbacks must not panic.
func Callback(fn func(error)) RequestOption {
	return func(c *requestConfig, _ *http.Request) error {
		if fn == nil {
			return errors.New("nil callback")
		}
		c.callback = fn
		return nil
	}
}

// WithDefaultHeaders copies headers at construction. They apply only to endpoints
// on the base origin. Explicit request headers may override these defaults.
func WithDefaultHeaders(headers http.Header) ClientOption {
	snapshot := headers.Clone()
	return func(c *clientConfig) error { c.headers = snapshot.Clone(); return nil }
}
