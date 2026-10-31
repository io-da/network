package network

import (
	"context"
	"io"
	"net/http"
	"net/url"
)

// Query replaces only the supplied query keys and preserves repeated values.
// Values are snapshotted when creating the option; nil removes a key.
func Query(values url.Values) RequestOption {
	snapshot := make(url.Values, len(values))
	for key, items := range values {
		snapshot[key] = append([]string(nil), items...)
	}
	return WithRequest(func(r *http.Request) error {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			return err
		}
		for key, items := range snapshot {
			if len(items) == 0 {
				delete(query, key)
			} else {
				query[key] = items
			}
		}
		r.URL.RawQuery = query.Encode()
		return nil
	})
}
func QueryParam(key, value string) RequestOption { return Query(url.Values{key: {value}}) }

// FormBody encodes an application/x-www-form-urlencoded body and is replayable.
func FormBody(values url.Values) RequestOption {
	body := Body([]byte(values.Encode()))
	return func(c *requestConfig, r *http.Request) error {
		r.Header.Set("Content-Type", ContentTypeForm)
		return body(c, r)
	}
}

// WithCookieJar enables persistent cookies. Nil disables cookie persistence.
// Apply after WithHTTPClient if replacing its jar. The jar must be concurrent-safe.
func WithCookieJar(jar http.CookieJar) ClientOption {
	return func(c *clientConfig) error { c.httpClient.Jar = jar; return nil }
}

// WithProxy configures a standard transport. Nil disables proxies, including
// environment proxies. Apply after WithHTTPClient. Custom transports configure
// their own proxy behavior.
func WithProxy(proxy func(*http.Request) (*url.URL, error)) ClientOption {
	return func(c *clientConfig) error {
		tr, ok := c.httpClient.Transport.(*http.Transport)
		if !ok || tr == nil {
			return ErrConfiguration
		}
		cloned := tr.Clone()
		cloned.Proxy = proxy
		c.httpClient.Transport = cloned
		return nil
	}
}

// AcceptStatus replaces the default 2xx policy for one request. Useful for 304,
// conditional requests, or endpoints with a specific status contract.
func AcceptStatus(codes ...int) RequestOption {
	codes = append([]int(nil), codes...)
	return func(c *requestConfig, _ *http.Request) error {
		if len(codes) == 0 {
			return ErrConfiguration
		}
		statuses := make(map[int]bool, len(codes))
		for _, code := range codes {
			if code < 200 || code > 599 {
				return ErrConfiguration
			}
			statuses[code] = true
		}
		c.acceptedStatuses = statuses
		return nil
	}
}
func BindString(target *string) RequestOption {
	return func(c *requestConfig, r *http.Request) error {
		if err := validateTarget(target); err != nil {
			return err
		}
		return consumer(func(_ context.Context, res *http.Response) error {
			data, err := io.ReadAll(res.Body)
			if err == nil {
				*target = string(data)
			}
			return err
		}, false)(c, r)
	}
}

// BindErrorBytes captures a non-success body without interpreting its format.
func BindErrorBytes(target *[]byte) RequestOption {
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
		}, true)(c, r)
	}
}

// HandleErrorResponse consumes a non-success response without replacing its
// ErrStatus result. Body ownership is the same as HandleResponse.
func HandleErrorResponse(handler func(*http.Response) error) RequestOption {
	return func(c *requestConfig, r *http.Request) error {
		if handler == nil {
			return ErrConfiguration
		}
		return consumer(func(_ context.Context, res *http.Response) error { return handler(res) }, true)(c, r)
	}
}
