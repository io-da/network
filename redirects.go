package network

import (
	"errors"
	"net/http"
)

// WithFollowRedirects enables redirect following for HTTP and WebSocket
// handshakes. The default is false, even for an injected HTTP client.
func WithFollowRedirects(enabled bool) ClientOption {
	return func(c *clientConfig) error { c.followRedirects = enabled; return nil }
}

// FollowRedirects overrides redirect following for one HTTP request.
func FollowRedirects(enabled bool) RequestOption {
	return func(c *requestConfig, _ *http.Request) error { c.followRedirects = enabled; return nil }
}

// WithMaxRedirects sets the maximum redirect hops (default 10). Zero permits no
// hops. The limit applies even when an injected client has a custom checker.
func WithMaxRedirects(n int) ClientOption {
	return func(c *clientConfig) error {
		if n < 0 {
			return ErrConfiguration
		}
		c.maxRedirects = n
		return nil
	}
}
func (c *Client) requestClient(follow bool) http.Client {
	client := c.config.httpClient
	checker := client.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if !follow {
			return http.ErrUseLastResponse
		}
		if len(via) > c.config.maxRedirects {
			return ErrRedirectLimit
		}
		if checker != nil {
			if err := checker(req, via); err != nil {
				if err == http.ErrUseLastResponse {
					return err
				}
				return errors.Join(ErrRedirectPolicy, err)
			}
		}
		// net/http handles ordinary authentication headers. Also prevent API keys
		// and credentials from crossing origins (including host/port changes).
		if len(via) > 0 && !sameOrigin(via[0].URL, req.URL) {
			for key := range req.Header {
				if sensitiveHeader(key) {
					delete(req.Header, key)
				}
			}
		}
		return nil
	}
	return client
}
