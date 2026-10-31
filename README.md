# network

A Go HTTP and WebSocket client with explicit body consumers, bounded response reads, replay-aware retries, and optional logging. Requires Go 1.27.

```go
client, err := network.NewClient("https://api.example.com/v1",
    network.WithTimeout(30*time.Second),
    network.WithDefaultHeaders(http.Header{
        "Authorization": {"Bearer " + token},
    }),
)
if err != nil {
    return err
}
defer client.CloseIdleConnections()

var user User
var apiError APIError
var status int
err = client.Post(ctx, "users",
    network.JSONBody(input),
    network.BindJSON(&user),
    network.BindErrorJSON(&apiError),
    network.BindStatusCode(&status),
)
```

## HTTP requests

`Do(ctx, method, endpoint, options...)` is the core operation. `Get`, `Head`, `Post`, `Put`, `Patch`, and `Delete` delegate to it. All return `error`. Relative endpoint paths append to the base path; query parameters merge with endpoint values taking precedence. Absolute endpoints replace the base URL. Default headers only apply to the base origin; explicit per-request headers apply to the selected endpoint.

Options run in caller order. Scalar settings and headers use the last value. Two success-body consumers or two error-body consumers are rejected. Client options and request options are separate types so mutable binding destinations cannot accidentally become client defaults.

| Request option | Purpose |
| --- | --- |
| `Body(data)` | Snapshot bytes; replayable for retries and redirects |
| `JSONBody(value)` | Encode and snapshot JSON when creating the option |
| `ReaderBody(reader)` | Stream a single-use body, transferring ownership |
| `ReplayableBody(factory, size)` | Open an independent reader for each attempt; size `-1` is unknown |
| `BindJSON(&value)`, `BindXML(&value)` | Explicit decoding, independent of response Content-Type |
| `BindBody(&value)` | Decode by Content-Type, including `+json` and `+xml` |
| `BindError(&value)`, `BindErrorJSON(&value)` | Decode error responses while retaining the HTTP error |
| `BindBytes(&data)` | Buffer a response explicitly |
| `WriteBody(writer)` | Stream a response to a writer |
| `HandleResponse(handler)` | Custom synchronous consumption of successful responses |
| `BindHeader`, `BindCookies`, `BindStatusCode` | Metadata on successful and unsuccessful responses, including HEAD |
| `WithRequest(fn)` | Customize the request before execution |

The client closes every HTTP response. A custom handler must finish reading before returning and must not retain the body. Without a consumer, at most 64 KiB is drained before closing; this bounds unnecessary work, but larger discarded responses may prevent connection reuse. HEAD, 204, and 205 skip body decoding. Other empty bodies produce a decoding error when a decoder is requested. Decoders require one complete document and reject trailing JSON/XML documents. A destination implementing `Validate(context.Context) error` is validated after decoding.

Buffered, decoded, and streamed responses default to a 16 MiB limit. Override with `WithMaxResponseBody` or per-request `MaxResponseBody`; zero disables the limit. Writers and decoded destinations can contain partial data after failure. A custom handler that does not read the full body cannot detect whether unread content exceeds the limit. `WithDecoder(mediaType, decoder)` registers additional formats. `ParseJSON[T](data)` and `ParseXML[T](data)` provide standalone conversion helpers.

`WithHTTPClient` preserves custom transports, cookies, and redirect policy. Standard HTTP transports and TLS configurations are cloned at initialization, then reused. Custom transports are shared and must be concurrency-safe. Use `WithTLSConfig` after `WithHTTPClient` when overriding TLS. Client timeout defaults to 30 seconds across all attempts and backoff; an injected HTTP client's own timeout also applies to each attempt.

## Errors and retries

Use `errors.Is` with `ErrConfiguration`, `ErrTransport`, `ErrStatus`, `ErrDecode`, or `ErrBodyTooLarge`. Use `errors.As(err, &networkError)` with `var networkError *network.Error` for HTTP metadata. Status zero means no HTTP response was received. Underlying cancellation, transport, and decoding errors remain available through unwrapping. Non-2xx responses are HTTP errors, including redirects when automatic redirect following is disabled. An error response with malformed bound content matches both `ErrStatus` and `ErrDecode`.

Retries are disabled by default. Configure `WithRetryPolicy(network.RetryPolicy{...})`, or `Retries(n)` per request. Default retry eligibility requires an idempotent method and a replayable body, and permits transport failures or HTTP 408/429/500/502/503/504. Decoding failures and canceled contexts are never retried. A custom `Condition` may permit other methods/statuses; it cannot bypass replay or decoding restrictions. Enabling POST retries requires application-level duplicate handling.

Backoff doubles up to `MaxBackoff`; optional jitter spreads retry load. `Retry-After` is honored up to the same maximum. The caller's context and overall timeout bound waits. Metadata/error binding destinations describe the latest received response; if a subsequent attempt fails before receiving a response, inspect the returned error (status zero) rather than stale bound metadata.

## Logging

```go
client, err := network.NewClient(baseURL,
    network.WithLogHandler(func(ctx context.Context, event network.LogEvent) {
        logger.Log(ctx, event) // Adapt to your application's logger.
    }),
    network.WithLogging(network.Behaviours{
        LogSuccessfulTransactions: true,
        LogRequestHeadersOnError: true,
        LogResponseHeadersOnError: true,
        LogResponseBodyOnError: true,
    }),
    network.WithLogBodyLimit(4096),
)
```

No handler means no logging or body capture. With a handler, failures are emitted automatically; successful events require `LogSuccessfulTransactions` or an always-on field flag. Each field uses `always || (failed && onError)`. Events distinguish retry attempts, final completion, and WebSocket handshakes. Per-request `WithRequestLogging` overrides the client policy.

Bodies are opt-in and capture only the consumed prefix, bounded to 4096 bytes by default. Truncation flags report capture limits. Authorization, proxy authorization, cookies, Set-Cookie, and X-Api-Key headers are redacted. Logged URLs omit credentials, query strings, and fragments. Application body fields and arbitrary underlying errors may contain sensitive data; use the handler to apply application-specific redaction. Logging handlers and callbacks run synchronously and must be concurrency-safe and must not panic.

## Async and WebSockets

Every HTTP method has an `Async` counterpart. `Await()` returns the final error and permits multiple concurrent waiters. Read bound values only after completion. `AwaitContext(ctx)` cancels waiting without canceling the request; cancel the request's context to stop the work. `Callback(fn)` runs before completion is published and must not await its own transaction. A callback is installed when its option is applied; URL/construction errors that occur before options run cannot invoke it.

```go
conn, err := client.DialWebSocket(ctx, "/events", network.WebSocketOptions{
    Subprotocols: []string{"events.v1"},
    HandshakeTimeout: 10*time.Second,
    ReadLimit: 1 << 20,
})
if err != nil {
    return err
}
defer conn.Close(network.StatusNormalClosure, "done")
err = conn.Write(ctx, network.MessageText, []byte("hello"))
```

HTTP(S) and WS(S) URLs are supported. WebSockets share transport, default headers, cookie jar, and handshake logging. Handshake timeout falls back to the client timeout; it does not set a connection lifetime deadline. The default message read limit is 1 MiB. Message reads, writes, and pings take their own contexts. Keep a reader active to process ping/pong and close frames. Reconnection and message replay are application responsibilities. `WebSocket` aliases `coder/websocket.Conn`, so its streaming, concurrency, and close semantics apply directly. HTTP response consumers and retry policies do not apply to WebSockets.

## Migration from the extracted API

- `NewClient` now returns `(*Client, error)` and accepts client options.
- Request methods return `error`; use binding/streaming options for results.
- `Post`, `Put`, and `Patch` take bodies through options.
- `AwaitHeader` is replaced by `BindHeader` plus `Await`.
- `ResponseHandler` becomes `HandleResponse(func(*http.Response) error)`.
- Mutable `SetBehaviours` is replaced by explicit retry/logging configuration.
- Logging uses an injected handler; OpenTelemetry references and `EndSpan` are removed.
- Process-exiting Must helpers and obsolete error wrapper interfaces are removed.

## Verification

Run `./scripts/test.sh`. It runs the race detector, tests all packages, enforces at least **90% statement coverage**, and runs `go vet`. Tests use local loopback HTTP/TLS/WebSocket servers; no external service is required. Go may need to download the pinned WebSocket module on the first run.
