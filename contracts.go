package network

import (
	"context"
	"net/http"
)

// ClientContract is the HTTP request API. Each call owns and closes its response.
// Binding destinations must not be shared by concurrent requests.
type ClientContract interface {
	Do(context.Context, string, string, ...RequestOption) error
	Get(context.Context, string, ...RequestOption) error
	Head(context.Context, string, ...RequestOption) error
	Post(context.Context, string, ...RequestOption) error
	Put(context.Context, string, ...RequestOption) error
	Patch(context.Context, string, ...RequestOption) error
	Delete(context.Context, string, ...RequestOption) error
	Options(context.Context, string, ...RequestOption) error
	DoAsync(context.Context, string, string, ...RequestOption) *AsyncTransaction
	GetAsync(context.Context, string, ...RequestOption) *AsyncTransaction
	HeadAsync(context.Context, string, ...RequestOption) *AsyncTransaction
	PostAsync(context.Context, string, ...RequestOption) *AsyncTransaction
	PutAsync(context.Context, string, ...RequestOption) *AsyncTransaction
	PatchAsync(context.Context, string, ...RequestOption) *AsyncTransaction
	DeleteAsync(context.Context, string, ...RequestOption) *AsyncTransaction
	OptionsAsync(context.Context, string, ...RequestOption) *AsyncTransaction
}

// TransactionContract is a read-only view for retry policies. Do not retain or
// mutate the request or response returned by these methods.
type TransactionContract interface {
	Context() context.Context
	Request() *http.Request
	Response() *http.Response
	Retry() int
}

type NetworkErrorContract interface {
	error
	StatusCode() int
	ErrorCode() string
	Unwrap() error
}

// SecondPassValidationContract is optionally implemented by decoded destinations.
type SecondPassValidationContract interface{ Validate(context.Context) error }

var _ ClientContract = (*Client)(nil)
var _ NetworkErrorContract = (*Error)(nil)

// AsyncTransactionContract permits multiple concurrent waiters.
type AsyncTransactionContract interface {
	Await() error
	AwaitContext(context.Context) error
}

// WebSocketClientContract separates persistent connections from HTTP requests.
type WebSocketClientContract interface {
	DialWebSocket(context.Context, string, WebSocketOptions) (*WebSocket, error)
}

var _ AsyncTransactionContract = (*AsyncTransaction)(nil)
var _ WebSocketClientContract = (*Client)(nil)
