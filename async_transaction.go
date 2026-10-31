package network

import "context"

// AsyncTransaction broadcasts completion to any number of waiters. Bound values
// may be read after Await returns. AwaitContext cancellation only stops waiting.
type AsyncTransaction struct {
	done chan struct{}
	err  error
}

func (a *AsyncTransaction) Await() error { <-a.done; return a.err }
func (a *AsyncTransaction) AwaitContext(ctx context.Context) error {
	select {
	case <-a.done:
		return a.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *Client) DoAsync(ctx context.Context, method, endpoint string, opts ...RequestOption) *AsyncTransaction {
	a := &AsyncTransaction{done: make(chan struct{})}
	opts = append([]RequestOption(nil), opts...)
	go func() { a.err = c.Do(ctx, method, endpoint, opts...); close(a.done) }()
	return a
}
