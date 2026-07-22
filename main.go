package gogi

import (
	"context"
	"io"
	"net/http"
	"sync"
)

// Transport is a custom RoundTripper that wraps an underlying RoundTripper
// and ensures that response bodies are properly closed and drained on context cancellation
// to prevent HTTP/2 connection leaks.
type Transport struct {
	Transport http.RoundTripper
}

// RoundTrip implements the http.RoundTripper interface.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := t.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}

	resp, err := rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Wrap the response body to monitor context cancellation
	resp.Body = &watchedBody{
		ReadCloser: resp.Body,
		ctx:        req.Context(),
		done:       make(chan struct{}),
	}

	// Start a goroutine to watch for context cancellation
	go resp.Body.(*watchedBody).watch()

	return resp, nil
}

type watchedBody struct {
	io.ReadCloser
	ctx  context.Context
	done chan struct{ }
	once sync.Once
}

func (wb *watchedBody) watch() {
	select {
	case <-wb.ctx.Done():
		// Context was cancelled or timed out. Close the body to release resources.
		wb.Close()
	case <-wb.done:
		// Body was closed normally.
	}
}

func (wb *watchedBody) Read(p []byte) (n int, err error) {
	n, err = wb.ReadCloser.Read(p)
	if err != nil {
		// If we hit EOF or any other error, close the body to clean up
		wb.Close()
	}
	return n, err
}

func (wb *watchedBody) Close() error {
	var err error
	wb.once.Do(func() {
		close(wb.done)
		// Drain the body (up to a limit) to ensure HTTP/2 stream is recycled.
		// This is crucial for HTTP/2 connection reuse.
		io.Copy(io.Discard, io.LimitReader(wb.ReadCloser, 4096))
		err = wb.ReadCloser.Close()
	})
	return err
}

// NewClient returns a new http.Client configured with the custom Transport.
func NewClient(client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{}
	}
	client.Transport = &Transport{Transport: client.Transport}
	return client
}
