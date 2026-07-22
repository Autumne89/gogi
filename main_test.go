package gogi

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionReuseOnContextCancellation(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("large payload or delayed body"))
	})

	server := httptest.NewTLSServer(handler)
	defer server.Close()

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	tr.ForceAttemptHTTP2 = true

	client := NewClient(&http.Client{
		Transport: tr,
	})

	const concurrency = 100
	var wg sync.WaitGroup
	var connCount int32

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithCancel(context.Background())
			trace := &httptrace.ClientTrace{
				GotConn: func(info httptrace.GotConnInfo) {
					if !info.Reused {
						atomic.AddInt32(&connCount, 1)
					}
				},
			}
			ctx = httptrace.WithClientTrace(ctx, trace)

			req, err := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
			if err != nil {
				t.Errorf("Failed to create request: %v", err)
				cancel()
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				cancel()
				return
			}

			if resp.ProtoMajor != 2 {
				t.Errorf("Expected HTTP/2, got %s", resp.Proto)
			}

			cancel()
			resp.Body.Close()
		}()
	}

	wg.Wait()

	finalConnCount := atomic.LoadInt32(&connCount)
	t.Logf("Total connections created: %d", finalConnCount)
	if finalConnCount > 5 {
		t.Errorf("Expected connection reuse, but created %d connections for %d requests", finalConnCount, concurrency)
	}
}
