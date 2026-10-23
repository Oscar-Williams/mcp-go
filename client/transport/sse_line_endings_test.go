package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sseLineEndingFrame terminates every field and the final blank line.
func sseLineEndingFrame(endings []string, fields ...string) string {
	var frame strings.Builder
	for i, field := range append(fields, "") {
		frame.WriteString(field)
		frame.WriteString(endings[i%len(endings)])
	}
	return frame.String()
}

// TestSSETransportsLineEndings exercises endpoint discovery and responses while
// the HTTP server keeps its event stream open after flushing the complete event.
func TestSSETransportsLineEndings(t *testing.T) {
	tests := []struct {
		name    string
		endings []string
	}{
		{name: "LF", endings: []string{"\n"}},
		{name: "CRLF", endings: []string{"\r\n"}},
		{name: "CR", endings: []string{"\r"}},
		{name: "mixed", endings: []string{"\n", "\r", "\r\n"}},
	}
	for _, legacy := range []bool{true, false} {
		name := "streamable_http"
		if legacy {
			name = "legacy_sse"
		}
		t.Run(name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					payload, err := json.Marshal(map[string]string{
						"text": strings.Repeat("data ", 16*1024) + "\u4e16\u754c",
					})
					require.NoError(t, err)
					responses := make(chan string, 1)
					writeFrame := func(w http.ResponseWriter, fields ...string) {
						if _, err := io.WriteString(w, sseLineEndingFrame(tt.endings, fields...)); err != nil {
							return
						}
						w.(http.Flusher).Flush()
						t.Logf("flushed complete SSE event (%d fields); keeping stream open", len(fields))
					}
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if legacy && r.Method == http.MethodGet {
							w.Header().Set("Content-Type", "text/event-stream")
							writeFrame(w, "event: endpoint", "data: /messages")
							for {
								select {
								case response := <-responses:
									writeFrame(w, "event: message", "data: "+response)
								case <-r.Context().Done():
									return
								}
							}
						}
						var request JSONRPCRequest
						if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
							http.Error(w, err.Error(), http.StatusBadRequest)
							return
						}
						response, err := json.Marshal(JSONRPCResponse{
							JSONRPC: "2.0", ID: request.ID, Result: payload,
						})
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						if legacy {
							responses <- string(response)
							w.WriteHeader(http.StatusAccepted)
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						writeFrame(w, "event: message", "data: "+string(response))
						<-r.Context().Done()
					}))
					t.Cleanup(server.Close)

					var transport Interface
					if legacy {
						transport, err = NewSSE(server.URL, WithEndpointTimeout(2*time.Second))
					} else {
						transport, err = NewStreamableHTTP(server.URL)
					}
					require.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, transport.Close()) })
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					require.NoError(t, transport.Start(ctx))
					request := JSONRPCRequest{JSONRPC: "2.0", ID: mcp.NewRequestId(int64(1)), Method: "ping"}
					response, err := transport.SendRequest(ctx, request)
					require.NoError(t, err)
					require.NotNil(t, response)
					assert.Equal(t, request.ID, response.ID)
					assert.JSONEq(t, string(payload), string(response.Result))
				})
			}
		})
	}
}

// TestReadSSELineEndingsAcrossReads verifies dispatch at CR before EOF or another
// byte arrives, and ignores the optional LF even when it arrives in a later read.
func TestReadSSELineEndingsAcrossReads(t *testing.T) {
	client, err := NewStreamableHTTP("http://localhost")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	type event struct{ name, data string }
	events := make(chan event, 3)
	done := make(chan bool, 1)
	var cursor sseCursor
	go func() {
		done <- client.readSSEWithCursor(ctx, reader, &cursor, func(name, data string) {
			events <- event{name, data}
		})
	}()
	write := func(chunk string) {
		_, err := io.WriteString(writer, chunk)
		require.NoError(t, err)
	}
	read := func(want event) {
		select {
		case got := <-events:
			assert.Equal(t, want, got)
		case <-time.After(5 * time.Second):
			t.Fatal("complete CR-delimited event was not dispatched while the stream remained open")
		}
	}
	write("id: first\rretry: 17\revent: custom\rdata: first\r\r")
	read(event{"custom", "first"})
	write("\nid: second\r")
	write("\nevent: custom\r")
	write("\ndata: second\r")
	write("\n\r")
	read(event{"custom", "second"})
	write("\n")
	require.NoError(t, writer.Close())
	select {
	case ended := <-done:
		assert.True(t, ended)
	case <-time.After(5 * time.Second):
		t.Fatal("SSE reader did not exit after EOF")
	}
	assert.Empty(t, events)
	assert.Equal(t, "second", cursor.lastEventID)
	assert.True(t, cursor.hasRetry)
	assert.Equal(t, 17*time.Millisecond, cursor.retry)
}
