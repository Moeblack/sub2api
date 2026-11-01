package service

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestSemanticSSEObserver(t *testing.T) {
	for _, tt := range []struct {
		name     string
		frame    string
		progress bool
	}{
		{"comment", ": ping\n\n", false},
		{"blank", "\n\n", false},
		{"event header only", "event: content_block_delta\n\n", false},
		{"ping event", "event: ping\ndata: {}\n\n", false},
		{"ping JSON", "data:{\"type\":\"ping\"}\n\n", false},
		{"pong", "data: {\"type\":\"pong\"}\n\n", false},
		{"heartbeat", "data: {\"type\":\"heartbeat\"}\n\n", false},
		{"multiline ping", "data: {\n: a comment\ndata: \"type\":\"ping\"\ndata: }\n\n", false},
		{"empty text delta", `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":""}}` + "\n\n", false},
		{"empty thinking delta", `data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":""}}` + "\n\n", false},
		{"empty OpenAI delta", `data: {"type":"response.output_text.delta","delta":""}` + "\n\n", false},
		{"empty CC delta", `data: {"choices":[{"index":0,"delta":{},"finish_reason":null}]}` + "\n\n", false},
		{"empty Gemini delta", `data: {"response":{"candidates":[{"content":{"parts":[{"text":""}]}}]}}` + "\n\n", false},
		{"text", `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}` + "\n\n", true},
		{"whitespace text", `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":" "}}` + "\n\n", true},
		{"thinking", `data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"reasoning"}}` + "\n\n", true},
		{"thinking.delta", `data: {"type":"thinking.delta","delta":"reasoning"}` + "\n\n", true},
		{"signature", `data: {"type":"content_block_delta","delta":{"type":"signature_delta","signature":"abc"}}` + "\n\n", true},
		{"tool", `data: {"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{\"x\":"}}` + "\n\n", true},
		{"OpenAI reasoning", `data: {"type":"response.reasoning_summary_text.delta","delta":"reasoning"}` + "\n\n", true},
		{"OpenAI function", `data: {"type":"response.function_call_arguments.delta","delta":"{}"}` + "\n\n", true},
		{"CC tool", `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]}}]}` + "\n\n", true},
		{"CC role", `data: {"choices":[{"delta":{"role":"assistant","content":""}}]}` + "\n\n", true},
		{"Gemini thinking", `data: {"response":{"candidates":[{"content":{"parts":[{"thought":true,"text":"thinking"}]}}]}}` + "\n\n", true},
		{"Gemini tool", `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"lookup","args":{}}}]}}]}` + "\n\n", true},
		{"image extension", `data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"AA=="}` + "\n\n", true},
		{"lifecycle", `data: {"type":"message_stop"}` + "\n\n", true},
		{"done", "data: [DONE]\n\n", true},
		{"opaque extension", "data: future-wire-format\n\n", true},
		{"multiline text", "data: {\n data ignored\ndata: \"type\":\"thinking.delta\",\ndata: \"delta\":\"x\"}\n\n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var observer semanticSSEObserver
			progress := false
			for _, line := range strings.Split(tt.frame, "\n") {
				progress = observer.observeLine([]byte(line)) || progress
			}
			require.Equal(t, tt.progress, progress)
		})
	}
}

// Lifecycle assertions use owned channels and active Read counts, not a global
// goroutine count (which is noisy under -race and unrelated service workers).
type semanticTrackedBody struct {
	io.ReadCloser
	readStarted chan struct{}
	closed      chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	activeReads atomic.Int64
}

func (b *semanticTrackedBody) Read(p []byte) (int, error) {
	b.activeReads.Add(1)
	defer b.activeReads.Add(-1)
	b.startOnce.Do(func() { close(b.readStarted) })
	return b.ReadCloser.Read(p)
}

func (b *semanticTrackedBody) Close() error {
	var err error
	b.closeOnce.Do(func() {
		err = b.ReadCloser.Close()
		close(b.closed)
	})
	return err
}

func newSemanticTrackedPipe(t *testing.T) (*semanticTrackedBody, *io.PipeWriter) {
	t.Helper()
	reader, writer := io.Pipe()
	body := &semanticTrackedBody{ReadCloser: reader, readStarted: make(chan struct{}), closed: make(chan struct{})}
	t.Cleanup(func() {
		_ = body.Close()
		_ = writer.Close()
	})
	return body, writer
}

func waitSemanticSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("stream worker did not release its resource")
	}
}

func waitSemanticScan(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("stream scanner did not finish")
		return nil
	}
}

func consumeSemanticScanner(scanner *bufio.Scanner) <-chan error {
	done := make(chan error, 1)
	go func() {
		for scanner.Scan() {
		}
		done <- scanner.Err()
	}()
	return done
}

func writeSemanticHeartbeats(writer *io.PipeWriter, frame string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if _, err := io.WriteString(writer, frame); err != nil {
				return
			}
		}
	}()
	return done
}

func TestSemanticSSEScanner_HeartbeatsTimeout(t *testing.T) {
	for _, frame := range []string{
		": ping\n\n",
		"event: ping\ndata: {\"type\":\"ping\"}\n\n",
		"data: {\ndata: \"type\":\"ping\"\ndata: }\n\n",
		`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":""}}` + "\n\n",
	} {
		t.Run(frame, func(t *testing.T) {
			body, writer := newSemanticTrackedPipe(t)
			scanner, stop := newSemanticSSEScannerWithInterval(context.Background(), body, 100*time.Millisecond)
			defer stop()
			done := consumeSemanticScanner(scanner)
			writerDone := writeSemanticHeartbeats(writer, frame)
			require.ErrorIs(t, waitSemanticScan(t, done), errStreamSemanticIdle)
			stop() // Must be idempotent and join the timer goroutine.
			waitSemanticSignal(t, writerDone)
			waitSemanticSignal(t, body.closed)
			require.Zero(t, body.activeReads.Load())
		})
	}
}

func TestSemanticSSEScanner_ProgressIsNotTotalDeadline(t *testing.T) {
	for _, payload := range []string{
		`{"type":"content_block_delta","delta":{"type":"text_delta","text":" "}}`,
		`{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"still thinking"}}`,
		`{"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{"}}`,
		`{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
		`{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{"}}]}}]}`,
		`{"response":{"candidates":[{"content":{"parts":[{"thought":true,"text":"thinking"}]}}]}}`,
	} {
		t.Run(payload, func(t *testing.T) {
			body, writer := newSemanticTrackedPipe(t)
			scanner, stop := newSemanticSSEScannerWithInterval(context.Background(), body, 250*time.Millisecond)
			defer stop()
			done := consumeSemanticScanner(scanner)
			writerDone := make(chan struct{})
			go func() {
				defer close(writerDone)
				defer writer.Close()
				for range 16 {
					if _, err := fmt.Fprintf(writer, "data: %s\n\n: ping\n\n", payload); err != nil {
						return
					}
					time.Sleep(40 * time.Millisecond)
				}
			}()
			require.NoError(t, waitSemanticScan(t, done))
			stop()
			waitSemanticSignal(t, writerDone)
			require.Zero(t, body.activeReads.Load())
		})
	}
}

func TestSemanticSSEScanner_DownstreamBackpressurePausesBudget(t *testing.T) {
	body, writer := newSemanticTrackedPipe(t)
	scanner, stop := newSemanticSSEScannerWithInterval(context.Background(), body, 100*time.Millisecond)
	defer stop()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer writer.Close()
		_, _ = io.WriteString(writer, "data: {\"type\":\"thinking.delta\",\"delta\":\"first\"}\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"thinking.delta\",\"delta\":\"second\"}\n")
	}()
	require.True(t, scanner.Scan())
	time.Sleep(350 * time.Millisecond) // No upstream Read: model must not be penalized.
	select {
	case <-body.closed:
		t.Fatal("downstream processing time consumed the semantic budget")
	default:
	}
	require.True(t, scanner.Scan())
	require.Contains(t, scanner.Text(), "second")
	require.False(t, scanner.Scan())
	require.NoError(t, scanner.Err())
	stop()
	waitSemanticSignal(t, writerDone)
}

func TestSemanticSSEScanner_CancelEOFAndEarlyStopReleaseReaders(t *testing.T) {
	for _, mode := range []string{"cancel", "EOF", "early stop"} {
		t.Run(mode, func(t *testing.T) {
			body, writer := newSemanticTrackedPipe(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resp := &http.Response{Body: body, Request: httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)}
			scanner, stop := newSemanticSSEScanner(resp, &config.Config{Gateway: config.GatewayConfig{StreamSemanticIdleTimeout: 60}})
			defer stop()
			done := consumeSemanticScanner(scanner)
			waitSemanticSignal(t, body.readStarted)
			switch mode {
			case "cancel":
				cancel()
			case "EOF":
				_ = writer.Close()
			case "early stop":
				stop()
			}
			err := waitSemanticScan(t, done)
			if mode == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else if mode == "EOF" {
				require.NoError(t, err)
			}
			stop()
			waitSemanticSignal(t, body.closed)
			require.Zero(t, body.activeReads.Load())
		})
	}
}

func TestSemanticSSEScanner_DetachedDrainKeepsItsOwnContext(t *testing.T) {
	body, writer := newSemanticTrackedPipe(t)
	ctx, cancel := context.WithCancel(context.Background())
	scanner, stop := newSemanticSSEScannerWithInterval(context.WithoutCancel(ctx), body, 100*time.Millisecond)
	defer stop()
	done := consumeSemanticScanner(scanner)
	writerDone := writeSemanticHeartbeats(writer, ": ping\n\n")
	cancel()
	err := waitSemanticScan(t, done)
	require.ErrorIs(t, err, errStreamSemanticIdle)
	require.False(t, errors.Is(err, context.Canceled), "do not cancel detached usage drains with the client")
	stop()
	waitSemanticSignal(t, writerDone)
}

func TestSemanticSSEScanner_EmptyDeltaDoesNotCompleteOrRewriteStream(t *testing.T) {
	wire := "event: content_block_delta\n" + `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":""}}` + "\n\n" +
		"event: content_block_delta\n" + `data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"later"}}` + "\n\n"
	scanner, stop := newSemanticSSEScannerWithInterval(context.Background(), io.NopCloser(strings.NewReader(wire)), time.Second)
	defer stop()
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, wire, strings.Join(lines, "\n")+"\n")
}

func TestAnthropicNativeLinePump_SemanticHeartbeatTimeout(t *testing.T) {
	body, writer := newSemanticTrackedPipe(t)
	scanner, stop := newSemanticSSEScannerWithInterval(context.Background(), body, 100*time.Millisecond)
	defer stop()
	pump := newAnthropicNativeLinePump(scanner, time.Second)
	defer pump.stop()
	writerDone := writeSemanticHeartbeats(writer, "event: ping\ndata: {\"type\":\"ping\"}\n\n")
	done := make(chan error, 1)
	go func() {
		for {
			if _, err := pump.next(); err != nil {
				done <- err
				return
			}
		}
	}()
	require.ErrorIs(t, waitSemanticScan(t, done), errStreamSemanticIdle)
	stop()
	waitSemanticSignal(t, writerDone)
	require.Zero(t, body.activeReads.Load())
}
