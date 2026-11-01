package service

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/tidwall/gjson"
)

var errStreamSemanticIdle = errors.New("stream semantic idle timeout")

// newSemanticSSEScanner observes the original wire lines without rewriting them.
// The existing transport-idle timers remain independent. The semantic timer
// (600s by default, explicit 0 disables) ignores heartbeats, not total duration. Dedicated
// image and WebSocket pumps intentionally do not use this scanner.
//
// stop must be deferred by the owner, after arranging to stop any event sender.
// Closing the body unblocks an outstanding Scan on *every* return path, including
// terminal events, cancellation, and errors (not just watchdog expiration).
func newSemanticSSEScanner(resp *http.Response, cfg *config.Config, split ...bufio.SplitFunc) (*bufio.Scanner, func()) {
	ctx := context.Background()
	if resp.Request != nil {
		// Keep the upstream's context: a detached billing drain must not be
		// canceled merely because the downstream client disconnected.
		ctx = resp.Request.Context()
	}
	interval := time.Duration(0)
	if cfg != nil && cfg.Gateway.StreamSemanticIdleTimeout > 0 {
		interval = time.Duration(cfg.Gateway.StreamSemanticIdleTimeout) * time.Second
	}
	return newSemanticSSEScannerWithInterval(ctx, resp.Body, interval, split...)
}

func newSemanticSSEScannerWithInterval(ctx context.Context, body io.ReadCloser, interval time.Duration, split ...bufio.SplitFunc) (*bufio.Scanner, func()) {
	splitLine := bufio.ScanLines
	if len(split) > 0 && split[0] != nil {
		splitLine = split[0]
	}
	if interval <= 0 {
		scanner := bufio.NewScanner(body)
		scanner.Split(splitLine)
		return scanner, func() { _ = body.Close() }
	}
	watch := &semanticStreamWatchdog{
		body: body, interval: interval, remaining: interval,
		changed: make(chan struct{}, 1), done: make(chan struct{}), exited: make(chan struct{}),
	}
	go watch.run(ctx)
	scanner := bufio.NewScanner(watch)
	var observer semanticSSEObserver
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		advance, token, err := splitLine(data, atEOF)
		if token != nil && observer.observeLine(token) {
			watch.progress()
		}
		return advance, token, err
	})
	return scanner, watch.stop
}

func (s *AntigravityGatewayService) newAntigravitySSEScanner(resp *http.Response) (*bufio.Scanner, func()) {
	var cfg *config.Config
	if s.settingService != nil {
		cfg = s.settingService.cfg
	}
	return newSemanticSSEScanner(resp, cfg)
}

// Only time spent waiting inside upstream Read consumes the semantic budget.
// Buffered lines, downstream writes, and channel backpressure cannot turn this
// idle guard into a wall-clock deadline. All timer state is protected by mu;
// scanner observation and the timer goroutine never share unsynchronized fields.
type semanticStreamWatchdog struct {
	body      io.ReadCloser
	interval  time.Duration
	mu        sync.Mutex
	remaining time.Duration
	reading   bool
	started   time.Time
	err       error
	changed   chan struct{}
	done      chan struct{}
	exited    chan struct{}
	doneOnce  sync.Once
	closeOnce sync.Once
}

func (w *semanticStreamWatchdog) notify() {
	select {
	case w.changed <- struct{}{}:
	default:
	}
}

func (w *semanticStreamWatchdog) progress() {
	w.mu.Lock()
	w.remaining = w.interval
	w.mu.Unlock()
	w.notify()
}

func (w *semanticStreamWatchdog) Read(p []byte) (int, error) {
	w.mu.Lock()
	// Enforce an exhausted budget even if an upstream floods immediately
	// readable heartbeat bytes and the timer goroutine rarely observes Read.
	if w.err == nil && w.remaining <= 0 {
		w.err = errStreamSemanticIdle
	}
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		w.doneOnce.Do(func() { close(w.done) })
		w.closeBody()
		return 0, err
	}
	w.reading, w.started = true, time.Now()
	w.mu.Unlock()
	w.notify()

	n, err := w.body.Read(p)
	w.mu.Lock()
	w.remaining -= time.Since(w.started)
	w.reading = false
	cause := w.err
	w.mu.Unlock()
	w.notify()
	if cause != nil {
		return n, cause
	}
	if err != nil {
		w.doneOnce.Do(func() { close(w.done) })
	}
	return n, err
}

func (w *semanticStreamWatchdog) closeBody() {
	w.closeOnce.Do(func() { _ = w.body.Close() })
}

func (w *semanticStreamWatchdog) stop() {
	w.doneOnce.Do(func() { close(w.done) })
	w.closeBody()
	<-w.exited
}

func (w *semanticStreamWatchdog) run(ctx context.Context) {
	defer close(w.exited)
	timer := time.NewTimer(w.interval)
	defer timer.Stop()
	for {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		var timeout <-chan time.Time
		w.mu.Lock()
		remaining := w.remaining
		if w.reading {
			remaining -= time.Since(w.started)
			if remaining <= 0 {
				w.err = errStreamSemanticIdle
				w.mu.Unlock()
				w.closeBody()
				return
			}
			timer.Reset(remaining)
			timeout = timer.C
		}
		w.mu.Unlock()
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			w.mu.Lock()
			w.err = ctx.Err()
			w.mu.Unlock()
			w.closeBody()
			return
		case <-w.changed:
		case <-timeout:
		}
	}
}

// Multiline SSE data is inspected as one payload. Ordinary one-line JSON avoids
// an additional copy; malformed/opaque extensions are conservatively progress
// at the frame boundary. Limit inspection buffering without limiting wire data.
const semanticSSEInspectionLimit = 1024 * 1024

type semanticSSEObserver struct {
	eventType string
	pending   []byte
	opaque    bool
}

func (o *semanticSSEObserver) observeLine(line []byte) bool {
	line = bytes.TrimSuffix(line, []byte("\r"))
	if len(line) == 0 {
		progress := len(o.pending) > 0 && semanticSSEPayloadProgress(o.pending, o.eventType)
		o.eventType, o.pending, o.opaque = "", nil, false
		return progress
	}
	if bytes.HasPrefix(line, []byte("event:")) {
		o.eventType = strings.TrimSpace(string(line[len("event:"):]))
		return false
	}
	if !bytes.HasPrefix(line, []byte("data:")) || o.opaque || semanticHeartbeatType(o.eventType) {
		return false
	}
	payload := bytes.TrimSpace(line[len("data:"):])
	if len(payload) == 0 {
		return false
	}
	if len(o.pending) == 0 && len(payload) > semanticSSEInspectionLimit {
		// Large text/tool/image data is progress without duplicating the entire
		// payload for JSON inspection. A large ping is still only a heartbeat.
		return !semanticHeartbeatType(gjson.GetBytes(payload, "type").String())
	}
	if len(o.pending) == 0 && (bytes.Equal(payload, []byte("[DONE]")) || gjson.ValidBytes(payload)) {
		return semanticSSEPayloadProgress(payload, o.eventType)
	}
	if len(o.pending)+len(payload) > semanticSSEInspectionLimit {
		o.pending, o.opaque = nil, true
		return true
	}
	if len(o.pending) > 0 {
		o.pending = append(o.pending, '\n')
	}
	o.pending = append(o.pending, payload...)
	if gjson.ValidBytes(o.pending) {
		progress := semanticSSEPayloadProgress(o.pending, o.eventType)
		o.pending = nil
		return progress
	}
	return false
}

func semanticHeartbeatType(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "ping", "pong", "heartbeat", "keepalive", "keep-alive", "keep_alive":
		return true
	}
	return false
}

func semanticSSEPayloadProgress(payload []byte, eventType string) bool {
	if semanticHeartbeatType(eventType) {
		return false
	}
	if bytes.Equal(payload, []byte("[DONE]")) {
		return true // Observation only: the protocol consumer decides completion.
	}
	root := gjson.ParseBytes(payload)
	kind := root.Get("type").String()
	if kind == "" {
		kind = eventType
	}
	if semanticHeartbeatType(kind) {
		return false
	}
	if kind == "content_block_delta" || strings.HasSuffix(kind, ".delta") {
		return semanticDeltaPresent(root.Get("delta"))
	}
	if choices := root.Get("choices"); choices.IsArray() {
		for _, choice := range choices.Array() {
			if semanticDeltaPresent(choice.Get("delta")) || semanticDeltaPresent(choice.Get("text")) || semanticDeltaPresent(choice.Get("finish_reason")) {
				return true
			}
		}
		return semanticDeltaPresent(root.Get("usage"))
	}
	gemini := root
	if response := root.Get("response"); response.IsObject() {
		gemini = response
	}
	if candidates := gemini.Get("candidates"); candidates.IsArray() {
		for _, candidate := range candidates.Array() {
			if semanticDeltaPresent(candidate.Get("content.parts")) || semanticDeltaPresent(candidate.Get("finishReason")) {
				return true
			}
		}
		return false
	}
	// Lifecycle/usage/error events and future protocol extensions count as
	// progress. Never discard them or mistake a legal empty delta for EOF.
	return len(bytes.TrimSpace(payload)) > 0
}

func semanticDeltaPresent(value gjson.Result) bool {
	switch value.Type {
	case gjson.String:
		return value.Str != "" // Whitespace text is still model output.
	case gjson.Number:
		return value.Num != 0
	case gjson.True:
		return true
	case gjson.JSON:
		present := false
		value.ForEach(func(key, child gjson.Result) bool {
			if key.Type == gjson.String && (key.Str == "type" || key.Str == "index") {
				return true
			}
			present = semanticDeltaPresent(child)
			return !present
		})
		return present
	}
	return false
}
