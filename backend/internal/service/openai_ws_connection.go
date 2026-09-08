package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	openAIWSMaxActiveResponses = 16
	openAIWSMaxNamedStreams    = 32
	openAIWSMaxQueuedRequests  = 64
	openAIWSMaxQueuedBytes     = 64 << 20
	openAIWSMaxHistoryBytes    = 256 << 20
)

type openAIWSConnectionScopeKey struct{}
type openAIWSBridgeExecutionKey struct{}

type openAIWSConnectionScope struct {
	connectionID string
	streamID     string
	startedAt    time.Time
}

// WithOpenAIWSConnectionScope separates protocol state from client routing hints.
// The connection ID must be server-generated, never taken from request headers.
func WithOpenAIWSConnectionScope(ctx context.Context, connectionID, streamID string) context.Context {
	startedAt := time.Now()
	if scope, ok := ctx.Value(openAIWSConnectionScopeKey{}).(openAIWSConnectionScope); ok && scope.connectionID == connectionID {
		startedAt = scope.startedAt
	}
	return context.WithValue(ctx, openAIWSConnectionScopeKey{}, openAIWSConnectionScope{
		connectionID: connectionID, streamID: streamID, startedAt: startedAt,
	})
}

func openAIWSProtocolSessionHash(ctx context.Context, fallback string) string {
	if scope, ok := ctx.Value(openAIWSConnectionScopeKey{}).(openAIWSConnectionScope); ok {
		return "conn-v2:" + scope.connectionID + ":" + scope.streamID
	}
	return fallback
}

func openAIWSProtocolResponseKey(ctx context.Context, responseID string) string {
	if scope, ok := ctx.Value(openAIWSConnectionScopeKey{}).(openAIWSConnectionScope); ok {
		return "conn-v2:" + scope.connectionID + ":" + scope.streamID + ":" + responseID
	}
	return responseID
}

// IsOpenAIWSBridgeExecution identifies HTTP execution behind a WS connection.
// It does not change authentication, quotas, or the account scheduling policy.
func IsOpenAIWSBridgeExecution(ctx context.Context) bool {
	value, _ := ctx.Value(openAIWSBridgeExecutionKey{}).(bool)
	return value
}

// OpenAIWSConnectionRequest is owned by one execution. Its byte slices and
// historical items are immutable and must not be retained by unrelated requests.
type OpenAIWSConnectionRequest struct {
	ConnectionID string
	StreamID     string
	Sequence     int
	RawPayload   []byte
	Payload      []byte
}

type OpenAIWSRequestExecutor func(context.Context, OpenAIWSConnectionRequest, func([]byte) error) error

type OpenAIWSConnectionOptions struct {
	FirstMessageTimeout time.Duration
	IdleTimeout         time.Duration
	Lifetime            time.Duration
	WriteTimeout        time.Duration
	PingInterval        time.Duration
	PingTimeout         time.Duration
	MaxActive           int
	MaxNamedStreams     int
	MaxQueuedRequests   int
	MaxQueuedBytes      int64
	// MaxHistoryBytes bounds retained snapshots per connection and, independently,
	// the sum of materialized inputs held by in-flight executions.
	MaxHistoryBytes int64
	// Persisted resolution must enforce the authenticated response owner. A
	// missing callback means that only this connection's snapshots are usable.
	ResolvePersisted func(context.Context, string) bool
	// NormalizeRequest resolves adapter-specific standalone inputs before an
	// uncovered tool output is rejected. A changed input is also used for
	// snapshots. The callback must not mutate its input or request controls.
	NormalizeRequest func([]byte) ([]byte, bool)
	// ValidateWarmup is an optional admission hook for generate:false warmup
	// checkpoints. It runs after the payload is resolved/prepared and before
	// any checkpoint is published or warmup event is written. A nil return
	// approves the checkpoint; an *OpenAIWSRequestError is delivered to the
	// lane as-is; any other error becomes a 400 warmup_rejected
	// invalid_request_error. The warmup performs no inference, tool calls,
	// execution lease, or billing regardless of the outcome.
	ValidateWarmup func(context.Context, OpenAIWSConnectionRequest) error
	// ObserveRequestError reports runtime-generated request errors that never
	// reach the executor: frame validation, queue admission, protocol
	// preparation, and warmup admission. Executor failures are already
	// observable through the execution path and are not reported here. The
	// callback runs synchronously on the connection loop before the error
	// event is written and must not block. The request is best-effort:
	// ConnectionID is always set; StreamID/Sequence/RawPayload are set when
	// known; Payload is set only after successful preparation.
	ObserveRequestError func(OpenAIWSConnectionRequest, *OpenAIWSRequestError)
	ObserveClosed       func(OpenAIWSConnectionOutcome)
}

type OpenAIWSConnectionOutcome struct {
	ConnectionID string
	Cause        string
	CloseCode    int
	CloseError   error
	Requests     int
}

type OpenAIWSRequestError struct {
	Status  int
	Code    string
	Message string
	Param   string
}

func (e *OpenAIWSRequestError) Error() string { return e.Message }

func newOpenAIWSRequestError(status int, code, message, param string) *OpenAIWSRequestError {
	return &OpenAIWSRequestError{Status: status, Code: code, Message: message, Param: param}
}

func openAIWSRequestErrorEvent(err *OpenAIWSRequestError, streamID string) []byte {
	errorType := "invalid_request_error"
	if err.Status == 429 {
		errorType = "rate_limit_error"
	} else if err.Status >= 500 {
		errorType = "server_error"
	}
	detail := map[string]any{"type": errorType, "code": err.Code, "message": err.Message}
	if err.Param != "" {
		detail["param"] = err.Param
	}
	event := map[string]any{"type": "error", "status": err.Status, "error": detail}
	if streamID != "" {
		event["stream_id"] = streamID
	}
	encoded, _ := json.Marshal(event)
	return encoded
}

type openAIWSQueuedRequest struct {
	raw      []byte
	streamID string
	sequence int
}

// openAIWSResponseSnapshot is the connection-local checkpoint pinned as one
// lane's latest completed response. items are the complete ordered replay
// history (materialized input plus collected output items); the byte bodies
// are immutable and shared under the replay ownership invariant.
type openAIWSResponseSnapshot struct {
	id         string
	streamID   string
	model      string
	items      []json.RawMessage
	inputKnown bool
	bytes      int64
}

type openAIWSConnectionLane struct {
	id      string
	model   string
	running bool
	queue   []openAIWSQueuedRequest
}

type openAIWSExecutionCompletion struct {
	request       openAIWSQueuedRequest
	reservedBytes int64
}

type openAIWSConnectionWrite struct {
	ctx     context.Context
	payload []byte
	result  chan error
}

// openAIWSConnectionCache holds the connection-local snapshot state shared by
// the connection loop and in-flight executions. Publication and invalidation
// happen inside an execution before its terminal frame becomes observable, so
// a client that reacts to a terminal event on any lane always sees a coherent
// checkpoint decision; the mutex only serializes those short critical sections.
type openAIWSConnectionCache struct {
	mu            sync.Mutex
	snapshots     map[string]*openAIWSResponseSnapshot
	laneLatest    map[string]string
	retainedBytes int64
	activeBytes   int64
	maxBytes      int64
}

func newOpenAIWSConnectionCache(maxBytes int64) *openAIWSConnectionCache {
	return &openAIWSConnectionCache{
		snapshots:  make(map[string]*openAIWSResponseSnapshot),
		laneLatest: make(map[string]string),
		maxBytes:   maxBytes,
	}
}

func (c *openAIWSConnectionCache) lookup(id string) *openAIWSResponseSnapshot {
	if id == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshots[id]
}

// publish pins snapshot as the lane's latest checkpoint, evicting the lane's
// previous one (a new chain or an advanced turn explicitly resets the lane).
// A snapshot that does not fit the retained budget is not cached: the lane
// still advances, and continuations fall back to previous_response_not_found
// or full-input recovery instead of evicting another lane's pinned checkpoint.
func (c *openAIWSConnectionCache) publish(laneID string, snapshot *openAIWSResponseSnapshot) {
	if snapshot == nil || snapshot.id == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if oldID := c.laneLatest[laneID]; oldID != "" && oldID != snapshot.id {
		if old := c.snapshots[oldID]; old != nil {
			c.retainedBytes -= old.bytes
			delete(c.snapshots, oldID)
		}
	}
	delete(c.laneLatest, laneID)
	if snapshot.bytes > c.maxBytes-c.retainedBytes {
		return
	}
	if existing := c.snapshots[snapshot.id]; existing != nil {
		c.retainedBytes -= existing.bytes
	}
	c.snapshots[snapshot.id] = snapshot
	c.retainedBytes += snapshot.bytes
	c.laneLatest[laneID] = snapshot.id
}

// invalidate evicts a checkpoint, clearing any lane that still pins it.
func (c *openAIWSConnectionCache) invalidate(id string) {
	if id == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.snapshots[id]; old != nil {
		c.retainedBytes -= old.bytes
		delete(c.snapshots, id)
	}
	for laneID, latestID := range c.laneLatest {
		if latestID == id {
			delete(c.laneLatest, laneID)
		}
	}
}

// reserveActive charges bytes against the connection's active execution
// memory budget: materialized inputs plus collected output items shared by all
// in-flight executions on this connection.
func (c *openAIWSConnectionCache) reserveActive(bytes int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bytes > c.maxBytes-c.activeBytes {
		return false
	}
	c.activeBytes += bytes
	return true
}

func (c *openAIWSConnectionCache) releaseActive(bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activeBytes -= bytes
	if c.activeBytes < 0 {
		c.activeBytes = 0
	}
}

// advance evicts the lane's pinned checkpoint without pinning a replacement:
// the lane completed a turn whose checkpoint is unavailable.
func (c *openAIWSConnectionCache) advance(laneID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if oldID := c.laneLatest[laneID]; oldID != "" {
		if old := c.snapshots[oldID]; old != nil {
			c.retainedBytes -= old.bytes
			delete(c.snapshots, oldID)
		}
		delete(c.laneLatest, laneID)
	}
}

// fits reports whether bytes could be retained for laneID, accounting for the
// lane checkpoint that publication would evict.
func (c *openAIWSConnectionCache) fits(laneID string, bytes int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	retained := c.retainedBytes
	if latestID := c.laneLatest[laneID]; latestID != "" {
		if old := c.snapshots[latestID]; old != nil {
			retained -= old.bytes
		}
	}
	return bytes <= c.maxBytes-retained
}

func normalizeOpenAIWSConnectionOptions(options OpenAIWSConnectionOptions) OpenAIWSConnectionOptions {
	if options.FirstMessageTimeout <= 0 {
		options.FirstMessageTimeout = 15 * time.Second
	}
	if options.Lifetime <= 0 || options.Lifetime > time.Hour {
		options.Lifetime = time.Hour
	}
	if options.WriteTimeout <= 0 {
		options.WriteTimeout = 15 * time.Second
	}
	if options.PingInterval == 0 {
		options.PingInterval = 20 * time.Second
	}
	if options.PingTimeout <= 0 {
		options.PingTimeout = 10 * time.Second
	}
	if options.MaxActive <= 0 || options.MaxActive > openAIWSMaxActiveResponses {
		options.MaxActive = openAIWSMaxActiveResponses
	}
	if options.MaxNamedStreams <= 0 || options.MaxNamedStreams > openAIWSMaxNamedStreams {
		options.MaxNamedStreams = openAIWSMaxNamedStreams
	}
	if options.MaxQueuedRequests <= 0 {
		options.MaxQueuedRequests = openAIWSMaxQueuedRequests
	}
	if options.MaxQueuedBytes <= 0 {
		options.MaxQueuedBytes = openAIWSMaxQueuedBytes
	}
	if options.MaxHistoryBytes <= 0 {
		options.MaxHistoryBytes = openAIWSMaxHistoryBytes
	}
	return options
}

// openAIWSStreamID validates the lane name. Only an omitted field selects the
// default lane: the schema is nullable:false, so an explicit null, an empty
// string, and any non-string value are all invalid_stream_id.
func openAIWSStreamID(payload []byte) (string, error) {
	value := gjson.GetBytes(payload, "stream_id")
	if !value.Exists() {
		return "", nil
	}
	id := value.String()
	if value.Type != gjson.String || len(id) == 0 || len(id) > 256 {
		return "", newOpenAIWSRequestError(400, "invalid_stream_id", "The 'stream_id' field must be a non-empty string with at most 256 characters and may only contain letters, numbers, underscores, hyphens, and periods.", "stream_id")
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return "", newOpenAIWSRequestError(400, "invalid_stream_id", "The 'stream_id' field must be a non-empty string with at most 256 characters and may only contain letters, numbers, underscores, hyphens, and periods.", "stream_id")
		}
	}
	return id, nil
}

func openAIWSHistoryBytes(items []json.RawMessage) int64 {
	var total int64
	for _, item := range items {
		total += int64(len(item))
	}
	return total
}

// openAIWSConnectionRuntime owns one physical WebSocket. Neither session-id,
// thread-id, nor prompt_cache_key is used to cancel or locate another socket:
// two physical connections never share runtime state. One reader stays active
// while isolated executions run, and all writes are serialized on a single
// pump independently of an individual execution's cancellation context.
type openAIWSConnectionRuntime struct {
	conn         *coderws.Conn
	parent       context.Context
	firstMessage []byte
	options      OpenAIWSConnectionOptions
	execute      OpenAIWSRequestExecutor
	connectionID string
	ctx          context.Context
	cancel       context.CancelCauseFunc
	outcome      OpenAIWSConnectionOutcome

	// Scheduler state below is owned by the connection loop goroutine only.
	lanes       map[string]*openAIWSConnectionLane
	laneOrder   []string
	named       int
	cursor      int
	queued      int
	queuedBytes int64
	active      int

	cache *openAIWSConnectionCache

	reads        chan openAIWSClientReadResult
	writes       chan openAIWSConnectionWrite
	writeFailure chan error
	completed    chan openAIWSExecutionCompletion
	pingResults  chan error

	pumps      sync.WaitGroup
	executions sync.WaitGroup
}

// RunOpenAIWSConnection runs the per-socket runtime until the connection
// closes, the lifetime elapses, or the parent context is canceled.
func RunOpenAIWSConnection(
	parent context.Context,
	conn *coderws.Conn,
	firstMessage []byte,
	options OpenAIWSConnectionOptions,
	execute OpenAIWSRequestExecutor,
) error {
	if conn == nil || execute == nil {
		return errors.New("websocket connection and executor are required")
	}
	if parent == nil {
		parent = context.Background()
	}
	options = normalizeOpenAIWSConnectionOptions(options)
	lifetimeCtx, stopLifetime := context.WithTimeoutCause(parent, options.Lifetime, errOpenAIWSIngressLifetimeExceeded)
	defer stopLifetime()
	ctx, cancel := context.WithCancelCause(lifetimeCtx)
	connectionID := uuid.NewString()
	if scope, ok := parent.Value(openAIWSConnectionScopeKey{}).(openAIWSConnectionScope); ok {
		connectionID = scope.connectionID
	}
	runtime := &openAIWSConnectionRuntime{
		conn:         conn,
		parent:       parent,
		firstMessage: firstMessage,
		options:      options,
		execute:      execute,
		connectionID: connectionID,
		ctx:          ctx,
		cancel:       cancel,
		outcome: OpenAIWSConnectionOutcome{
			ConnectionID: connectionID,
			Cause:        "client_close",
			CloseCode:    int(coderws.StatusNormalClosure),
		},
		lanes:        make(map[string]*openAIWSConnectionLane),
		cache:        newOpenAIWSConnectionCache(options.MaxHistoryBytes),
		reads:        make(chan openAIWSClientReadResult, 1),
		writes:       make(chan openAIWSConnectionWrite),
		writeFailure: make(chan error, 1),
		completed:    make(chan openAIWSExecutionCompletion, options.MaxActive),
		pingResults:  make(chan error, 1),
	}
	err := runtime.run()
	if errors.Is(context.Cause(ctx), errOpenAIWSIngressLifetimeExceeded) {
		return nil
	}
	return err
}

func (r *openAIWSConnectionRuntime) run() error {
	r.pumps.Add(2)
	go r.readPump()
	go r.writePump()
	defer r.teardown()

	var firstTimer *time.Timer
	var firstTimeout <-chan time.Time
	if r.firstMessage == nil {
		firstTimer = time.NewTimer(r.options.FirstMessageTimeout)
		firstTimeout = firstTimer.C
		defer firstTimer.Stop()
	}

	var idleTimer *time.Timer
	var idle <-chan time.Time
	resetIdle := func() {
		if idleTimer != nil {
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
		}
		idle = nil
		if r.options.IdleTimeout > 0 && r.active == 0 && r.queued == 0 && firstTimeout == nil {
			if idleTimer == nil {
				idleTimer = time.NewTimer(r.options.IdleTimeout)
			} else {
				idleTimer.Reset(r.options.IdleTimeout)
			}
			idle = idleTimer.C
		}
	}
	defer func() {
		if idleTimer != nil {
			idleTimer.Stop()
		}
	}()

	var pingTicker *time.Ticker
	var ping <-chan time.Time
	if r.options.PingInterval > 0 {
		pingTicker = time.NewTicker(r.options.PingInterval)
		ping = pingTicker.C
		defer pingTicker.Stop()
	}
	pingRunning := false

	if r.firstMessage != nil {
		if err := r.accept(r.firstMessage); err != nil {
			r.outcome.Cause = "write_error"
			r.outcome.CloseCode = int(coderws.StatusInternalError)
			return err
		}
		resetIdle()
	}

	for {
		select {
		case frame := <-r.reads:
			if frame.err != nil {
				if code := coderws.CloseStatus(frame.err); code != -1 {
					r.outcome.Cause = "client_close"
					if code == coderws.StatusNormalClosure || code == coderws.StatusGoingAway {
						r.outcome.CloseCode = int(code)
					}
					return nil
				}
				r.outcome.Cause = "network_read_error"
				r.outcome.CloseCode = int(coderws.StatusGoingAway)
				return frame.err
			}
			if firstTimer != nil {
				firstTimer.Stop()
				firstTimeout = nil
			}
			if err := r.accept(frame.payload); err != nil {
				r.outcome.Cause = "write_error"
				r.outcome.CloseCode = int(coderws.StatusInternalError)
				return err
			}
			resetIdle()
		case completion := <-r.completed:
			if err := r.handleCompletion(completion); err != nil {
				r.outcome.Cause = "write_error"
				r.outcome.CloseCode = int(coderws.StatusInternalError)
				return err
			}
			resetIdle()
		case err := <-r.writeFailure:
			r.outcome.Cause = "network_write_error"
			r.outcome.CloseCode = int(coderws.StatusGoingAway)
			return err
		case <-firstTimeout:
			r.outcome.Cause = "first_message_timeout"
			r.outcome.CloseCode = int(coderws.StatusPolicyViolation)
			return context.DeadlineExceeded
		case <-idle:
			r.outcome.Cause = "idle_timeout"
			return nil
		case <-ping:
			if !pingRunning {
				pingRunning = true
				r.pumps.Add(1)
				go func() {
					defer r.pumps.Done()
					pingCtx, stopPing := context.WithTimeout(r.ctx, r.options.PingTimeout)
					err := r.conn.Ping(pingCtx)
					stopPing()
					select {
					case r.pingResults <- err:
					case <-r.ctx.Done():
					}
				}()
			}
		case err := <-r.pingResults:
			pingRunning = false
			if err != nil {
				r.outcome.Cause = "heartbeat_timeout"
				r.outcome.CloseCode = int(coderws.StatusGoingAway)
				return err
			}
		case <-r.ctx.Done():
			r.outcome.Cause = "connection_canceled"
			r.outcome.CloseCode = int(coderws.StatusGoingAway)
			if errors.Is(context.Cause(r.ctx), ErrOpenAIWSIngressLeaseLost) {
				r.outcome.Cause = "ingress_lease_lost"
				r.outcome.CloseCode = int(coderws.StatusTryAgainLater)
			}
			var controlledClose *OpenAIWSClientCloseError
			if errors.As(context.Cause(r.ctx), &controlledClose) {
				switch code := controlledClose.StatusCode(); code {
				case coderws.StatusNormalClosure, coderws.StatusGoingAway, coderws.StatusPolicyViolation, coderws.StatusInternalError, coderws.StatusTryAgainLater:
					r.outcome.CloseCode = int(code)
				}
			}
			return context.Cause(r.ctx)
		}
	}
}

// teardown cancels all execution contexts, performs the bounded close
// handshake while the reader is still alive to receive the peer's close frame
// (CloseNow is only the cleanup fallback), then joins executions and pumps.
// The close code is always a valid wire code; 1006 is never sent.
func (r *openAIWSConnectionRuntime) teardown() {
	if errors.Is(context.Cause(r.ctx), errOpenAIWSIngressLifetimeExceeded) {
		r.outcome.Cause = "connection_lifetime"
		r.outcome.CloseCode = int(coderws.StatusGoingAway)
		// The execution deadline has already fired. A detached, bounded final
		// control write still lets clients observe the documented limit event.
		writeCtx, stopWrite := context.WithTimeout(context.WithoutCancel(r.parent), r.options.WriteTimeout)
		_ = r.conn.Write(writeCtx, coderws.MessageText, openAIWSRequestErrorEvent(newOpenAIWSRequestError(400, "websocket_connection_limit_reached", "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue.", ""), ""))
		stopWrite()
	} else if errors.Is(context.Cause(r.ctx), ErrOpenAIWSIngressLeaseLost) {
		r.outcome.Cause = "ingress_lease_lost"
		r.outcome.CloseCode = int(coderws.StatusTryAgainLater)
	} else if r.parent.Err() != nil {
		r.outcome.Cause = "connection_canceled"
		r.outcome.CloseCode = int(coderws.StatusGoingAway)
	}
	r.cancel(errors.New(r.outcome.Cause))
	r.outcome.CloseError = r.conn.Close(coderws.StatusCode(r.outcome.CloseCode), r.outcome.Cause)
	_ = r.conn.CloseNow()
	r.executions.Wait()
	r.pumps.Wait()
	// A timed-out frame write closes coder/websocket's transport, waking the
	// reader too. Attribute that race to the confirmed write deadline rather
	// than whichever error channel happened to win the connection loop.
	if r.outcome.Cause == "network_read_error" {
		select {
		case writeErr := <-r.writeFailure:
			if errors.Is(writeErr, context.DeadlineExceeded) {
				r.outcome.Cause = "network_write_error"
			}
		default:
		}
	}
	if r.options.ObserveClosed != nil {
		r.options.ObserveClosed(r.outcome)
	}
}

func (r *openAIWSConnectionRuntime) readPump() {
	defer r.pumps.Done()
	for {
		var kind coderws.MessageType
		var payload []byte
		var err error
		if client, ok := r.parent.Value(openAIWSPhysicalClientKey{}).(*openAIWSPhysicalClient); ok {
			kind, payload, err = client.read(r.ctx, 0, coderws.StatusNormalClosure, "", nil, nil)
		} else {
			kind, payload, err = r.conn.Read(context.Background())
		}
		select {
		case r.reads <- openAIWSClientReadResult{messageType: kind, payload: payload, err: err}:
		case <-r.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (r *openAIWSConnectionRuntime) writePump() {
	defer r.pumps.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case write := <-r.writes:
			if err := write.ctx.Err(); err != nil {
				write.result <- err
				continue
			}
			// Canceling one execution must never cancel a partially written
			// frame and thereby corrupt every lane on the physical socket.
			writeCtx, stopWrite := context.WithTimeout(context.WithoutCancel(r.parent), r.options.WriteTimeout)
			err := r.conn.Write(writeCtx, coderws.MessageText, write.payload)
			stopWrite()
			write.result <- err
			if err != nil {
				select {
				case r.writeFailure <- err:
				default:
				}
				return
			}
		}
	}
}

func (r *openAIWSConnectionRuntime) write(writeCtx context.Context, payload []byte) error {
	ack := make(chan error, 1)
	select {
	case r.writes <- openAIWSConnectionWrite{ctx: writeCtx, payload: payload, result: ack}:
	case <-writeCtx.Done():
		return writeCtx.Err()
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
	select {
	case err := <-ack:
		return err
	case <-writeCtx.Done():
		return writeCtx.Err()
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

// reject delivers a scoped recoverable error event; the connection and every
// other lane stay alive. Runtime-generated errors are also reported through
// ObserveRequestError because they never reach the executor.
func (r *openAIWSConnectionRuntime) reject(request OpenAIWSConnectionRequest, requestErr *OpenAIWSRequestError) error {
	if r.options.ObserveRequestError != nil {
		r.options.ObserveRequestError(request, requestErr)
	}
	return r.write(r.ctx, openAIWSRequestErrorEvent(requestErr, request.StreamID))
}

// accept validates and enqueues one client frame. Invalid frames produce
// scoped error events, never a whole-socket reset.
func (r *openAIWSConnectionRuntime) accept(payload []byte) error {
	frame := OpenAIWSConnectionRequest{ConnectionID: r.connectionID, RawPayload: payload}
	if !gjson.ValidBytes(payload) || !gjson.ParseBytes(payload).IsObject() {
		return r.reject(frame, newOpenAIWSRequestError(400, "invalid_json", "response event must be a JSON object", ""))
	}
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType != "" && eventType != "response.create" {
		if streamID, streamErr := openAIWSStreamID(payload); streamErr == nil {
			frame.StreamID = streamID
		}
		if eventType == "response.steer" {
			// The HTTP bridge must not emulate native upstream steering by
			// canceling and regenerating active work.
			return r.reject(frame, newOpenAIWSRequestError(400, "steering_not_supported", "mid-turn steering requires a native WebSocket upstream; the HTTP bridge does not cancel and regenerate work", "type"))
		}
		return r.reject(frame, newOpenAIWSRequestError(400, "unsupported_event", "use response.create for this connection", "type"))
	}
	streamID, streamErr := openAIWSStreamID(payload)
	if streamErr != nil {
		requestErr, ok := streamErr.(*OpenAIWSRequestError)
		if !ok {
			requestErr = newOpenAIWSRequestError(400, "invalid_stream_id", streamErr.Error(), "stream_id")
		}
		return r.reject(frame, requestErr)
	}
	frame.StreamID = streamID
	lane := r.lanes[streamID]
	if lane == nil {
		if streamID != "" && r.named >= r.options.MaxNamedStreams {
			r.invalidateAcceptParent(payload, streamID)
			return r.reject(frame, newOpenAIWSRequestError(400, "websocket_stream_limit_reached", fmt.Sprintf("This WebSocket connection has reached its maximum number of distinct stream IDs (%d). Reuse an existing stream_id or open a new WebSocket connection.", r.options.MaxNamedStreams), "stream_id"))
		}
		lane = &openAIWSConnectionLane{id: streamID}
		r.lanes[streamID] = lane
		r.laneOrder = append(r.laneOrder, streamID)
		if streamID != "" {
			r.named++
		}
	}
	if r.queued >= r.options.MaxQueuedRequests || int64(len(payload)) > r.options.MaxQueuedBytes-r.queuedBytes {
		r.invalidateAcceptParent(payload, streamID)
		return r.reject(frame, newOpenAIWSRequestError(429, "connection_queue_full", "too many queued requests on this connection", ""))
	}
	r.outcome.Requests++
	lane.queue = append(lane.queue, openAIWSQueuedRequest{raw: payload, streamID: streamID, sequence: r.outcome.Requests})
	r.queued++
	r.queuedBytes += int64(len(payload))
	return r.startReady()
}

// startReady drains lane heads round-robin while capacity remains. A lane runs
// one request at a time (FIFO), lanes cannot starve each other, and a lane
// whose head does not fit the active materialized memory budget is skipped
// only for this pass and retried when a completion frees memory.
func (r *openAIWSConnectionRuntime) startReady() error {
	memorySkipped := make(map[string]bool)
	for r.active < r.options.MaxActive && r.queued > 0 {
		var lane *openAIWSConnectionLane
		for range len(r.laneOrder) {
			candidate := r.lanes[r.laneOrder[r.cursor%len(r.laneOrder)]]
			r.cursor = (r.cursor + 1) % len(r.laneOrder)
			if candidate.running || len(candidate.queue) == 0 || memorySkipped[candidate.id] {
				continue
			}
			lane = candidate
			break
		}
		if lane == nil {
			return nil
		}
		request := lane.queue[0]
		lane.queue[0] = openAIWSQueuedRequest{}
		lane.queue = lane.queue[1:]
		r.queued--
		r.queuedBytes -= int64(len(request.raw))

		observed := OpenAIWSConnectionRequest{
			ConnectionID: r.connectionID,
			StreamID:     lane.id,
			Sequence:     request.sequence,
			RawPayload:   request.raw,
		}
		prepared, input, inputKnown, parentSnapshot, warmup, prepareErr := prepareOpenAIWSConnectionRequest(r.ctx, request.raw, lane, r.cache, r.options.ResolvePersisted, r.options.NormalizeRequest)
		if prepareErr != nil {
			r.invalidateContinuationParent(parentSnapshot, lane.id)
			if err := r.reject(observed, prepareErr); err != nil {
				return err
			}
			continue
		}
		observed.Payload = prepared
		inputBytes := openAIWSHistoryBytes(input)
		if inputBytes > r.options.MaxHistoryBytes {
			r.invalidateContinuationParent(parentSnapshot, lane.id)
			if err := r.reject(observed, newOpenAIWSRequestError(413, "context_too_large", "conversation exceeds the gateway connection history limit", "input")); err != nil {
				return err
			}
			continue
		}
		if !r.cache.reserveActive(inputBytes) {
			lane.queue = append([]openAIWSQueuedRequest{request}, lane.queue...)
			r.queued++
			r.queuedBytes += int64(len(request.raw))
			memorySkipped[lane.id] = true
			continue
		}
		model := gjson.GetBytes(prepared, "model").String()
		lane.model = model
		if warmup {
			if err := r.runWarmup(lane, observed, input, inputKnown, model, parentSnapshot); err != nil {
				return err
			}
			continue
		}
		lane.running = true
		r.active++
		r.executions.Add(1)
		execCtx := WithOpenAIWSConnectionScope(r.ctx, r.connectionID, lane.id)
		execCtx = context.WithValue(execCtx, openAIWSBridgeExecutionKey{}, true)
		go r.runExecution(execCtx, observed, request, input, inputKnown, inputBytes, model, parentSnapshot)
	}
	return nil
}

func (r *openAIWSConnectionRuntime) runExecution(
	execCtx context.Context,
	observed OpenAIWSConnectionRequest,
	request openAIWSQueuedRequest,
	input []json.RawMessage,
	inputKnown bool,
	inputBytes int64,
	model string,
	parentSnapshot *openAIWSResponseSnapshot,
) {
	defer r.executions.Done()
	outputReserved := runOpenAIWSConnectionExecution(execCtx, observed, input, inputKnown, model, parentSnapshot, r.execute, r.write, r.cache)
	completion := openAIWSExecutionCompletion{request: request, reservedBytes: inputBytes + outputReserved}
	select {
	case r.completed <- completion:
	case <-r.ctx.Done():
		r.cache.releaseActive(completion.reservedBytes)
	}
}

// runWarmup resolves a generate:false checkpoint locally: no inference, tool
// calls, execution lease, or billing. The optional admission hook runs after
// preparation, the checkpoint is published before the first observable event,
// and the emitted created/completed pair mirrors a real completed response.
func (r *openAIWSConnectionRuntime) runWarmup(
	lane *openAIWSConnectionLane,
	request OpenAIWSConnectionRequest,
	input []json.RawMessage,
	inputKnown bool,
	model string,
	parentSnapshot *openAIWSResponseSnapshot,
) error {
	if r.options.ValidateWarmup != nil {
		warmCtx := WithOpenAIWSConnectionScope(r.ctx, r.connectionID, lane.id)
		warmCtx = context.WithValue(warmCtx, openAIWSBridgeExecutionKey{}, true)
		if validateErr := r.options.ValidateWarmup(warmCtx, request); validateErr != nil {
			requestErr := newOpenAIWSRequestError(400, "warmup_rejected", validateErr.Error(), "")
			var typed *OpenAIWSRequestError
			if errors.As(validateErr, &typed) {
				requestErr = typed
			}
			r.invalidateContinuationParent(parentSnapshot, lane.id)
			return r.reject(request, requestErr)
		}
	}
	snapshot := &openAIWSResponseSnapshot{
		id:         "resp_warmup_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		streamID:   lane.id,
		model:      model,
		items:      input,
		inputKnown: inputKnown,
		bytes:      openAIWSHistoryBytes(input),
	}
	if !r.cache.fits(lane.id, snapshot.bytes) {
		r.invalidateContinuationParent(parentSnapshot, lane.id)
		return r.reject(request, newOpenAIWSRequestError(429, "connection_state_capacity_exceeded", "connection history capacity is exhausted", "input"))
	}
	r.cache.publish(lane.id, snapshot)
	for _, event := range openAIWSWarmupEvents(snapshot.id, model, lane.id) {
		if err := r.write(r.ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// invalidateContinuationParent evicts the referenced checkpoint when a
// same-lane continuation fails with a request error or execution failure. A
// cross-lane fork failure preserves the shared parent so the source lane can
// continue.
func (r *openAIWSConnectionRuntime) invalidateContinuationParent(parent *openAIWSResponseSnapshot, laneID string) {
	if parent != nil && parent.streamID == laneID {
		r.cache.invalidate(parent.id)
	}
}

// invalidateAcceptParent applies the same same-lane eviction rule to
// connection-level request rejections (queue admission, named stream limit)
// that never reach preparation.
func (r *openAIWSConnectionRuntime) invalidateAcceptParent(payload []byte, laneID string) {
	r.invalidateContinuationParent(r.cache.lookup(gjson.GetBytes(payload, "previous_response_id").String()), laneID)
}

func (r *openAIWSConnectionRuntime) handleCompletion(completion openAIWSExecutionCompletion) error {
	if lane := r.lanes[completion.request.streamID]; lane != nil {
		lane.running = false
	}
	r.active--
	r.cache.releaseActive(completion.reservedBytes)
	return r.startReady()
}

func prepareOpenAIWSConnectionRequest(
	ctx context.Context,
	payload []byte,
	lane *openAIWSConnectionLane,
	cache *openAIWSConnectionCache,
	resolvePersisted func(context.Context, string) bool,
	normalizeRequest func([]byte) ([]byte, bool),
) ([]byte, []json.RawMessage, bool, *openAIWSResponseSnapshot, bool, *OpenAIWSRequestError) {
	// Lineage resolves before any validation: every 4xx/5xx returned to a
	// same-lane continuation must be able to evict the referenced parent.
	previous := gjson.GetBytes(payload, "previous_response_id")
	previousID := previous.String()
	parent := cache.lookup(previousID)
	if previous.Exists() && previous.Type != gjson.Null && (previous.Type != gjson.String || strings.TrimSpace(previousID) == "") {
		return nil, nil, false, parent, false, newOpenAIWSRequestError(400, "previous_response_not_found", "previous_response_id is not available on this connection", "previous_response_id")
	}
	generate := gjson.GetBytes(payload, "generate")
	if generate.Exists() && generate.Type != gjson.True && generate.Type != gjson.False {
		return nil, nil, false, parent, false, newOpenAIWSRequestError(400, "invalid_generate", "generate must be a boolean", "generate")
	}
	warmup := generate.Type == gjson.False
	model := strings.TrimSpace(gjson.GetBytes(payload, "model").String())
	if model == "" {
		model = lane.model
	}
	if model == "" {
		return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "model_required", "model is required in the first request on a stream", "model")
	}
	conversation := gjson.GetBytes(payload, "conversation")
	if previousID != "" && conversation.Exists() && conversation.Type != gjson.Null {
		return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "invalid_request", "conversation and previous_response_id are mutually exclusive", "conversation")
	}
	if gjson.GetBytes(payload, "background").Bool() {
		return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "unsupported_background", "background is not supported over WebSocket", "background")
	}
	if warmup && (gjson.GetBytes(payload, "store").Type == gjson.True || conversation.Exists() && conversation.Type != gjson.Null) {
		return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "unsupported_warmup_state", "persistent warmup requires a native WebSocket upstream", "store")
	}
	input, inputExists, err := openAIWSExtractNormalizedInputSequence(payload)
	if err != nil {
		return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "invalid_input", err.Error(), "input")
	}
	materialize := parent != nil && parent.inputKnown
	if previousID != "" && !materialize {
		// Unknown or cross-owner lineage is never silently stripped: only an
		// authenticated persisted response may hydrate a non-cached parent.
		allowPersisted := !warmup && gjson.GetBytes(payload, "store").Type != gjson.False && resolvePersisted != nil && resolvePersisted(ctx, previousID)
		if !allowPersisted {
			return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "previous_response_not_found", fmt.Sprintf("Previous response with id '%s' not found on this connection. Retry with full input and no previous_response_id.", previousID), "previous_response_id")
		}
	}
	if materialize {
		// This checkpoint is authoritative, not heuristic recovery history.
		// Preserve every item; do not drop unanswered calls or dedupe explicit
		// client input against a guessed prefix.
		input = combineOpenAIWSReplayItems(parent.items, input)
		inputExists = true
	}
	// An empty warmup is a valid checkpoint, not an empty model inference.
	if warmup && !inputExists {
		input = []json.RawMessage{}
		inputExists = true
	}
	body := payload
	for _, key := range []string{"type", "generate", "stream_id", "background"} {
		body, err = sjson.DeleteBytes(body, key)
		if err != nil {
			return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "invalid_request", err.Error(), key)
		}
	}
	// A null checkpoint starts a new chain, just like an omitted checkpoint.
	if materialize || warmup || previous.Type == gjson.Null {
		body = RemovePreviousResponseIDFromBody(body)
	}
	body, err = setOpenAIWSPayloadInputSequence(body, input, inputExists)
	if err == nil {
		body, err = sjson.SetBytes(body, "model", model)
	}
	if err == nil {
		body, err = sjson.SetBytes(body, "stream", true)
	}
	if err != nil {
		return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "invalid_request", err.Error(), "")
	}
	if normalizeRequest != nil {
		if normalized, changed := normalizeRequest(body); changed {
			body = normalized
			input, inputExists, err = openAIWSExtractNormalizedInputSequence(body)
			if err != nil {
				return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "invalid_input", err.Error(), "input")
			}
		}
	}
	coverage := AnalyzeToolCallOutputContextCoverageBytes(body)
	// Persisted parents must already have passed the ownership gate above.
	// Encrypted compaction windows are self-contained but opaque to us. In
	// either case the upstream, not this local scan, must validate call IDs.
	upstreamOwnsHistory := previousID != "" && !materialize && !warmup &&
		coverage.MissingCallIDCount == 0 && !coverage.AmbiguousStandalone
	if !coverage.CanReplayWithoutPreviousResponse() && !upstreamOwnsHistory {
		message := fmt.Sprintf("tool output has no matching call in the available response history (unmatched_call_ids=%d)", coverage.UnmatchedCallIDCount)
		if coverage.MissingCallIDCount > 0 {
			message = fmt.Sprintf("tool output is missing a valid call_id and is not a recognized standalone notification (invalid_outputs=%d)", coverage.MissingCallIDCount)
		}
		if coverage.AmbiguousStandalone {
			message = "standalone tool output is ambiguous because the request contains duplicate JSON members"
		}
		return nil, nil, false, parent, warmup, newOpenAIWSRequestError(400, "invalid_input", message, "input")
	}
	known := inputExists && (previousID == "" || materialize) && (!conversation.Exists() || conversation.Type == gjson.Null)
	return body, input, known, parent, warmup, nil
}

func openAIWSWarmupEvents(responseID, model, streamID string) [][]byte {
	createdAt := time.Now().Unix()
	events := make([][]byte, 0, 2)
	for index, eventType := range []string{"response.created", "response.completed"} {
		status := "in_progress"
		if index == 1 {
			status = "completed"
		}
		event := map[string]any{
			"type":            eventType,
			"sequence_number": index,
			"response": map[string]any{
				"id":                 responseID,
				"object":             "response",
				"created_at":         createdAt,
				"model":              model,
				"status":             status,
				"output":             []any{},
				"error":              nil,
				"incomplete_details": nil,
				"store":              false,
				"usage":              map[string]int{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
			},
		}
		if streamID != "" {
			event["stream_id"] = streamID
		}
		encoded, _ := json.Marshal(event)
		events = append(events, encoded)
	}
	return events
}

// runOpenAIWSConnectionExecution drives one isolated execution. Checkpoint
// publication and same-lane parent invalidation happen before the terminal
// frame is written, so a client reacting to the terminal event on any lane
// observes a coherent cache decision, and scheduler completion never races
// continuation lookup.
//
// Output items collect under the same per-connection active memory budget as
// materialized inputs (charged conservatively by frame size; deduped items
// may be charged twice). Budget overflow never truncates or alters the client
// stream: collection stops and the turn's checkpoint becomes unavailable, so
// replay history is either complete or absent, never silently partial. The
// returned byte count is the reservation the caller must release.
func runOpenAIWSConnectionExecution(
	ctx context.Context,
	request OpenAIWSConnectionRequest,
	input []json.RawMessage,
	inputKnown bool,
	model string,
	parent *openAIWSResponseSnapshot,
	execute OpenAIWSRequestExecutor,
	write func(context.Context, []byte) error,
	cache *openAIWSConnectionCache,
) (outputReserved int64) {
	collector := &openAIWSToolCallReplayCollector{}
	responseID := ""
	terminal := false
	checkpointLost := false
	emit := func(payload []byte) error {
		if terminal {
			return nil
		}
		if !gjson.ValidBytes(payload) {
			return errors.New("upstream emitted invalid response event JSON")
		}
		typeName := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
		if id := gjson.GetBytes(payload, "response.id").String(); id != "" {
			responseID = id
		}
		isTerminal := typeName == "response.completed" ||
			typeName == "response.done" ||
			typeName == "response.failed" ||
			typeName == "response.incomplete" ||
			typeName == "error"
		if typeName == "response.output_item.done" || typeName == "response.completed" || typeName == "response.done" || typeName == "response.incomplete" {
			if !checkpointLost {
				charge := int64(len(payload))
				if cache.reserveActive(charge) {
					outputReserved += charge
					if typeName == "response.incomplete" {
						// The shared collector mines terminal output[] only from
						// completed/done; an incomplete terminal carries the same
						// array shape and its items belong to the checkpoint.
						collector.AddEvent("response.completed", payload)
					} else {
						collector.AddEvent(typeName, payload)
					}
				} else {
					checkpointLost = true
				}
			}
		}
		if isTerminal {
			if typeName == "response.failed" || typeName == "error" {
				if parent != nil && parent.streamID == request.StreamID {
					cache.invalidate(parent.id)
				}
			} else if checkpointLost {
				cache.advance(request.StreamID)
			} else if responseID != "" {
				items := combineOpenAIWSReplayItems(input, collector.AllItems())
				cache.publish(request.StreamID, &openAIWSResponseSnapshot{
					id:         responseID,
					streamID:   request.StreamID,
					model:      model,
					items:      items,
					inputKnown: inputKnown,
					bytes:      openAIWSHistoryBytes(items),
				})
			}
		}
		if request.StreamID != "" {
			stamped, err := sjson.SetBytes(payload, "stream_id", request.StreamID)
			if err != nil {
				return err
			}
			payload = stamped
		} else if gjson.GetBytes(payload, "stream_id").Exists() {
			stripped, err := sjson.DeleteBytes(payload, "stream_id")
			if err != nil {
				return err
			}
			payload = stripped
		}
		if err := write(ctx, payload); err != nil {
			return err
		}
		if isTerminal {
			terminal = true
		}
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil && !terminal && ctx.Err() == nil {
			_ = emit(openAIWSRequestErrorEvent(newOpenAIWSRequestError(500, "gateway_execution_error", "response execution failed", ""), request.StreamID))
		}
	}()
	err := execute(ctx, request, emit)
	if !terminal && ctx.Err() == nil {
		if err != nil {
			requestErr := newOpenAIWSRequestError(502, "upstream_error", "upstream response failed before completion", "")
			var typed *OpenAIWSRequestError
			if errors.As(err, &typed) {
				requestErr = typed
			}
			_ = emit(openAIWSRequestErrorEvent(requestErr, request.StreamID))
		} else {
			_ = emit(openAIWSRequestErrorEvent(newOpenAIWSRequestError(502, "incomplete_upstream_stream", "upstream stream ended without a response terminal event", ""), request.StreamID))
		}
	}
	return outputReserved
}
