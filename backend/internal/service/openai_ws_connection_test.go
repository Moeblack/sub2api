package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// openAIWSConnTestHarness hosts RunOpenAIWSConnection on a real loopback
// WebSocket server so every assertion observes actual socket frames.
type openAIWSConnTestHarness struct {
	t        *testing.T
	server   *httptest.Server
	runErrs  chan error
	outcomes chan OpenAIWSConnectionOutcome

	mu      sync.Mutex
	clients []*coderws.Conn
}

func newOpenAIWSConnTestHarness(
	t *testing.T,
	parent context.Context,
	first []byte,
	options OpenAIWSConnectionOptions,
	executor OpenAIWSRequestExecutor,
) *openAIWSConnTestHarness {
	t.Helper()
	if parent == nil {
		parent = context.Background()
	}
	h := &openAIWSConnTestHarness{
		t:        t,
		runErrs:  make(chan error, 8),
		outcomes: make(chan OpenAIWSConnectionOutcome, 8),
	}
	userObserveClosed := options.ObserveClosed
	options.ObserveClosed = func(outcome OpenAIWSConnectionOutcome) {
		if userObserveClosed != nil {
			userObserveClosed(outcome)
		}
		h.outcomes <- outcome
	}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		h.runErrs <- RunOpenAIWSConnection(parent, conn, first, options, executor)
	}))
	t.Cleanup(h.shutdown)
	return h
}

func (h *openAIWSConnTestHarness) dial(t *testing.T) *coderws.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(h.server.URL, "http"), nil)
	require.NoError(t, err)
	h.mu.Lock()
	h.clients = append(h.clients, conn)
	h.mu.Unlock()
	return conn
}

func (h *openAIWSConnTestHarness) requireRunErr(t *testing.T) error {
	t.Helper()
	select {
	case err := <-h.runErrs:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the connection runtime to return")
		return nil
	}
}

func (h *openAIWSConnTestHarness) requireOutcome(t *testing.T) OpenAIWSConnectionOutcome {
	t.Helper()
	select {
	case outcome := <-h.outcomes:
		return outcome
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for ObserveClosed")
		return OpenAIWSConnectionOutcome{}
	}
}

func (h *openAIWSConnTestHarness) shutdown() {
	h.mu.Lock()
	clients := h.clients
	h.clients = nil
	h.mu.Unlock()
	for _, conn := range clients {
		_ = conn.CloseNow()
	}
	h.server.Close()
}

// openAIWSFakeExecutor scripts executions by metadata.tag. Gates block a tag
// until the test releases it, making concurrency deterministic.
type openAIWSFakeExecutor struct {
	startCh    chan string
	returnedCh chan string

	mu       sync.Mutex
	calls    map[string]int
	payloads map[string][]byte
	gates    map[string]chan struct{}
	events   map[string][][]byte
	errs     map[string]error
}

func newOpenAIWSFakeExecutor() *openAIWSFakeExecutor {
	return &openAIWSFakeExecutor{
		startCh:    make(chan string, 128),
		returnedCh: make(chan string, 128),
		calls:      make(map[string]int),
		payloads:   make(map[string][]byte),
		gates:      make(map[string]chan struct{}),
		events:     make(map[string][][]byte),
		errs:       make(map[string]error),
	}
}

func (f *openAIWSFakeExecutor) setGate(tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gates[tag] = make(chan struct{})
}

func (f *openAIWSFakeExecutor) releaseGate(tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	close(f.gates[tag])
}

func (f *openAIWSFakeExecutor) setEvents(tag string, events ...[]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events[tag] = events
}

func (f *openAIWSFakeExecutor) setErr(tag string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs[tag] = err
}

func (f *openAIWSFakeExecutor) callCount(tag string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[tag]
}

func (f *openAIWSFakeExecutor) payload(tag string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.payloads[tag]...)
}

func (f *openAIWSFakeExecutor) run(ctx context.Context, request OpenAIWSConnectionRequest, emit func([]byte) error) error {
	tag := gjson.GetBytes(request.Payload, "metadata.tag").String()
	f.mu.Lock()
	f.calls[tag]++
	f.payloads[tag] = append([]byte(nil), request.Payload...)
	gate := f.gates[tag]
	events := f.events[tag]
	execErr := f.errs[tag]
	f.mu.Unlock()
	defer func() { f.returnedCh <- tag }()
	select {
	case f.startCh <- tag:
	case <-ctx.Done():
		return ctx.Err()
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if execErr != nil {
		return execErr
	}
	for _, event := range events {
		if err := emit(event); err != nil {
			return err
		}
	}
	return nil
}

func requireWSExecStart(t *testing.T, f *openAIWSFakeExecutor, want string) {
	t.Helper()
	select {
	case got := <-f.startCh:
		require.Equal(t, want, got)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for execution %q to start", want)
	}
}

func requireWSExecStarts(t *testing.T, f *openAIWSFakeExecutor, wants ...string) {
	t.Helper()
	got := make([]string, 0, len(wants))
	for range len(wants) {
		select {
		case tag := <-f.startCh:
			got = append(got, tag)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for executions %v (got %v)", wants, got)
		}
	}
	require.ElementsMatch(t, wants, got)
}

func requireWSNoExecStart(t *testing.T, f *openAIWSFakeExecutor, within time.Duration) {
	t.Helper()
	select {
	case got := <-f.startCh:
		t.Fatalf("unexpected execution start %q", got)
	case <-time.After(within):
	}
}

func requireWSExecReturned(t *testing.T, f *openAIWSFakeExecutor, want string) {
	t.Helper()
	select {
	case got := <-f.returnedCh:
		require.Equal(t, want, got)
	default:
		t.Fatalf("execution %q was not joined before the connection returned", want)
	}
}

// Frame builders. metadata.tag is the executor discriminator; it is transport
// metadata and never becomes part of the replay history.
func wsTestCreateFrame(t *testing.T, fields map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(fields)
	require.NoError(t, err)
	return string(payload)
}

func wsTestTextMessage(text string) map[string]any {
	return map[string]any{
		"type":    "message",
		"role":    "user",
		"content": []any{map[string]any{"type": "input_text", "text": text}},
	}
}

func wsTestBusinessFields(tag, model string, input []any) map[string]any {
	return map[string]any{
		"type":     "response.create",
		"model":    model,
		"store":    false,
		"input":    input,
		"metadata": map[string]any{"tag": tag},
	}
}

func wsTestCreatedEvent(responseID string) []byte {
	return []byte(`{"type":"response.created","response":{"id":"` + responseID + `","status":"in_progress"}}`)
}

func wsTestItemDoneEvent(item string) []byte {
	return []byte(`{"type":"response.output_item.done","item":` + item + `}`)
}

func wsTestCompletedEvent(responseID string, output ...string) []byte {
	return []byte(`{"type":"response.completed","response":{"id":"` + responseID + `","status":"completed","output":[` + strings.Join(output, ",") + `]}}`)
}

func wsTestFailedEvent(responseID string) []byte {
	return []byte(`{"type":"response.failed","response":{"id":"` + responseID + `","status":"failed"}}`)
}

func wsTestIncompleteEvent(responseID string, output ...string) []byte {
	return []byte(`{"type":"response.incomplete","response":{"id":"` + responseID + `","status":"incomplete","output":[` + strings.Join(output, ",") + `]}}`)
}

func wsTestStream(responseID string, items ...string) [][]byte {
	events := [][]byte{wsTestCreatedEvent(responseID)}
	for _, item := range items {
		events = append(events, wsTestItemDoneEvent(item))
	}
	return append(events, wsTestCompletedEvent(responseID))
}

func wsTestAssistantMessage(id, text string) string {
	return `{"type":"message","id":"` + id + `","role":"assistant","content":[{"type":"output_text","text":"` + text + `"}]}`
}

// Client socket helpers: every read is deadline-bounded.
func wsTestSend(t *testing.T, conn *coderws.Conn, frame string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(frame)))
}

func wsTestReadEvent(t *testing.T, conn *coderws.Conn) gjson.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kind, payload, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, coderws.MessageText, kind)
	require.True(t, gjson.ValidBytes(payload), "server emitted invalid JSON frame: %q", payload)
	return gjson.ParseBytes(payload)
}

func wsTestReadEventWhere(t *testing.T, conn *coderws.Conn, description string, match func(gjson.Result) bool) gjson.Result {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		require.False(t, time.Now().After(deadline), "timed out waiting for %s", description)
		event := wsTestReadEvent(t, conn)
		if match(event) {
			return event
		}
	}
}

func wsTestReadCompleted(t *testing.T, conn *coderws.Conn, responseID string) gjson.Result {
	t.Helper()
	return wsTestReadEventWhere(t, conn, "response.completed "+responseID, func(event gjson.Result) bool {
		return event.Get("type").String() == "response.completed" && event.Get("response.id").String() == responseID
	})
}

func wsTestReadErrorEvent(t *testing.T, conn *coderws.Conn, code string) gjson.Result {
	t.Helper()
	return wsTestReadEventWhere(t, conn, "error "+code, func(event gjson.Result) bool {
		return event.Get("type").String() == "error" && event.Get("error.code").String() == code
	})
}

func wsTestInputTypes(t *testing.T, payload []byte) []string {
	t.Helper()
	var types []string
	for _, itemType := range gjson.GetBytes(payload, "input.#.type").Array() {
		types = append(types, itemType.String())
	}
	return types
}

func TestRunOpenAIWSConnection_IsolatesIdenticalSessionsAcrossSockets(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setGate("A")
	exec.setEvents("A", wsTestStream("resp_A1", wsTestAssistantMessage("msg_a1", "A done"))...)
	exec.setEvents("B", wsTestStream("resp_B1", wsTestAssistantMessage("msg_b1", "B done"))...)
	h := newOpenAIWSConnTestHarness(t, nil, nil, OpenAIWSConnectionOptions{}, exec.run)

	// Identical business payloads on two physical sockets: same model, same
	// input, same store. The legacy session-id preemption bug canceled A when
	// B started; lane ownership must keep both alive.
	connA := h.dial(t)
	connB := h.dial(t)
	wsTestSend(t, connA, wsTestCreateFrame(t, wsTestBusinessFields("A", "gpt-test", []any{wsTestTextMessage("identical session prompt")})))
	requireWSExecStart(t, exec, "A")
	wsTestSend(t, connB, wsTestCreateFrame(t, wsTestBusinessFields("B", "gpt-test", []any{wsTestTextMessage("identical session prompt")})))
	requireWSExecStart(t, exec, "B")

	completedB := wsTestReadCompleted(t, connB, "resp_B1")
	require.Equal(t, "completed", completedB.Get("response.status").String())

	exec.releaseGate("A")
	wsTestReadCompleted(t, connA, "resp_A1")

	require.NoError(t, connA.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, connB.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_LaneFIFOAndCrossLaneConcurrency(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setGate("A1")
	exec.setEvents("A1", wsTestStream("resp_A1")...)
	exec.setEvents("A2", wsTestStream("resp_A2")...)
	exec.setEvents("B1", wsTestStream("resp_B1")...)
	exec.setEvents("D1", wsTestStream("resp_D1")...)
	h := newOpenAIWSConnTestHarness(t, nil, nil, OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	send := func(tag, streamID string) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage("turn " + tag)})
		if streamID != "" {
			fields["stream_id"] = streamID
		}
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}
	send("A1", "main")
	send("A2", "main")
	send("B1", "other")
	send("D1", "")
	requireWSExecStarts(t, exec, "A1", "B1", "D1")

	// Independent lanes may finish in either order. Do not discard D1's
	// terminal while waiting for B1 (or accidentally impose cross-lane FIFO).
	completed := make(map[string]gjson.Result)
	for len(completed) < 2 {
		event := wsTestReadEvent(t, conn)
		if event.Get("type").String() != "response.completed" {
			continue
		}
		id := event.Get("response.id").String()
		require.Contains(t, []string{"resp_B1", "resp_D1"}, id)
		completed[id] = event
	}
	require.Equal(t, "other", completed["resp_B1"].Get("stream_id").String(), "named lane events echo stream_id")
	require.False(t, completed["resp_D1"].Get("stream_id").Exists(), "default lane events omit stream_id")

	// Two lane completions must not unblock A2: same-lane FIFO.
	requireWSNoExecStart(t, exec, 200*time.Millisecond)
	exec.releaseGate("A1")
	completedA1 := wsTestReadCompleted(t, conn, "resp_A1")
	require.Equal(t, "main", completedA1.Get("stream_id").String())
	requireWSExecStart(t, exec, "A2")
	wsTestReadCompleted(t, conn, "resp_A2")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_StreamIDValidation(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("OK", wsTestStream("resp_OK1")...)
	h := newOpenAIWSConnTestHarness(t, nil, nil, OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	// The schema is nullable:false: explicit null is just as invalid as an
	// empty string, an overlong value, bad characters, or a non-string.
	for _, streamID := range []any{"", strings.Repeat("a", 257), "bad space", "emoji-\U0001f600", 42, true, nil} {
		wsTestSend(t, conn, wsTestCreateFrame(t, map[string]any{
			"type":      "response.create",
			"model":     "gpt-test",
			"stream_id": streamID,
		}))
		event := wsTestReadErrorEvent(t, conn, "invalid_stream_id")
		require.False(t, event.Get("stream_id").Exists(), "invalid_stream_id errors carry no lane")
		require.Equal(t, 400, int(event.Get("status").Int()))
		require.Equal(t, "stream_id", event.Get("error.param").String())
	}

	// Only an omitted field selects the default lane.
	wsTestSend(t, conn, wsTestCreateFrame(t, wsTestBusinessFields("OK", "gpt-test", []any{wsTestTextMessage("default lane")})))
	requireWSExecStart(t, exec, "OK")
	completed := wsTestReadCompleted(t, conn, "resp_OK1")
	require.False(t, completed.Get("stream_id").Exists())

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_NamedStreamLimit(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("a1", wsTestStream("resp_a1")...)
	exec.setEvents("b1", wsTestStream("resp_b1")...)
	exec.setEvents("a2", wsTestStream("resp_a2")...)
	options := OpenAIWSConnectionOptions{MaxNamedStreams: 2}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	send := func(tag, streamID string) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage("turn " + tag)})
		fields["stream_id"] = streamID
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}
	send("a1", "lane-a")
	send("b1", "lane-b")
	requireWSExecStarts(t, exec, "a1", "b1")

	send("c1", "lane-c")
	event := wsTestReadErrorEvent(t, conn, "websocket_stream_limit_reached")
	require.Equal(t, "lane-c", event.Get("stream_id").String(), "stream limit errors echo the rejected lane")
	require.Equal(t, "stream_id", event.Get("error.param").String())
	require.Equal(t, 0, exec.callCount("c1"))

	// Reusing an existing lane stays legal.
	send("a2", "lane-a")
	requireWSExecStart(t, exec, "a2")
	wsTestReadCompleted(t, conn, "resp_a2")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_ActiveLimitAndQueueBound(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setGate("X1")
	exec.setEvents("X1", wsTestStream("resp_X1")...)
	exec.setEvents("Y1", wsTestStream("resp_Y1")...)
	options := OpenAIWSConnectionOptions{MaxActive: 1, MaxQueuedRequests: 1}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	send := func(tag, streamID string) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage("turn " + tag)})
		fields["stream_id"] = streamID
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}
	send("X1", "lane-x")
	requireWSExecStart(t, exec, "X1")
	send("Y1", "lane-y")
	send("Z1", "lane-z")
	event := wsTestReadErrorEvent(t, conn, "connection_queue_full")
	require.Equal(t, "lane-z", event.Get("stream_id").String())
	require.Equal(t, 429, int(event.Get("status").Int()))
	require.Equal(t, "rate_limit_error", event.Get("error.type").String())
	require.Equal(t, 0, exec.callCount("Z1"))

	exec.releaseGate("X1")
	wsTestReadCompleted(t, conn, "resp_X1")
	requireWSExecStart(t, exec, "Y1")
	wsTestReadCompleted(t, conn, "resp_Y1")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_UnknownAndCrossOwnerParent(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("P1", wsTestStream("resp_P1")...)
	exec.setEvents("N1", wsTestStream("resp_N1")...)
	resolvedCalls := make(chan string, 8)
	options := OpenAIWSConnectionOptions{
		ResolvePersisted: func(_ context.Context, responseID string) bool {
			resolvedCalls <- responseID
			return responseID == "resp_persisted"
		},
	}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	send := func(tag string, extra map[string]any) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage("turn " + tag)})
		fields["stream_id"] = "main"
		// Persisted hydration requires store!=false; drop the helper default and
		// let each case opt back in explicitly.
		delete(fields, "store")
		for key, value := range extra {
			fields[key] = value
		}
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}

	// store:false lineage is connection-local only: the persisted resolver
	// must not even be consulted, and the executor must never run.
	send("G1", map[string]any{"previous_response_id": "resp_ghost", "store": false})
	event := wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, "main", event.Get("stream_id").String())
	require.Equal(t, "previous_response_id", event.Get("error.param").String())
	select {
	case resolved := <-resolvedCalls:
		t.Fatalf("store:false continuation unexpectedly hit the persisted resolver for %q", resolved)
	default:
	}
	require.Equal(t, 0, exec.callCount("G1"))

	// A persisted response owned by someone else resolves to not found; the
	// resolver is consulted and the executor still never runs.
	send("F1", map[string]any{"previous_response_id": "resp_foreign"})
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	select {
	case resolved := <-resolvedCalls:
		require.Equal(t, "resp_foreign", resolved)
	default:
		t.Fatal("store-default continuation did not consult the persisted resolver")
	}
	require.Equal(t, 0, exec.callCount("F1"))

	// An authenticated persisted parent hydrates: previous_response_id is
	// preserved for the upstream, never materialized from local state.
	send("P1", map[string]any{"previous_response_id": "resp_persisted"})
	requireWSExecStart(t, exec, "P1")
	prepared := exec.payload("P1")
	require.Equal(t, "resp_persisted", gjson.GetBytes(prepared, "previous_response_id").String())
	require.Equal(t, []string{"message"}, wsTestInputTypes(t, prepared))
	wsTestReadCompleted(t, conn, "resp_P1")

	// Explicit null starts a new chain.
	send("N1", map[string]any{"previous_response_id": nil})
	requireWSExecStart(t, exec, "N1")
	require.Empty(t, gjson.GetBytes(exec.payload("N1"), "previous_response_id").String())
	wsTestReadCompleted(t, conn, "resp_N1")

	// An empty string is not a valid lineage reference.
	send("E1", map[string]any{"previous_response_id": ""})
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("E1"))

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_OrderedReplayWithLiteTerminalAndDedup(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	// A Lite-style terminal: response.completed carries output: [] while all
	// items arrive as separately streamed output_item.done events. Duplicates
	// (streamed twice, or repeated in the terminal output) collapse by id.
	exec.setEvents("T1",
		wsTestCreatedEvent("resp_T1"),
		wsTestItemDoneEvent(`{"type":"reasoning","id":"rs_1","encrypted_content":"opaque","summary":[{"type":"summary_text","text":"think"}]}`),
		wsTestItemDoneEvent(`{"type":"reasoning","id":"rs_1","encrypted_content":"opaque","summary":[{"type":"summary_text","text":"think"}]}`),
		wsTestItemDoneEvent(`{"type":"message","id":"msg_plan","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"planning"}]}`),
		wsTestItemDoneEvent(`{"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{}"}`),
		wsTestCompletedEvent("resp_T1",
			`{"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{}"}`,
			wsTestAssistantMessage("msg_t", "final"),
		),
		// Post-terminal junk must be dropped, never forwarded to the client.
		wsTestItemDoneEvent(`{"type":"message","id":"msg_late"}`),
	)
	exec.setEvents("T2", wsTestStream("resp_T2")...)
	exec.setEvents("N1",
		wsTestCreatedEvent("resp_N1"),
		wsTestItemDoneEvent(`{"type":"function_call","id":"fc_n1","call_id":"call_n1","name":"lookup","arguments":"{}"}`),
		wsTestIncompleteEvent("resp_N1",
			`{"type":"function_call","id":"fc_n1","call_id":"call_n1","name":"lookup","arguments":"{}"}`,
			wsTestAssistantMessage("msg_n1t", "n1-terminal"),
		),
	)
	exec.setEvents("N2", wsTestStream("resp_N2")...)
	h := newOpenAIWSConnTestHarness(t, nil, nil, OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	fieldsT1 := wsTestBusinessFields("T1", "gpt-test", []any{wsTestTextMessage("turn-1")})
	fieldsT1["stream_id"] = "main"
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsT1))
	requireWSExecStart(t, exec, "T1")
	wsTestReadCompleted(t, conn, "resp_T1")

	fieldsT2 := wsTestBusinessFields("T2", "gpt-test", []any{
		map[string]any{"type": "function_call_output", "call_id": "call_1", "output": "ok"},
		wsTestTextMessage("turn-2"),
	})
	fieldsT2["stream_id"] = "main"
	fieldsT2["previous_response_id"] = "resp_T1"
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsT2))

	// The frame right after T1's terminal must already belong to T2: the
	// post-terminal junk was dropped.
	nextAfterTerminal := wsTestReadEvent(t, conn)
	require.Equal(t, "response.created", nextAfterTerminal.Get("type").String())
	require.Equal(t, "resp_T2", nextAfterTerminal.Get("response.id").String())

	requireWSExecStart(t, exec, "T2")
	prepared := exec.payload("T2")
	require.Equal(t, []string{
		"message", "reasoning", "message", "function_call", "message", "function_call_output", "message",
	}, wsTestInputTypes(t, prepared))
	require.Equal(t, "opaque", gjson.GetBytes(prepared, "input.1.encrypted_content").String())
	require.Equal(t, "commentary", gjson.GetBytes(prepared, "input.2.phase").String())
	require.Equal(t, "turn-1", gjson.GetBytes(prepared, "input.0.content.0.text").String())
	require.Equal(t, "final", gjson.GetBytes(prepared, "input.4.content.0.text").String(), "terminal-only output item follows streamed items")
	require.Equal(t, "turn-2", gjson.GetBytes(prepared, "input.6.content.0.text").String())
	require.False(t, gjson.GetBytes(prepared, "previous_response_id").Exists(), "materialized continuation strips previous_response_id")
	require.True(t, gjson.GetBytes(prepared, "stream").Bool())
	require.Equal(t, "gpt-test", gjson.GetBytes(prepared, "model").String())
	for _, stripped := range []string{"type", "generate", "stream_id", "background"} {
		require.False(t, gjson.GetBytes(prepared, stripped).Exists(), "transport field %q must not reach the upstream body", stripped)
	}
	wsTestReadCompleted(t, conn, "resp_T2")

	// response.incomplete is a checkpoint terminal too: its terminal-only
	// output items must join the history exactly like completed ones.
	fieldsN1 := wsTestBusinessFields("N1", "gpt-test", []any{wsTestTextMessage("inc-1")})
	fieldsN1["stream_id"] = "inc"
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsN1))
	requireWSExecStart(t, exec, "N1")
	wsTestReadEventWhere(t, conn, "N1 incomplete", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.incomplete" && event.Get("response.id").String() == "resp_N1"
	})
	fieldsN2 := wsTestBusinessFields("N2", "gpt-test", []any{wsTestTextMessage("inc-2")})
	fieldsN2["stream_id"] = "inc"
	fieldsN2["previous_response_id"] = "resp_N1"
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsN2))
	requireWSExecStart(t, exec, "N2")
	preparedN2 := exec.payload("N2")
	require.Equal(t, []string{"message", "function_call", "message", "message"}, wsTestInputTypes(t, preparedN2))
	require.Equal(t, "n1-terminal", gjson.GetBytes(preparedN2, "input.2.content.0.text").String())
	wsTestReadCompleted(t, conn, "resp_N2")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_WarmupCheckpoints(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("C2", wsTestStream("resp_C2")...)
	exec.setEvents("C3", wsTestStream("resp_C3")...)
	exec.setEvents("G1", wsTestStream("resp_G1")...)
	h := newOpenAIWSConnTestHarness(t, nil, nil, OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	requireWarmupShape := func(created, completed gjson.Result, streamID string) {
		require.Equal(t, "in_progress", created.Get("response.status").String())
		require.Equal(t, "completed", completed.Get("response.status").String())
		require.True(t, completed.Get("response.output").IsArray())
		require.Empty(t, completed.Get("response.output").Array())
		require.Equal(t, int64(0), completed.Get("response.usage.total_tokens").Int())
		require.False(t, completed.Get("response.store").Bool())
		require.True(t, strings.HasPrefix(completed.Get("response.id").String(), "resp_warmup_"))
		if streamID == "" {
			require.False(t, completed.Get("stream_id").Exists())
		} else {
			require.Equal(t, streamID, completed.Get("stream_id").String())
		}
	}

	// Empty warmup: a real local checkpoint with no inference and no executor.
	fieldsW1 := map[string]any{"type": "response.create", "generate": false, "model": "gpt-test", "store": false, "stream_id": "warm"}
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsW1))
	createdW1 := wsTestReadEventWhere(t, conn, "warmup created", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.created" && event.Get("stream_id").String() == "warm"
	})
	completedW1 := wsTestReadEventWhere(t, conn, "warmup completed", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.completed" && event.Get("stream_id").String() == "warm"
	})
	requireWarmupShape(createdW1, completedW1, "warm")
	warmupID1 := completedW1.Get("response.id").String()

	// Nonempty warmup carries tools/instructions and its input checkpoint.
	fieldsW2 := map[string]any{
		"type":         "response.create",
		"generate":     false,
		"model":        "gpt-test",
		"store":        false,
		"stream_id":    "warm",
		"instructions": "be terse",
		"tools":        []any{map[string]any{"type": "function", "name": "noop", "parameters": map[string]any{"type": "object"}}},
		"input":        []any{wsTestTextMessage("warm context")},
	}
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsW2))
	completedW2 := wsTestReadEventWhere(t, conn, "warmup completed", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.completed" && event.Get("stream_id").String() == "warm"
	})
	warmupID2 := completedW2.Get("response.id").String()
	require.NotEqual(t, warmupID1, warmupID2)
	requireWSNoExecStart(t, exec, 200*time.Millisecond)

	// The repeated warmup replaced the first: chaining from it is not found.
	fieldsC1 := wsTestBusinessFields("C1", "gpt-test", []any{wsTestTextMessage("stale chain")})
	fieldsC1["stream_id"] = "warm"
	fieldsC1["previous_response_id"] = warmupID1
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsC1))
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("C1"))

	// Chaining from the latest warmup materializes the warmup input checkpoint.
	fieldsC2 := wsTestBusinessFields("C2", "gpt-test", []any{wsTestTextMessage("business question")})
	fieldsC2["stream_id"] = "warm"
	fieldsC2["previous_response_id"] = warmupID2
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsC2))
	requireWSExecStart(t, exec, "C2")
	preparedC2 := exec.payload("C2")
	require.Equal(t, []string{"message", "message"}, wsTestInputTypes(t, preparedC2))
	require.Equal(t, "warm context", gjson.GetBytes(preparedC2, "input.0.content.0.text").String())
	require.Equal(t, "business question", gjson.GetBytes(preparedC2, "input.1.content.0.text").String())
	require.False(t, gjson.GetBytes(preparedC2, "previous_response_id").Exists())
	wsTestReadCompleted(t, conn, "resp_C2")

	// Default lane warmup omits stream_id and chains the same way.
	fieldsW3 := map[string]any{"type": "response.create", "generate": false, "model": "gpt-test", "store": false}
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsW3))
	createdW3 := wsTestReadEventWhere(t, conn, "default warmup created", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.created"
	})
	completedW3 := wsTestReadEventWhere(t, conn, "default warmup completed", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.completed"
	})
	requireWarmupShape(createdW3, completedW3, "")
	fieldsC3 := wsTestBusinessFields("C3", "gpt-test", []any{wsTestTextMessage("after default warm")})
	fieldsC3["previous_response_id"] = completedW3.Get("response.id").String()
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsC3))
	requireWSExecStart(t, exec, "C3")
	require.Equal(t, []string{"message"}, wsTestInputTypes(t, exec.payload("C3")), "empty warmup contributes an empty checkpoint")
	wsTestReadCompleted(t, conn, "resp_C3")

	// Legacy generate:true is tolerated and never reaches the upstream body.
	fieldsG1 := wsTestBusinessFields("G1", "gpt-test", []any{wsTestTextMessage("real turn")})
	fieldsG1["generate"] = true
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsG1))
	requireWSExecStart(t, exec, "G1")
	require.False(t, gjson.GetBytes(exec.payload("G1"), "generate").Exists())
	wsTestReadCompleted(t, conn, "resp_G1")

	// Persistent warmup state needs a native upstream and is rejected scoped.
	fieldsW4 := map[string]any{"type": "response.create", "generate": false, "model": "gpt-test", "store": true, "stream_id": "warm"}
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsW4))
	event := wsTestReadErrorEvent(t, conn, "unsupported_warmup_state")
	require.Equal(t, "warm", event.Get("stream_id").String())

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_WarmupValidateCallback(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("V2", wsTestStream("resp_V2")...)
	type validatedWarmup struct{ stream, model string }
	validated := make(chan validatedWarmup, 4)
	options := OpenAIWSConnectionOptions{
		ValidateWarmup: func(_ context.Context, request OpenAIWSConnectionRequest) error {
			model := gjson.GetBytes(request.Payload, "model").String()
			validated <- validatedWarmup{stream: request.StreamID, model: model}
			switch model {
			case "gpt-blocked":
				return newOpenAIWSRequestError(403, "model_not_allowed", "model blocked for this key", "model")
			case "gpt-deny":
				return errors.New("plain deny")
			default:
				return nil
			}
		},
	}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	warmup := func(model string) {
		wsTestSend(t, conn, wsTestCreateFrame(t, map[string]any{
			"type":      "response.create",
			"generate":  false,
			"model":     model,
			"stream_id": "wv",
		}))
	}

	// A typed rejection reaches the lane untouched; no warmup event precedes it.
	warmup("gpt-blocked")
	first := wsTestReadEvent(t, conn)
	require.Equal(t, "error", first.Get("type").String())
	require.Equal(t, "model_not_allowed", first.Get("error.code").String())
	require.Equal(t, 403, int(first.Get("status").Int()))
	require.Equal(t, "wv", first.Get("stream_id").String())
	select {
	case got := <-validated:
		require.Equal(t, validatedWarmup{stream: "wv", model: "gpt-blocked"}, got)
	default:
		t.Fatal("ValidateWarmup was not invoked before the rejection")
	}

	// A generic error maps to a scoped 400 invalid_request_error.
	warmup("gpt-deny")
	event := wsTestReadErrorEvent(t, conn, "warmup_rejected")
	require.Equal(t, 400, int(event.Get("status").Int()))
	require.Equal(t, "invalid_request_error", event.Get("error.type").String())

	// An approved warmup publishes normally and the connection stays usable.
	warmup("gpt-test")
	wsTestReadEventWhere(t, conn, "approved warmup completed", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.completed"
	})
	fieldsV2 := wsTestBusinessFields("V2", "gpt-test", []any{wsTestTextMessage("ordinary turn")})
	fieldsV2["stream_id"] = "wv"
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsV2))
	requireWSExecStart(t, exec, "V2")
	wsTestReadCompleted(t, conn, "resp_V2")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_ForkPinningEvictionAndFailures(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("A1", wsTestStream("resp_A1", wsTestAssistantMessage("msg_a1", "a1 out"))...)
	exec.setEvents("F1", wsTestStream("resp_F1")...)
	exec.setEvents("A2", wsTestCreatedEvent("resp_A2"), wsTestFailedEvent("resp_A2"))
	exec.setEvents("A4", wsTestStream("resp_A4", wsTestAssistantMessage("msg_a4", "a4 out"))...)
	exec.setEvents("C1", wsTestCreatedEvent("resp_C1"), wsTestFailedEvent("resp_C1"))
	exec.setEvents("A5", wsTestStream("resp_A5")...)
	exec.setEvents("G2", wsTestStream("resp_G2")...)
	exec.setErr("A6", errors.New("upstream exploded"))
	exec.setEvents("A8", wsTestStream("resp_A8")...)
	exec.setGate("B1")
	exec.setEvents("B1", wsTestStream("resp_B1")...)
	exec.setEvents("A9", wsTestStream("resp_A9")...)
	h := newOpenAIWSConnTestHarness(t, nil, nil, OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	send := func(tag, streamID, previousID, text string) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage(text)})
		if streamID != "" {
			fields["stream_id"] = streamID
		}
		if previousID != "" {
			fields["previous_response_id"] = previousID
		}
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}

	// Turn 1 on the source lane.
	send("A1", "main", "", "a1")
	requireWSExecStart(t, exec, "A1")
	wsTestReadCompleted(t, conn, "resp_A1")

	// Publication-race regression: forking immediately after the observable
	// terminal frame must find a complete checkpoint — no scheduler delay.
	send("F1", "fork", "resp_A1", "fork question")
	requireWSExecStart(t, exec, "F1")
	preparedF1 := exec.payload("F1")
	require.Equal(t, []string{"message", "message", "message"}, wsTestInputTypes(t, preparedF1))
	require.Equal(t, "a1", gjson.GetBytes(preparedF1, "input.0.content.0.text").String())
	require.Equal(t, "msg_a1", gjson.GetBytes(preparedF1, "input.1.id").String())
	require.Equal(t, "fork question", gjson.GetBytes(preparedF1, "input.2.content.0.text").String())
	wsTestReadCompleted(t, conn, "resp_F1")

	// A failing same-lane continuation invalidates its referenced parent.
	send("A2", "main", "resp_A1", "a2")
	requireWSExecStart(t, exec, "A2")
	failedA2 := wsTestReadEventWhere(t, conn, "A2 failed", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.failed" && event.Get("response.id").String() == "resp_A2"
	})
	require.Equal(t, "main", failedA2.Get("stream_id").String())
	send("A3", "main", "resp_A1", "a3")
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("A3"))

	// A fresh chain re-pins the lane.
	send("A4", "main", "", "a4")
	requireWSExecStart(t, exec, "A4")
	wsTestReadCompleted(t, conn, "resp_A4")

	// A failing cross-lane fork preserves the shared parent.
	send("C1", "side", "resp_A4", "critic")
	requireWSExecStart(t, exec, "C1")
	wsTestReadEventWhere(t, conn, "C1 failed", func(event gjson.Result) bool {
		return event.Get("type").String() == "response.failed" && event.Get("response.id").String() == "resp_C1"
	})
	send("A5", "main", "resp_A4", "a5")
	requireWSExecStart(t, exec, "A5")
	preparedA5 := exec.payload("A5")
	require.Equal(t, []string{"message", "message", "message"}, wsTestInputTypes(t, preparedA5))
	require.Equal(t, "a4", gjson.GetBytes(preparedA5, "input.0.content.0.text").String())
	require.Equal(t, "a5", gjson.GetBytes(preparedA5, "input.2.content.0.text").String())
	wsTestReadCompleted(t, conn, "resp_A5")

	// Advancing the source lane evicted the older checkpoint.
	send("G1", "fork", "resp_A4", "stale fork")
	eventG1 := wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, "fork", eventG1.Get("stream_id").String())
	send("G2", "fork", "resp_A5", "fresh fork")
	requireWSExecStart(t, exec, "G2")
	wsTestReadCompleted(t, conn, "resp_G2")

	// An executor error surfaces as a scoped 502 and invalidates the parent.
	send("A6", "main", "resp_A5", "a6")
	requireWSExecStart(t, exec, "A6")
	eventA6 := wsTestReadErrorEvent(t, conn, "upstream_error")
	require.Equal(t, 502, int(eventA6.Get("status").Int()))
	require.Equal(t, "server_error", eventA6.Get("error.type").String())
	require.Equal(t, "main", eventA6.Get("stream_id").String())
	send("A7", "main", "resp_A5", "a7")
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("A7"))

	// A fork queued behind a busy lane may lose its parent when the source
	// lane advances before the fork starts (documented recovery race).
	send("A8", "main", "", "a8")
	requireWSExecStart(t, exec, "A8")
	wsTestReadCompleted(t, conn, "resp_A8")
	send("B1", "busyB", "", "b1")
	requireWSExecStart(t, exec, "B1")
	send("Q1", "busyB", "resp_A8", "queued fork")
	send("A9", "main", "resp_A8", "a9")
	requireWSExecStart(t, exec, "A9")
	wsTestReadCompleted(t, conn, "resp_A9")
	exec.releaseGate("B1")
	wsTestReadCompleted(t, conn, "resp_B1")
	eventQ1 := wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, "busyB", eventQ1.Get("stream_id").String())
	require.Equal(t, 0, exec.callCount("Q1"))

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_MemoryBoundaries(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setGate("M1")
	exec.setEvents("M1", wsTestStream("resp_M1")...)
	exec.setEvents("M2", wsTestStream("resp_M2")...)
	exec.setEvents("K1", wsTestStream("resp_K1")...)
	options := OpenAIWSConnectionOptions{MaxHistoryBytes: 4096, MaxActive: 2}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	send := func(tag, streamID, previousID, text string) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage(text)})
		fields["stream_id"] = streamID
		if previousID != "" {
			fields["previous_response_id"] = previousID
		}
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}

	// A request whose materialized input can never fit is rejected, not queued.
	send("BIG", "big", "", strings.Repeat("x", 5000))
	event := wsTestReadErrorEvent(t, conn, "context_too_large")
	require.Equal(t, "big", event.Get("stream_id").String())
	require.Equal(t, 413, int(event.Get("status").Int()))
	require.Equal(t, 0, exec.callCount("BIG"))

	// Active materialized inputs share one budget: M2 waits for M1's memory.
	send("M1", "m1", "", strings.Repeat("x", 2500))
	requireWSExecStart(t, exec, "M1")
	send("M2", "m2", "", strings.Repeat("y", 2000))
	requireWSNoExecStart(t, exec, 200*time.Millisecond)
	exec.releaseGate("M1")
	wsTestReadCompleted(t, conn, "resp_M1")
	requireWSExecStart(t, exec, "M2")
	wsTestReadCompleted(t, conn, "resp_M2")

	// Retained snapshots share one budget without cross-lane eviction: M1's
	// checkpoint stays pinned while M2's could not be retained.
	send("K1", "m1", "resp_M1", "k1")
	requireWSExecStart(t, exec, "K1")
	wsTestReadCompleted(t, conn, "resp_K1")
	send("K2", "m2", "resp_M2", "k2")
	eventK2 := wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, "m2", eventK2.Get("stream_id").String())
	require.Equal(t, 0, exec.callCount("K2"))

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_InvalidFramesAreRecoverable(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setGate("S1")
	exec.setEvents("S1", wsTestStream("resp_S1")...)
	exec.setEvents("T9", wsTestStream("resp_T9")...)
	type observedError struct{ code, stream string }
	observed := make(chan observedError, 16)
	options := OpenAIWSConnectionOptions{
		ObserveRequestError: func(request OpenAIWSConnectionRequest, requestErr *OpenAIWSRequestError) {
			observed <- observedError{code: requestErr.Code, stream: request.StreamID}
		},
	}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	fieldsS1 := wsTestBusinessFields("S1", "gpt-test", []any{wsTestTextMessage("hold")})
	fieldsS1["stream_id"] = "main"
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsS1))
	requireWSExecStart(t, exec, "S1")

	// Every invalid frame is a scoped recoverable error; none resets the socket
	// or interrupts the active execution on the addressed lane.
	wsTestSend(t, conn, "this is not json")
	wsTestSend(t, conn, `[1,2,3]`)
	wsTestSend(t, conn, `{"type":"response.delete","stream_id":"main"}`)
	wsTestSend(t, conn, `{"type":"response.steer","stream_id":"main","response_id":"resp_S1","input":[]}`)
	for _, want := range []observedError{
		{code: "invalid_json", stream: ""},
		{code: "invalid_json", stream: ""},
		{code: "unsupported_event", stream: "main"},
		{code: "steering_not_supported", stream: "main"},
	} {
		event := wsTestReadErrorEvent(t, conn, want.code)
		if want.stream == "" {
			require.False(t, event.Get("stream_id").Exists())
		} else {
			require.Equal(t, want.stream, event.Get("stream_id").String())
		}
		select {
		case got := <-observed:
			require.Equal(t, want, got)
		default:
			t.Fatalf("ObserveRequestError missing %q", want.code)
		}
	}

	// The steered execution was never canceled or regenerated.
	exec.releaseGate("S1")
	wsTestReadCompleted(t, conn, "resp_S1")

	fieldsT9 := wsTestBusinessFields("T9", "gpt-test", []any{wsTestTextMessage("after errors")})
	fieldsT9["stream_id"] = "main"
	wsTestSend(t, conn, wsTestCreateFrame(t, fieldsT9))
	requireWSExecStart(t, exec, "T9")
	wsTestReadCompleted(t, conn, "resp_T9")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
	outcome := h.requireOutcome(t)
	require.Equal(t, "client_close", outcome.Cause)
	require.Equal(t, int(coderws.StatusNormalClosure), outcome.CloseCode)
	require.Equal(t, 2, outcome.Requests, "rejected frames never enter the queue")
}

func TestRunOpenAIWSConnection_FirstMessageProvided(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("FC", wsTestStream("resp_FC")...)
	first := wsTestCreateFrame(t, map[string]any{
		"type":     "response.create",
		"generate": false,
		"model":    "gpt-test",
		"store":    false,
	})
	h := newOpenAIWSConnTestHarness(t, nil, []byte(first), OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	// The pre-read first message is processed before any client frame.
	created := wsTestReadEvent(t, conn)
	require.Equal(t, "response.created", created.Get("type").String())
	completed := wsTestReadEvent(t, conn)
	require.Equal(t, "response.completed", completed.Get("type").String())
	require.False(t, completed.Get("stream_id").Exists())

	fields := wsTestBusinessFields("FC", "gpt-test", []any{wsTestTextMessage("after warm")})
	fields["previous_response_id"] = completed.Get("response.id").String()
	wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	requireWSExecStart(t, exec, "FC")
	wsTestReadCompleted(t, conn, "resp_FC")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_InvalidFirstMessageStaysRecoverable(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("V1", wsTestStream("resp_V1")...)
	h := newOpenAIWSConnTestHarness(t, nil, []byte("garbage{"), OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	event := wsTestReadErrorEvent(t, conn, "invalid_json")
	require.False(t, event.Get("stream_id").Exists())

	wsTestSend(t, conn, wsTestCreateFrame(t, wsTestBusinessFields("V1", "gpt-test", []any{wsTestTextMessage("valid after garbage")})))
	requireWSExecStart(t, exec, "V1")
	wsTestReadCompleted(t, conn, "resp_V1")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_LifetimeLimit(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	options := OpenAIWSConnectionOptions{Lifetime: 300 * time.Millisecond}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	event := wsTestReadErrorEvent(t, conn, "websocket_connection_limit_reached")
	require.Equal(t, 400, int(event.Get("status").Int()))
	require.False(t, event.Get("stream_id").Exists())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, coderws.StatusGoingAway, coderws.CloseStatus(err))

	require.NoError(t, h.requireRunErr(t))
	outcome := h.requireOutcome(t)
	require.Equal(t, "connection_lifetime", outcome.Cause)
}

func TestRunOpenAIWSConnection_LifetimeCancelsBlockedWarmupAdmission(t *testing.T) {
	entered := make(chan struct{})
	exec := newOpenAIWSFakeExecutor()
	options := OpenAIWSConnectionOptions{
		Lifetime: 500 * time.Millisecond,
		ValidateWarmup: func(ctx context.Context, _ OpenAIWSConnectionRequest) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)
	wsTestSend(t, conn, `{"type":"response.create","model":"gpt-test","store":false,"generate":false,"input":[]}`)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("warmup admission did not start")
	}
	wsTestReadErrorEvent(t, conn, "websocket_connection_limit_reached")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Equal(t, coderws.StatusGoingAway, coderws.CloseStatus(err))
	require.NoError(t, h.requireRunErr(t))
	require.Equal(t, "connection_lifetime", h.requireOutcome(t).Cause)
}

func TestRunOpenAIWSConnection_FirstMessageTimeout(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	options := OpenAIWSConnectionOptions{FirstMessageTimeout: 200 * time.Millisecond}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, coderws.StatusPolicyViolation, coderws.CloseStatus(err))

	runErr := h.requireRunErr(t)
	require.ErrorIs(t, runErr, context.DeadlineExceeded)
	outcome := h.requireOutcome(t)
	require.Equal(t, "first_message_timeout", outcome.Cause)
}

func TestRunOpenAIWSConnection_IdleTimeout(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("I1", wsTestStream("resp_I1")...)
	options := OpenAIWSConnectionOptions{IdleTimeout: 200 * time.Millisecond}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	wsTestSend(t, conn, wsTestCreateFrame(t, wsTestBusinessFields("I1", "gpt-test", []any{wsTestTextMessage("one turn")})))
	requireWSExecStart(t, exec, "I1")
	wsTestReadCompleted(t, conn, "resp_I1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, coderws.StatusNormalClosure, coderws.CloseStatus(err))

	require.NoError(t, h.requireRunErr(t))
	outcome := h.requireOutcome(t)
	require.Equal(t, "idle_timeout", outcome.Cause)
}

func TestRunOpenAIWSConnection_SlowConsumerWriteFailure(t *testing.T) {
	big := []byte(`{"type":"response.output_text.delta","delta":"` + strings.Repeat("d", 1<<20) + `"}`)
	executor := func(ctx context.Context, _ OpenAIWSConnectionRequest, emit func([]byte) error) error {
		for {
			if err := emit(big); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}
	options := OpenAIWSConnectionOptions{WriteTimeout: 100 * time.Millisecond}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, executor)
	conn := h.dial(t)
	// The client never reads: kernel buffers fill, a frame write blocks, and
	// the bounded write timeout must tear down only this connection.
	wsTestSend(t, conn, wsTestCreateFrame(t, wsTestBusinessFields("S", "gpt-test", []any{wsTestTextMessage("flood")})))

	runErr := h.requireRunErr(t)
	require.Error(t, runErr)
	outcome := h.requireOutcome(t)
	require.Equal(t, "network_write_error", outcome.Cause)
	_ = conn.CloseNow()
}

func TestRunOpenAIWSConnection_ParentCancelShutdown(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setGate("C1")
	parent, cancelParent := context.WithCancel(context.Background())
	h := newOpenAIWSConnTestHarness(t, parent, nil, OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	wsTestSend(t, conn, wsTestCreateFrame(t, wsTestBusinessFields("C1", "gpt-test", []any{wsTestTextMessage("held")})))
	requireWSExecStart(t, exec, "C1")

	cancelParent()
	runErr := h.requireRunErr(t)
	require.ErrorIs(t, runErr, context.Canceled)
	// Cancellation joined the execution before the runtime returned.
	requireWSExecReturned(t, exec, "C1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	require.Error(t, err)
	require.Equal(t, coderws.StatusGoingAway, coderws.CloseStatus(err))

	outcome := h.requireOutcome(t)
	require.Equal(t, "connection_canceled", outcome.Cause)
	_ = conn.CloseNow()
}

func TestRunOpenAIWSConnection_ClientCloseCancelsAndJoins(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setGate("D1")
	h := newOpenAIWSConnTestHarness(t, nil, nil, OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	wsTestSend(t, conn, wsTestCreateFrame(t, wsTestBusinessFields("D1", "gpt-test", []any{wsTestTextMessage("held")})))
	requireWSExecStart(t, exec, "D1")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "bye"))
	require.NoError(t, h.requireRunErr(t))
	// The blocked execution was canceled and joined before the return.
	requireWSExecReturned(t, exec, "D1")

	outcome := h.requireOutcome(t)
	require.Equal(t, "client_close", outcome.Cause)
	require.Equal(t, int(coderws.StatusNormalClosure), outcome.CloseCode)
}

func TestRunOpenAIWSConnection_EarlyValidationInvalidatesSameLaneParent(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("M1", wsTestStream("resp_M1")...)
	exec.setEvents("M4", wsTestStream("resp_M4")...)
	h := newOpenAIWSConnTestHarness(t, nil, nil, OpenAIWSConnectionOptions{}, exec.run)
	conn := h.dial(t)

	send := func(tag string, extra map[string]any) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage("turn " + tag)})
		fields["stream_id"] = "main"
		for key, value := range extra {
			fields[key] = value
		}
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}

	send("M1", map[string]any{})
	wsTestReadCompleted(t, conn, "resp_M1")

	// A validation-time 4xx on a same-lane continuation evicts the referenced
	// parent, exactly like an execution failure.
	send("M2", map[string]any{"previous_response_id": "resp_M1", "generate": "yes"})
	event := wsTestReadErrorEvent(t, conn, "invalid_generate")
	require.Equal(t, "main", event.Get("stream_id").String())
	require.Equal(t, 0, exec.callCount("M2"))
	send("M3", map[string]any{"previous_response_id": "resp_M1"})
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("M3"))

	send("M4", map[string]any{})
	wsTestReadCompleted(t, conn, "resp_M4")
	send("M5", map[string]any{"previous_response_id": "resp_M4", "background": true})
	wsTestReadErrorEvent(t, conn, "unsupported_background")
	require.Equal(t, 0, exec.callCount("M5"))
	send("M6", map[string]any{"previous_response_id": "resp_M4"})
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("M6"))

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_QueueFullInvalidatesSameLaneParent(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setGate("X1")
	exec.setEvents("X1", wsTestStream("resp_X1")...)
	exec.setEvents("B1", wsTestStream("resp_B1")...)
	exec.setEvents("C1", wsTestStream("resp_C1")...)
	options := OpenAIWSConnectionOptions{MaxActive: 1, MaxQueuedRequests: 1}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	send := func(tag, streamID, previousID string) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage("turn " + tag)})
		fields["stream_id"] = streamID
		if previousID != "" {
			fields["previous_response_id"] = previousID
		}
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}

	send("B1", "b", "")
	requireWSExecStart(t, exec, "B1")
	wsTestReadCompleted(t, conn, "resp_B1")
	send("X1", "x", "")
	requireWSExecStart(t, exec, "X1")
	send("C1", "b", "")
	// The queue is full: a same-lane continuation rejected with 429 evicts its
	// referenced parent, while the queued fresh-chain request is unaffected.
	send("C2", "b", "resp_B1")
	event := wsTestReadErrorEvent(t, conn, "connection_queue_full")
	require.Equal(t, "b", event.Get("stream_id").String())
	require.Equal(t, 0, exec.callCount("C2"))

	exec.releaseGate("X1")
	wsTestReadCompleted(t, conn, "resp_X1")
	requireWSExecStart(t, exec, "C1")
	wsTestReadCompleted(t, conn, "resp_C1")

	send("C3", "b", "resp_B1")
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("C3"))

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}

func TestRunOpenAIWSConnection_OutputCollectionBound(t *testing.T) {
	exec := newOpenAIWSFakeExecutor()
	exec.setEvents("O0", wsTestStream("resp_O0")...)
	exec.setEvents("O1",
		wsTestCreatedEvent("resp_O1"),
		wsTestItemDoneEvent(wsTestAssistantMessage("msg_b1", strings.Repeat("b", 1500))),
		wsTestItemDoneEvent(wsTestAssistantMessage("msg_b2", strings.Repeat("b", 1500))),
		wsTestItemDoneEvent(wsTestAssistantMessage("msg_b3", strings.Repeat("b", 1500))),
		wsTestCompletedEvent("resp_O1"),
	)
	exec.setEvents("O4", wsTestStream("resp_O4")...)
	options := OpenAIWSConnectionOptions{MaxHistoryBytes: 4096}
	h := newOpenAIWSConnTestHarness(t, nil, nil, options, exec.run)
	conn := h.dial(t)

	send := func(tag, previousID string) {
		fields := wsTestBusinessFields(tag, "gpt-test", []any{wsTestTextMessage("turn " + tag)})
		fields["stream_id"] = "main"
		if previousID != "" {
			fields["previous_response_id"] = previousID
		}
		wsTestSend(t, conn, wsTestCreateFrame(t, fields))
	}

	send("O0", "")
	requireWSExecStart(t, exec, "O0")
	wsTestReadCompleted(t, conn, "resp_O0")

	// The stream is never truncated by the output collection budget: every
	// item event and the completed terminal still reach the client.
	send("O1", "resp_O0")
	requireWSExecStart(t, exec, "O1")
	itemEvents := 0
	completedO1 := wsTestReadEventWhere(t, conn, "O1 completed", func(event gjson.Result) bool {
		if event.Get("type").String() == "response.output_item.done" {
			itemEvents++
		}
		return event.Get("type").String() == "response.completed" && event.Get("response.id").String() == "resp_O1"
	})
	require.Equal(t, 3, itemEvents)
	require.Equal(t, "completed", completedO1.Get("response.status").String())

	// Overflow made the checkpoint unavailable rather than partially
	// recorded, and the lane advanced past its previous checkpoint.
	send("O2", "resp_O1")
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("O2"))
	send("O3", "resp_O0")
	wsTestReadErrorEvent(t, conn, "previous_response_not_found")
	require.Equal(t, 0, exec.callCount("O3"))

	// The reservation was released: a fresh turn runs and checkpoints again.
	send("O4", "")
	requireWSExecStart(t, exec, "O4")
	wsTestReadCompleted(t, conn, "resp_O4")

	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, h.requireRunErr(t))
}
