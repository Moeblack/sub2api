package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// These tests exercise the connection loop with independently driven peers.
// A write is recorded even when it fails, because its upstream outcome is then
// unknown and issuing it again could execute a tool twice.
type nativeRuntimeTestConn struct {
	reads      chan openAIWSNativeFrame
	writes     chan openAIWSNativeFrame
	readEvents chan []byte
	mu         sync.Mutex
	count      int
	failAt     int
	failGate   <-chan struct{}
}

func newNativeRuntimeTestConn() *nativeRuntimeTestConn {
	return &nativeRuntimeTestConn{
		reads: make(chan openAIWSNativeFrame, 256), writes: make(chan openAIWSNativeFrame, 256),
		readEvents: make(chan []byte, 256),
	}
}

func (c *nativeRuntimeTestConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	select {
	case frame := <-c.reads:
		c.readEvents <- append([]byte(nil), frame.payload...)
		return frame.messageType, frame.payload, frame.err
	case <-ctx.Done():
		return coderws.MessageText, nil, ctx.Err()
	}
}

func (c *nativeRuntimeTestConn) WriteFrame(ctx context.Context, kind coderws.MessageType, payload []byte) error {
	c.mu.Lock()
	c.count++
	fail := c.failAt > 0 && c.count == c.failAt
	gate := c.failGate
	c.mu.Unlock()
	select {
	case c.writes <- openAIWSNativeFrame{messageType: kind, payload: append([]byte(nil), payload...)}:
		if fail {
			if gate != nil {
				select {
				case <-gate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return errors.New("injected ambiguous write failure")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*nativeRuntimeTestConn) Close() error { return nil }

func (c *nativeRuntimeTestConn) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

type nativeRuntimeTestOutcome struct {
	turn   int
	result *OpenAIForwardResult
	err    error
}

type nativeRuntimeTestLifecycle struct {
	kind   string
	parent int
	turn   int
}

type nativeRuntimeTestHarness struct {
	client   *nativeRuntimeTestConn
	upstream *nativeRuntimeTestConn
	cancel   context.CancelFunc
	done     chan struct{}
	runErr   error
	settled  chan nativeRuntimeTestOutcome
	mu       sync.Mutex
	outcomes []nativeRuntimeTestOutcome
	leases   []nativeRuntimeTestLifecycle
}

func nativeRuntimeTestPrepare(number int, payload []byte) (*openAIWSNativeTurn, error) {
	requested := gjson.GetBytes(payload, "model").String()
	upstream := strings.Replace(requested, "alias-", "upstream-", 1)
	forwarded := append([]byte(nil), payload...)
	if upstream != requested {
		var err error
		forwarded, err = sjson.SetBytes(forwarded, "model", upstream)
		if err != nil {
			return nil, err
		}
	}
	return &openAIWSNativeTurn{
		number: number, payload: forwarded, requestModel: requested, upstreamModel: upstream,
		result: OpenAIForwardResult{
			Model: requested, UpstreamModel: openAIWSDifferentModel(requested, upstream), Stream: true, OpenAIWSMode: true,
		},
	}, nil
}

func newNativeRuntimeTestHarness(t *testing.T, first string, failUpstreamWrite int) *nativeRuntimeTestHarness {
	t.Helper()
	h := &nativeRuntimeTestHarness{
		client: newNativeRuntimeTestConn(), upstream: newNativeRuntimeTestConn(),
		done: make(chan struct{}), settled: make(chan nativeRuntimeTestOutcome, 256),
	}
	h.upstream.failAt = failUpstreamWrite
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	initial, err := nativeRuntimeTestPrepare(1, []byte(first))
	require.NoError(t, err)
	options := openAIWSNativeOptions{
		prepare: nativeRuntimeTestPrepare,
		beforeTurn: func(turn int) error {
			h.recordLease("before", 0, turn)
			return nil
		},
		continueTurn: func(parent, successor int) error {
			h.recordLease("continue", parent, successor)
			return nil
		},
		releaseTurn: func(turn int) { h.recordLease("release", 0, turn) },
		afterTurn: func(turn int, result *OpenAIForwardResult, err error) {
			outcome := nativeRuntimeTestOutcome{turn: turn, err: err}
			if result != nil {
				copy := *result
				outcome.result = &copy
			}
			h.mu.Lock()
			h.outcomes = append(h.outcomes, outcome)
			h.leases = append(h.leases, nativeRuntimeTestLifecycle{kind: "after", turn: turn})
			h.mu.Unlock()
			h.settled <- outcome
		},
		writeTimeout: 3 * time.Second,
	}
	go func() {
		h.runErr = runOpenAIWSNative(ctx, h.client, h.upstream, initial, options)
		close(h.done)
	}()
	t.Cleanup(func() { h.stop(t) })
	return h
}

func (h *nativeRuntimeTestHarness) recordLease(kind string, parent, turn int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.leases = append(h.leases, nativeRuntimeTestLifecycle{kind: kind, parent: parent, turn: turn})
}

func (h *nativeRuntimeTestHarness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Error("native runtime did not stop after cancellation")
	}
}

func nativeRuntimeTestSend(conn *nativeRuntimeTestConn, payload string) {
	conn.reads <- openAIWSNativeFrame{messageType: coderws.MessageText, payload: []byte(payload)}
}

func nativeRuntimeTestRead(t *testing.T, conn *nativeRuntimeTestConn) gjson.Result {
	t.Helper()
	select {
	case frame := <-conn.writes:
		require.Equal(t, coderws.MessageText, frame.messageType)
		require.True(t, gjson.ValidBytes(frame.payload), "invalid JSON: %s", frame.payload)
		return gjson.ParseBytes(frame.payload)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a native websocket frame")
		return gjson.Result{}
	}
}

func (h *nativeRuntimeTestHarness) upstreamEvent(t *testing.T, payload string) gjson.Result {
	t.Helper()
	nativeRuntimeTestSend(h.upstream, payload)
	return nativeRuntimeTestRead(t, h.client)
}

func (h *nativeRuntimeTestHarness) outcome(t *testing.T) nativeRuntimeTestOutcome {
	t.Helper()
	select {
	case result := <-h.settled:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for native response settlement")
		return nativeRuntimeTestOutcome{}
	}
}

func (h *nativeRuntimeTestHarness) snapshot() ([]nativeRuntimeTestOutcome, []nativeRuntimeTestLifecycle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nativeRuntimeTestOutcome(nil), h.outcomes...), append([]nativeRuntimeTestLifecycle(nil), h.leases...)
}

func nativeRuntimeTestCreated(lane, id, model string) string {
	return fmt.Sprintf(`{"type":"response.created","stream_id":%q,"response":{"id":%q,"model":%q,"status":"in_progress"}}`, lane, id, model)
}

func nativeRuntimeTestCompleted(lane, id, model string, input, output, cached int) string {
	return fmt.Sprintf(`{"type":"response.completed","stream_id":%q,"response":{"id":%q,"model":%q,"status":"completed","usage":{"input_tokens":%d,"output_tokens":%d,"input_tokens_details":{"cached_tokens":%d}}}}`, lane, id, model, input, output, cached)
}

// Official contract: same-lane creates remain FIFO while different lanes may
// interleave. Model rewrites and billing follow each response's own request.
func TestOpenAIWSNativeRuntime_InterleavedLanesKeepFIFOAndOwnUsage(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"a","model":"alias-a","input":"a1"}`, 0)
	require.Equal(t, "a1", nativeRuntimeTestRead(t, h.upstream).Get("input").String())
	nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"a","model":"alias-next","input":"a2"}`)
	nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"b","model":"alias-b","input":"b1"}`)
	bRequest := nativeRuntimeTestRead(t, h.upstream)
	require.Equal(t, "b1", bRequest.Get("input").String(), "a2 must remain queued while a1 is active")
	require.Equal(t, "upstream-b", bRequest.Get("model").String())

	aCreated := h.upstreamEvent(t, nativeRuntimeTestCreated("a", "resp_a1", "upstream-a"))
	bCreated := h.upstreamEvent(t, nativeRuntimeTestCreated("b", "resp_b1", "upstream-b"))
	require.Equal(t, "alias-a", aCreated.Get("response.model").String())
	require.Equal(t, "alias-b", bCreated.Get("response.model").String())
	h.upstreamEvent(t, nativeRuntimeTestCompleted("b", "resp_b1", "upstream-b", 101, 13, 17))
	b := h.outcome(t)
	require.NoError(t, b.err)
	require.NotNil(t, b.result)
	require.Equal(t, "resp_b1", b.result.RequestID)
	require.Equal(t, "alias-b", b.result.Model)
	require.Equal(t, "upstream-b", b.result.UpstreamResponseModel)
	require.Equal(t, OpenAIUsage{InputTokens: 101, OutputTokens: 13, CacheReadInputTokens: 17}, b.result.Usage)
	require.Equal(t, 2, h.upstream.writeCount(), "finishing b must not release a's queue")

	h.upstreamEvent(t, nativeRuntimeTestCompleted("a", "resp_a1", "upstream-a", 211, 23, 31))
	a1 := h.outcome(t)
	require.Equal(t, "alias-a", a1.result.Model)
	require.Equal(t, OpenAIUsage{InputTokens: 211, OutputTokens: 23, CacheReadInputTokens: 31}, a1.result.Usage)
	next := nativeRuntimeTestRead(t, h.upstream)
	require.Equal(t, "a2", next.Get("input").String())
	require.Equal(t, "upstream-next", next.Get("model").String())
	h.upstreamEvent(t, nativeRuntimeTestCreated("a", "resp_a2", "upstream-next"))
	h.upstreamEvent(t, nativeRuntimeTestCompleted("a", "resp_a2", "upstream-next", 307, 37, 41))
	a2 := h.outcome(t)
	require.Equal(t, "alias-next", a2.result.Model)
	require.Equal(t, OpenAIUsage{InputTokens: 307, OutputTokens: 37, CacheReadInputTokens: 41}, a2.result.Usage)
	require.NotEqual(t, a1.turn, a2.turn)
	h.stop(t)
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, 3)
}

func TestOpenAIWSNativeRuntime_SteeringSuccessorIsBilledOnceWithoutFabricatedCreate(t *testing.T) {
	for _, parentTerminal := range []string{"response.incomplete", "response.completed"} {
		t.Run(parentTerminal, func(t *testing.T) {
			h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"draft"}`, 0)
			nativeRuntimeTestRead(t, h.upstream)
			h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_parent", "gpt-6-astra"))
			steer := `{"type":"response.steer","previous_response_id":"resp_parent","input":[{"role":"user","content":[{"type":"input_text","text":"keep it small"}]}]}`
			nativeRuntimeTestSend(h.client, steer)
			require.Equal(t, steer, nativeRuntimeTestRead(t, h.upstream).Raw)
			h.upstreamEvent(t, `{"type":"response.steer.accepted","stream_id":"main","steer":{"id":"steer_1","previous_response_id":"resp_parent"}}`)
			terminal := fmt.Sprintf(`{"type":%q,"stream_id":"main","response":{"id":"resp_parent","model":"gpt-6-astra","usage":{"input_tokens":11,"output_tokens":3}}}`, parentTerminal)
			if parentTerminal == "response.incomplete" {
				var err error
				terminal, err = sjson.Set(terminal, "response.incomplete_details.reason", "steered")
				require.NoError(t, err)
			}
			h.upstreamEvent(t, terminal)
			parent := h.outcome(t)
			require.NoError(t, parent.err)
			require.Equal(t, "resp_parent", parent.result.RequestID)
			h.upstreamEvent(t, `{"type":"response.created","stream_id":"main","response":{"id":"resp_successor","previous_response_id":"resp_parent","model":"gpt-6-astra","status":"in_progress"}}`)
			// A late parent terminal must never settle or overwrite its successor.
			h.upstreamEvent(t, terminal)
			h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_successor", "gpt-6-astra", 19, 7, 5))
			successor := h.outcome(t)
			require.NoError(t, successor.err)
			require.Equal(t, "resp_successor", successor.result.RequestID)
			require.Equal(t, OpenAIUsage{InputTokens: 19, OutputTokens: 7, CacheReadInputTokens: 5}, successor.result.Usage)
			require.NotEqual(t, parent.turn, successor.turn)
			h.stop(t)
			require.Equal(t, 2, h.upstream.writeCount(), "only the client's create and steer may be written upstream")
			outcomes, leases := h.snapshot()
			require.Len(t, outcomes, 2, "parent and successor each settle exactly once")
			require.Equal(t, []nativeRuntimeTestLifecycle{
				{kind: "continue", parent: parent.turn, turn: successor.turn},
				{kind: "after", turn: parent.turn},
				{kind: "after", turn: successor.turn},
			}, leases, "reserve the successor before releasing its parent")
		})
	}
}

func TestOpenAIWSNativeRuntime_SteeringToolFollowupPreservesOpaqueInputExactlyOnce(t *testing.T) {
	for _, pendingFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("pending_before_followup_%t", pendingFirst), func(t *testing.T) {
			h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","instructions":"original","input":"inspect"}`, 0)
			nativeRuntimeTestRead(t, h.upstream)
			h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_tool", "gpt-6-astra"))
			nativeRuntimeTestSend(h.client, `{"type":"response.steer","previous_response_id":"resp_tool","input":"also explain the result"}`)
			nativeRuntimeTestRead(t, h.upstream)
			h.upstreamEvent(t, `{"type":"response.steer.accepted","stream_id":"main","steer":{"id":"steer_tool","previous_response_id":"resp_tool"}}`)
			h.upstreamEvent(t, `{"type":"response.completed","stream_id":"main","response":{"id":"resp_tool","model":"gpt-6-astra","output":[{"type":"function_call","call_id":"call_opaque:worker/42","name":"get_status","arguments":"{}"}],"usage":{"input_tokens":5,"output_tokens":2}}}`)
			parent := h.outcome(t)
			if pendingFirst {
				h.upstreamEvent(t, `{"type":"response.steer.pending","stream_id":"main","steer":{"id":"steer_tool","previous_response_id":"resp_tool"},"reason":"waiting_for_required_input","required_input":[{"type":"function_call_output","call_id":"call_opaque:worker/42","name":"get_status"}]}`)
			}
			// The client sends saved tool output and opaque history once. The
			// server owns accepted steering; the gateway must not add it again.
			followup := `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","previous_response_id":"resp_tool","instructions":"explain, do not act","tools":[],"input":[{"type":"reasoning","id":"rs_opaque","encrypted_content":"opaque/A+Bc==","summary":[]},{"type":"function_call_output","call_id":"call_opaque:worker/42","output":"saved tool result"},{"role":"user","content":"wait for approval"}]}`
			nativeRuntimeTestSend(h.client, followup)
			require.Equal(t, followup, nativeRuntimeTestRead(t, h.upstream).Raw)
			h.upstreamEvent(t, `{"type":"response.created","stream_id":"main","response":{"id":"resp_followup","previous_response_id":"resp_tool","model":"gpt-6-astra"}}`)
			h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_followup", "gpt-6-astra", 17, 3, 0))
			followupResult := h.outcome(t)
			require.Equal(t, "resp_followup", followupResult.result.RequestID)
			require.NotEqual(t, parent.turn, followupResult.turn)
			h.stop(t)
			require.Equal(t, 3, h.upstream.writeCount(), "exactly one create, one steer, and one explicit tool followup")
			outcomes, leases := h.snapshot()
			require.Len(t, outcomes, 2)
			releases := 0
			for _, event := range leases {
				if event.kind == "release" {
					releases++
				}
			}
			if pendingFirst {
				require.Equal(t, 1, releases, "pending releases only its unused automatic successor reservation")
			} else {
				require.Zero(t, releases, "an early explicit followup consumes the existing reservation")
			}
		})
	}
}

func TestOpenAIWSNativeRuntime_AsyncInjectionPreservesCallIDsAndEvents(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"agent","model":"gpt-6-astra","input":"work","tools":[{"type":"function","name":"lookup","async":true}]}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("agent", "resp_async", "gpt-6-astra"))
	tool := `{"type":"response.output_item.done","stream_id":"agent","response_id":"resp_async","item":{"type":"function_call","id":"fc_original","call_id":"call_original:task/7","name":"lookup","arguments":"{}","async":true}}`
	require.Equal(t, tool, h.upstreamEvent(t, tool).Raw)
	inject := `{"type":"response.inject","response_id":"resp_async","input":[{"type":"function_call_output","call_id":"call_original:task/7","output":"opaque result","status":"completed"}]}`
	nativeRuntimeTestSend(h.client, inject)
	require.Equal(t, inject, nativeRuntimeTestRead(t, h.upstream).Raw)
	created := `{"type":"response.inject.created","stream_id":"agent","response_id":"resp_async","input":[{"type":"function_call_output","call_id":"call_original:task/7"}]}`
	require.Equal(t, created, h.upstreamEvent(t, created).Raw)
	h.upstreamEvent(t, nativeRuntimeTestCompleted("agent", "resp_async", "gpt-6-astra", 29, 11, 0))
	result := h.outcome(t)
	require.Equal(t, "resp_async", result.result.RequestID)
	h.stop(t)
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, 1, "auxiliary inject events must not create billable turns")
	require.Equal(t, 2, h.upstream.writeCount())
}

func TestOpenAIWSNativeRuntime_OrdinaryCreateCannotConsumeSteeringReservation(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"draft"}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_parent", "gpt-6-astra"))
	nativeRuntimeTestSend(h.client, `{"type":"response.steer","previous_response_id":"resp_parent","input":"add references"}`)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, `{"type":"response.steer.accepted","stream_id":"main","steer":{"id":"steer_reserved","previous_response_id":"resp_parent"}}`)
	h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_parent", "gpt-6-astra", 5, 2, 0))
	parent := h.outcome(t)
	// A user-only create is not the required tool result for a pending steer.
	// It must wait even when it references the same parent. The independent
	// lane acts as a barrier proving both client frames have been processed.
	nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"main","model":"alias-next","previous_response_id":"resp_parent","instructions":"different request settings","input":"start another task"}`)
	nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"barrier","model":"gpt-6-astra","input":"independent"}`)
	barrier := nativeRuntimeTestRead(t, h.upstream)
	require.Equal(t, "barrier", barrier.Get("stream_id").String(), "ordinary user input must not consume the automatic successor's reservation")
	h.upstreamEvent(t, nativeRuntimeTestCreated("barrier", "resp_barrier", "gpt-6-astra"))
	h.upstreamEvent(t, nativeRuntimeTestCompleted("barrier", "resp_barrier", "gpt-6-astra", 7, 3, 0))
	h.outcome(t)
	h.upstreamEvent(t, `{"type":"response.created","stream_id":"main","response":{"id":"resp_automatic","previous_response_id":"resp_parent","model":"gpt-6-astra"}}`)
	require.Equal(t, 3, h.upstream.writeCount(), "automatic steering must not synthesize or send the queued ordinary create")
	h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_automatic", "gpt-6-astra", 11, 5, 0))
	successor := h.outcome(t)
	require.NoError(t, successor.err)
	require.Equal(t, "resp_automatic", successor.result.RequestID)
	require.Equal(t, "gpt-6-astra", successor.result.Model, "automatic successor inherits its parent's settings")
	require.NotEqual(t, parent.turn, successor.turn)
	h.stop(t)
}

func TestOpenAIWSNativeRuntime_UnknownControlTargetNeverReachesUpstream(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"work"}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_owned", "gpt-6-astra"))
	for _, payload := range []string{
		`{"type":"response.steer","previous_response_id":"resp_other_connection","input":"change it"}`,
		`{"type":"response.inject","response_id":"resp_other_connection","input":[{"type":"function_call_output","call_id":"call_foreign","output":"wrong session"}]}`,
	} {
		nativeRuntimeTestSend(h.client, payload)
		failure := nativeRuntimeTestRead(t, h.client)
		require.Equal(t, gjson.Get(payload, "type").String()+".failed", failure.Get("type").String())
		require.Equal(t, "response_not_found", failure.Get("error.code").String())
		returnedInput := failure.Get("input")
		if !returnedInput.Exists() {
			returnedInput = failure.Get("steer.input")
		}
		require.JSONEq(t, gjson.Get(payload, "input").Raw, returnedInput.Raw)
	}
	h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_owned", "gpt-6-astra", 7, 3, 0))
	h.outcome(t)
	h.stop(t)
	require.Equal(t, 1, h.upstream.writeCount())
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, 1)
}

func TestOpenAIWSNativeRuntime_CompletedInjectionFailureKeepsItsLaneAndCallID(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"agent","model":"gpt-6-astra","input":"work"}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("agent", "resp_finished", "gpt-6-astra"))
	h.upstreamEvent(t, nativeRuntimeTestCompleted("agent", "resp_finished", "gpt-6-astra", 7, 3, 0))
	h.outcome(t)
	inject := `{"type":"response.inject","response_id":"resp_finished","input":[{"type":"function_call_output","call_id":"call_late:task/9","output":"saved result"}]}`
	nativeRuntimeTestSend(h.client, inject)
	failure := nativeRuntimeTestRead(t, h.client)
	require.Equal(t, "response.inject.failed", failure.Get("type").String())
	require.Equal(t, "response_not_found", failure.Get("error.code").String())
	require.Equal(t, "agent", failure.Get("stream_id").String(), "named-lane clients must be able to route local control failures")
	require.Equal(t, "resp_finished", failure.Get("response_id").String())
	require.JSONEq(t, gjson.Get(inject, "input").Raw, failure.Get("input").Raw)
	h.stop(t)
	require.Equal(t, 1, h.upstream.writeCount(), "late tool outputs cannot target a finished execution")
}

func TestOpenAIWSNativeRuntime_BoundedQueueRejectsOnlyExcessAndPreservesFIFO(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"first"}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_0", "gpt-6-astra"))
	for i := 1; i <= openAIWSMaxQueuedRequests+1; i++ {
		nativeRuntimeTestSend(h.client, fmt.Sprintf(`{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":%q}`, fmt.Sprintf("queued_%d", i)))
	}
	rejection := nativeRuntimeTestRead(t, h.client)
	require.Equal(t, "websocket_queue_limit_reached", rejection.Get("error.code").String())
	require.Equal(t, "main", rejection.Get("stream_id").String())
	require.Equal(t, 1, h.upstream.writeCount(), "queued requests cannot overlap their active lane")
	for i := 0; i <= openAIWSMaxQueuedRequests; i++ {
		h.upstreamEvent(t, nativeRuntimeTestCompleted("main", fmt.Sprintf("resp_%d", i), "gpt-6-astra", i+1, 1, 0))
		h.outcome(t)
		if i < openAIWSMaxQueuedRequests {
			next := nativeRuntimeTestRead(t, h.upstream)
			require.Equal(t, fmt.Sprintf("queued_%d", i+1), next.Get("input").String())
			h.upstreamEvent(t, nativeRuntimeTestCreated("main", fmt.Sprintf("resp_%d", i+1), "gpt-6-astra"))
		}
	}
	h.stop(t)
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, openAIWSMaxQueuedRequests+1, "rejected request must not acquire a lease or produce a bill")
	require.Equal(t, openAIWSMaxQueuedRequests+1, h.upstream.writeCount())
}

func TestOpenAIWSNativeRuntime_AmbiguousWriteIsNeverReplayed(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("write_%d", failAt), func(t *testing.T) {
			h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"first"}`, failAt)
			nativeRuntimeTestRead(t, h.upstream)
			if failAt == 2 {
				h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_first", "gpt-6-astra"))
				h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_first", "gpt-6-astra", 5, 2, 0))
				h.outcome(t)
				nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","previous_response_id":"resp_first","input":[{"type":"function_call_output","call_id":"call_do_not_execute_twice","output":"already executed"}]}`)
				nativeRuntimeTestRead(t, h.upstream)
			}
			select {
			case <-h.done:
			case <-time.After(3 * time.Second):
				t.Fatal("runtime did not terminate after an ambiguous upstream write")
			}
			require.ErrorContains(t, h.runErr, "outcome unknown")
			require.Equal(t, failAt, h.upstream.writeCount(), "an uncertain write must not be replayed")
			outcomes, _ := h.snapshot()
			require.Len(t, outcomes, failAt)
			require.Nil(t, outcomes[len(outcomes)-1].result, "uncertain execution cannot fabricate authoritative usage")
			require.Error(t, outcomes[len(outcomes)-1].err)
		})
	}
}

func TestOpenAIWSNativeRuntime_TerminalClientWriteFailureStillRecordsAuthoritativeUsage(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"work"}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_finished", "gpt-6-astra"))
	h.client.mu.Lock()
	h.client.failAt = h.client.count + 1
	h.client.mu.Unlock()
	h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_finished", "gpt-6-astra", 43, 17, 11))
	result := h.outcome(t)
	require.NoError(t, result.err)
	require.NotNil(t, result.result)
	require.Equal(t, "resp_finished", result.result.RequestID)
	require.Equal(t, OpenAIUsage{InputTokens: 43, OutputTokens: 17, CacheReadInputTokens: 11}, result.result.Usage)
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not stop after its downstream write failed")
	}
	require.Error(t, h.runErr)
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, 1, "socket cleanup cannot settle an already-recorded terminal twice")
}

func TestOpenAIWSNativeRuntime_InterleavedImagesAreCountedPerResponse(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"a","model":"alias-a","input":"draw a","tools":[{"type":"image_generation"}]}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"b","model":"alias-b","input":"draw b","tools":[{"type":"image_generation"}]}`)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("a", "resp_image_a", "upstream-a"))
	h.upstreamEvent(t, nativeRuntimeTestCreated("b", "resp_image_b", "upstream-b"))
	// Reusing an item ID on another response must not share its deduplication
	// state or size; repeating it within one response must count only once.
	aImage := `{"type":"response.output_item.done","stream_id":"a","response_id":"resp_image_a","item":{"type":"image_generation_call","id":"ig_shared","result":"image-a","status":"completed","size":"1024x1024"}}`
	bImage := `{"type":"response.output_item.done","stream_id":"b","response_id":"resp_image_b","item":{"type":"image_generation_call","id":"ig_shared","result":"image-b","status":"completed","size":"1536x1024"}}`
	h.upstreamEvent(t, aImage)
	h.upstreamEvent(t, bImage)
	h.upstreamEvent(t, aImage)
	h.upstreamEvent(t, `{"type":"response.output_item.done","stream_id":"b","response_id":"resp_image_b","item":{"type":"image_generation_call","id":"ig_b2","result":"image-b2","status":"completed","size":"1024x1536"}}`)
	h.upstreamEvent(t, `{"type":"response.completed","stream_id":"b","response":{"id":"resp_image_b","model":"upstream-b","output":[{"type":"image_generation_call","id":"ig_shared","result":"image-b","status":"completed","size":"1536x1024"},{"type":"image_generation_call","id":"ig_b2","result":"image-b2","status":"completed","size":"1024x1536"}],"usage":{"input_tokens":13,"output_tokens":5}}}`)
	b := h.outcome(t)
	require.NoError(t, b.err)
	require.NotNil(t, b.result)
	require.Equal(t, "resp_image_b", b.result.ResponseID)
	require.Equal(t, b.result.ResponseID, b.result.RequestID)
	require.Equal(t, 2, b.result.ImageCount)
	require.Equal(t, []string{"1536x1024", "1024x1536"}, b.result.ImageOutputSizes)
	require.Equal(t, "alias-b", b.result.Model)
	h.upstreamEvent(t, `{"type":"response.completed","stream_id":"a","response":{"id":"resp_image_a","model":"upstream-a","output":[{"type":"image_generation_call","id":"ig_shared","result":"image-a","status":"completed","size":"1024x1024"}],"usage":{"input_tokens":7,"output_tokens":3}}}`)
	a := h.outcome(t)
	require.NoError(t, a.err)
	require.NotNil(t, a.result)
	require.Equal(t, "resp_image_a", a.result.ResponseID)
	require.Equal(t, a.result.ResponseID, a.result.RequestID)
	require.Equal(t, 1, a.result.ImageCount)
	require.Equal(t, []string{"1024x1024"}, a.result.ImageOutputSizes)
	require.Equal(t, "alias-a", a.result.Model)
	h.stop(t)
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, 2)
}

func TestOpenAIWSNativeRuntime_DisconnectRetainsObservedUsageAndImages(t *testing.T) {
	for _, emitError := range []bool{false, true} {
		t.Run(fmt.Sprintf("upstream_error_before_disconnect_%t", emitError), func(t *testing.T) {
			h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"alias-images","input":"draw","tools":[{"type":"image_generation"}]}`, 0)
			nativeRuntimeTestRead(t, h.upstream)
			h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_partial", "upstream-images"))
			h.upstreamEvent(t, `{"type":"response.in_progress","stream_id":"main","response":{"id":"resp_partial","model":"upstream-images","usage":{"input_tokens":17,"output_tokens":7,"input_tokens_details":{"cached_tokens":5}}}}`)
			h.upstreamEvent(t, `{"type":"response.output_item.done","stream_id":"main","response_id":"resp_partial","item":{"type":"image_generation_call","id":"ig_partial","status":"completed","result":"already-delivered-image","size":"1024x1024"}}`)
			if emitError {
				h.upstreamEvent(t, `{"type":"error","stream_id":"main","response_id":"resp_partial","error":{"type":"server_error","code":"server_error","message":"upstream failed after partial output"}}`)
			}
			h.upstream.reads <- openAIWSNativeFrame{err: io.ErrUnexpectedEOF}
			partial := h.outcome(t)
			require.Error(t, partial.err)
			require.NotNil(t, partial.result, "known usage and delivered images survive an unclean upstream close")
			require.Equal(t, "resp_partial", partial.result.RequestID)
			require.Equal(t, "resp_partial", partial.result.ResponseID)
			require.Equal(t, "alias-images", partial.result.Model)
			require.Equal(t, OpenAIUsage{InputTokens: 17, OutputTokens: 7, CacheReadInputTokens: 5}, partial.result.Usage)
			require.Equal(t, 1, partial.result.ImageCount)
			require.Equal(t, []string{"1024x1024"}, partial.result.ImageOutputSizes)
			require.Empty(t, partial.result.UpstreamTerminalEvent, "an unclean close must not invent a terminal response")
			h.stop(t)
			outcomes, _ := h.snapshot()
			require.Len(t, outcomes, 1)
			require.Equal(t, 1, h.upstream.writeCount(), "partial execution is never replayed")
		})
	}
}

func TestOpenAIWSNativeRuntime_DuplicateEnvelopeKeysCannotChangeOwnership(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		param   string
	}{
		{"type", `{"type":"response.create","type":"response.inject","response_id":"resp_owned","model":"gpt-6-astra","input":"ambiguous"}`, "type"},
		{"model", `{"type":"response.create","stream_id":"other","model":"gpt-6-astra","model":"foreign-model","input":"ambiguous"}`, "model"},
		{"stream_id", `{"type":"response.create","stream_id":"main","stream_id":"foreign","model":"gpt-6-astra","input":"ambiguous"}`, "stream_id"},
		{"previous_response_id", `{"type":"response.steer","previous_response_id":"resp_owned","previous_response_id":"resp_foreign","input":"ambiguous"}`, "previous_response_id"},
		{"response_id", `{"type":"response.inject","response_id":"resp_owned","response_id":"resp_foreign","input":[{"type":"function_call_output","call_id":"call_owned","output":"ambiguous"}]}`, "response_id"},
		{"escaped_response_id", `{"type":"response.inject","response_id":"resp_owned","response_\u0069d":"resp_foreign","input":[{"type":"function_call_output","call_id":"call_owned","output":"ambiguous"}]}`, "response_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"work"}`, 0)
			nativeRuntimeTestRead(t, h.upstream)
			h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_owned", "gpt-6-astra"))
			nativeRuntimeTestSend(h.client, tc.payload)
			failure := nativeRuntimeTestRead(t, h.client)
			require.Equal(t, "error", failure.Get("type").String())
			require.Equal(t, "invalid_request_error", failure.Get("error.code").String())
			require.Equal(t, tc.param, failure.Get("error.param").String())
			require.Equal(t, 1, h.upstream.writeCount(), "ownership cannot be checked against one key then executed against its duplicate")
			h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_owned", "gpt-6-astra", 5, 2, 0))
			h.outcome(t)
			h.stop(t)
			outcomes, leases := h.snapshot()
			require.Len(t, outcomes, 1)
			require.Equal(t, []nativeRuntimeTestLifecycle{{kind: "after", turn: 1}}, leases)
		})
	}
}

func TestOpenAIWSNativeRuntime_DuplicateInitialEnvelopeNeverWritesUpstream(t *testing.T) {
	first := `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","previous_response_id":"resp_owned","previous_response_id":"resp_foreign","input":"work"}`
	h := newNativeRuntimeTestHarness(t, first, 0)
	failure := nativeRuntimeTestRead(t, h.client)
	require.Equal(t, "error", failure.Get("type").String())
	require.Equal(t, "previous_response_id", failure.Get("error.param").String())
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("invalid initial envelope did not stop before forwarding")
	}
	require.Error(t, h.runErr)
	require.Zero(t, h.upstream.writeCount())
}

func TestOpenAIWSNativeRuntime_SteeringFailureBeforeAcceptanceLeavesNoReservation(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"work"}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_parent", "gpt-6-astra"))
	nativeRuntimeTestSend(h.client, `{"type":"response.steer","previous_response_id":"resp_parent","input":"change direction"}`)
	nativeRuntimeTestRead(t, h.upstream)
	// The server can allocate a steer ID while rejecting initial admission,
	// without ever sending response.steer.accepted for that submission.
	h.upstreamEvent(t, `{"type":"response.steer.failed","stream_id":"main","steer":{"id":"steer_rejected_before_acceptance","previous_response_id":"resp_parent","input":"change direction"},"error":{"code":"steering_not_supported","type":"invalid_request_error","message":"steering unavailable"}}`)
	nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","previous_response_id":"resp_parent","input":"ordinary followup"}`)
	h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_parent", "gpt-6-astra", 7, 3, 0))
	h.outcome(t)
	next := nativeRuntimeTestRead(t, h.upstream)
	require.Equal(t, "ordinary followup", next.Get("input").String(), "a rejected steer cannot leave a phantom successor reservation")
	h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_followup", "gpt-6-astra"))
	h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_followup", "gpt-6-astra", 11, 5, 0))
	h.outcome(t)
	h.stop(t)
	outcomes, leases := h.snapshot()
	require.Len(t, outcomes, 2)
	for _, event := range leases {
		require.NotEqual(t, "continue", event.kind, "failed steering never acquires an automatic successor slot")
		require.NotEqual(t, "release", event.kind, "there is no unused successor reservation to release")
	}
	require.Equal(t, 3, h.upstream.writeCount())
}

func TestOpenAIWSNativeRuntime_ClientWriteFailureDrainsTurnsWithoutForwardingBufferedControls(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"a","model":"alias-a","input":"work a"}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"b","model":"alias-b","input":"work b"}`)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("a", "resp_a", "upstream-a"))
	h.upstreamEvent(t, nativeRuntimeTestCreated("b", "resp_b", "upstream-b"))

	// Hold the failing write while the client reader buffers controls. This
	// reproduces the race without depending on which peer goroutine runs first.
	gate := make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	h.client.mu.Lock()
	h.client.failAt = h.client.count + 1
	h.client.failGate = gate
	h.client.mu.Unlock()
	h.upstreamEvent(t, `{"type":"response.output_text.delta","stream_id":"a","response_id":"resp_a","delta":"client disappeared"}`)
	nativeRuntimeTestSend(h.client, `{"type":"response.steer","previous_response_id":"resp_a","input":"must not run"}`)
	nativeRuntimeTestSend(h.client, `{"type":"response.inject","response_id":"resp_b","input":[{"type":"function_call_output","call_id":"call_buffered","output":"must not inject"}]}`)
	buffered := make(map[string]bool)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(buffered) != 2 {
		select {
		case payload := <-h.client.readEvents:
			kind := gjson.GetBytes(payload, "type").String()
			if kind == "response.steer" || kind == "response.inject" {
				buffered[kind] = true
			}
		case <-deadline.C:
			t.Fatal("client control frames were not buffered during the failing write")
		}
	}
	release.Do(func() { close(gate) })
	nativeRuntimeTestSend(h.upstream, nativeRuntimeTestCompleted("b", "resp_b", "upstream-b", 19, 7, 5))
	nativeRuntimeTestSend(h.upstream, nativeRuntimeTestCompleted("a", "resp_a", "upstream-a", 11, 3, 2))
	b := h.outcome(t)
	a := h.outcome(t)
	for _, outcome := range []nativeRuntimeTestOutcome{a, b} {
		require.NoError(t, outcome.err)
		require.NotNil(t, outcome.result)
		require.True(t, outcome.result.ClientDisconnect)
		require.Equal(t, "response.completed", outcome.result.UpstreamTerminalEvent)
	}
	require.Equal(t, "resp_b", b.result.ResponseID)
	require.Equal(t, OpenAIUsage{InputTokens: 19, OutputTokens: 7, CacheReadInputTokens: 5}, b.result.Usage)
	require.Equal(t, "resp_a", a.result.ResponseID)
	require.Equal(t, OpenAIUsage{InputTokens: 11, OutputTokens: 3, CacheReadInputTokens: 2}, a.result.Usage)
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("draining runtime did not stop after both upstream turns finished")
	}
	require.ErrorContains(t, h.runErr, "injected ambiguous write failure")
	require.Equal(t, 2, h.upstream.writeCount(), "buffered steer and inject must never be forwarded after client write failure")
	require.Equal(t, 3, h.client.writeCount(), "terminal draining must suppress further writes to the disconnected client")
	outcomes, leases := h.snapshot()
	require.Len(t, outcomes, 2)
	for _, event := range leases {
		require.NotEqual(t, "continue", event.kind, "unforwarded steering cannot reserve a successor")
	}
}

func TestOpenAIWSNativeRuntime_InitialControlNeverForwardsEvenWithModel(t *testing.T) {
	for _, kind := range []string{"response.steer", "response.inject"} {
		t.Run(kind, func(t *testing.T) {
			first := fmt.Sprintf(`{"type":%q,"model":"gpt-6-astra","previous_response_id":"resp_foreign","response_id":"resp_foreign","input":"not a create"}`, kind)
			// The adapter rejects the control before touching the provided socket,
			// credentials, or dialer. The runtime independently enforces the rule.
			adapterErr := (&OpenAIGatewayService{}).proxyResponsesWebSocketV2PassthroughAttempt(
				context.Background(), nil, &coderws.Conn{}, &Account{}, "unused", []byte(first),
				&OpenAIWSIngressHooks{NativeResponses: true}, OpenAIWSProtocolDecision{},
			)
			require.ErrorContains(t, adapterErr, "first native websocket event must be response.create")
			h := newNativeRuntimeTestHarness(t, first, 0)
			failure := nativeRuntimeTestRead(t, h.client)
			require.Equal(t, "error", failure.Get("type").String())
			require.Equal(t, "type", failure.Get("error.param").String())
			select {
			case <-h.done:
			case <-time.After(3 * time.Second):
				t.Fatal("initial control was not rejected before forwarding")
			}
			require.Error(t, h.runErr)
			require.Zero(t, h.upstream.writeCount())
		})
	}
}

func TestOpenAIWSNativeRuntime_MissingTerminalIDCannotSettleTheNextTurn(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"first"}`, 0)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_a1", "gpt-6-astra"))
	h.upstreamEvent(t, nativeRuntimeTestCompleted("main", "resp_a1", "gpt-6-astra", 5, 2, 0))
	h.outcome(t)
	nativeRuntimeTestSend(h.client, `{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"second"}`)
	nativeRuntimeTestRead(t, h.upstream)
	h.upstreamEvent(t, nativeRuntimeTestCreated("main", "resp_a2", "gpt-6-astra"))
	nativeRuntimeTestSend(h.upstream, `{"type":"response.completed","stream_id":"main","response":{"model":"gpt-6-astra","usage":{"input_tokens":999,"output_tokens":999}}}`)
	second := h.outcome(t)
	require.ErrorContains(t, second.err, "terminal is missing its response ID")
	require.Nil(t, second.result, "an unattributable terminal cannot invent usage or success for the active response")
	select {
	case <-h.done:
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not reject a terminal without a response ID")
	}
	require.ErrorContains(t, h.runErr, "terminal is missing its response ID")
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, 2)
	require.NoError(t, outcomes[0].err)
	require.Equal(t, "resp_a1", outcomes[0].result.ResponseID)
	require.Error(t, outcomes[1].err)
	require.Equal(t, 3, h.client.writeCount(), "an unidentified terminal must not be forwarded as completion of the current turn")
}
