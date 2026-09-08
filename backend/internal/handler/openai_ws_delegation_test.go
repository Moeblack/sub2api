package handler

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSHTTPBridgeDelegationSurvivesReplay(t *testing.T) {
	const secondEnvelope = `<codex_delegation><source_thread_id>thread-2</source_thread_id><input>second report</input></codex_delegation>`
	bodies := make(chan []byte, 3)
	var calls atomic.Int32
	server, _, _ := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		bodies <- body
		n := calls.Add(1)
		output := `[]`
		if n == 1 {
			output = `[{"type":"function_call","id":"fc_next","call_id":"call_next","name":"lookup","arguments":"{}"}]`
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_delegation_%d\",\"status\":\"in_progress\",\"model\":\"gpt-5.1\"}}\n\n", n)
		fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_delegation_%d\",\"status\":\"completed\",\"model\":\"gpt-5.1\",\"output\":%s,\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n", n, output)
	}))
	conn := dialIsolationBridge(t, server)
	readCompleted := func() []byte {
		t.Helper()
		for {
			event := readIsolationEvent(t, conn)
			typ := gjson.GetBytes(event, "type").String()
			require.NotEqual(t, "error", typ, string(event))
			if typ == "response.completed" {
				return <-bodies
			}
		}
	}
	assertDelegations := func(body []byte, expected ...string) {
		t.Helper()
		var actual []string
		for _, item := range gjson.GetBytes(body, "input").Array() {
			if item.Get("type").String() == "function_call_output" {
				require.NotEmpty(t, item.Get("call_id").String(), "standalone input must not reach HTTP as an orphan result")
			}
			text := item.Get("content.0.text").String()
			if text == delegationEnvelope || text == secondEnvelope {
				require.Equal(t, "user", item.Get("role").String(), "delegation must not acquire system/developer authority")
				actual = append(actual, text)
			}
		}
		require.Equal(t, expected, actual, "each complete attributed report must survive replay exactly once and in order")
	}

	// Resume a full history containing normal tool traffic and a call-less report.
	writeIsolationFrame(t, conn, `{"type":"response.create","model":"gpt-5.1","store":false,"input":[{"type":"function_call","call_id":"call_old","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_old","output":"old result"},{"type":"function_call_output","id":"fco_report","namespace":"codex_app","name":"send_message_to_thread","output":"`+delegationEnvelope+`"}]}`)
	first := readCompleted()
	assertDelegations(first, delegationEnvelope)
	// The existing OAuth adapter canonicalizes both sides of a tool pair.
	require.Equal(t, "fc_old", gjson.GetBytes(first, "input.0.call_id").String())
	require.Equal(t, "fc_old", gjson.GetBytes(first, "input.1.call_id").String())
	require.Equal(t, "old result", gjson.GetBytes(first, "input.1.output").String())

	// Merge a genuine tool result and a second report with the response snapshot.
	writeIsolationFrame(t, conn, `{"type":"response.create","model":"gpt-5.1","store":false,"previous_response_id":"resp_delegation_1","input":[{"type":"function_call_output","call_id":"call_next","output":"new result"},{"type":"function_call_output","namespace":"codex_app","name":"send_message_to_thread","output":"`+secondEnvelope+`"}]}`)
	second := readCompleted()
	assertDelegations(second, delegationEnvelope, secondEnvelope)
	require.Equal(t, "function_call", gjson.GetBytes(second, "input.3.type").String())
	require.Equal(t, "fc_next", gjson.GetBytes(second, "input.3.call_id").String())
	require.Equal(t, "fc_next", gjson.GetBytes(second, "input.4.call_id").String())
	require.Equal(t, "new result", gjson.GetBytes(second, "input.4.output").String())

	// A plain continuation must replay normalized snapshots, not lose the reports.
	writeIsolationFrame(t, conn, `{"type":"response.create","model":"gpt-5.1","store":false,"previous_response_id":"resp_delegation_2","input":[{"type":"message","role":"user","content":"continue"}]}`)
	third := readCompleted()
	assertDelegations(third, delegationEnvelope, secondEnvelope)
	require.Equal(t, "continue", gjson.GetBytes(third, "input.6.content").String())
	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "complete"))
}

func TestOpenAIWSHTTPBridgeDelegationDoesNotAuthorizeOrphans(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
	}{
		{"anonymous missing call id", `[{"type":"function_call_output","output":"missing call id and name"}]`},
		{"numeric call id", `[{"type":"function_call_output","name":"send_message_to_thread","call_id":42,"output":"invalid id type"}]`},
		{"valid report with orphan result", `[{"type":"function_call_output","namespace":"codex_app","name":"send_message_to_thread","output":"` + delegationEnvelope + `"},{"type":"function_call_output","call_id":"call_missing","output":"orphan"}]`},
		{"report with explicit call id", `[{"type":"function_call_output","namespace":"codex_app","name":"send_message_to_thread","call_id":"call_missing","output":"` + delegationEnvelope + `"}]`},
		{"duplicate member", `[{"type":"function_call_output","namespace":"other","namespace":"codex_app","name":"send_message_to_thread","output":"` + delegationEnvelope + `"}]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server, _, _ := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			conn := dialIsolationBridge(t, server)
			writeIsolationFrame(t, conn, `{"type":"response.create","model":"gpt-5.1","store":false,"input":`+tt.input+`}`)
			event := readIsolationEvent(t, conn)
			require.Equal(t, "error", gjson.GetBytes(event, "type").String())
			require.EqualValues(t, 400, gjson.GetBytes(event, "status").Int())
			require.Equal(t, "invalid_input", gjson.GetBytes(event, "error.code").String())
			require.Zero(t, calls.Load(), "invalid context must not execute an upstream request")
			require.NoError(t, conn.Close(coderws.StatusNormalClosure, "rejected"))
		})
	}
}

func TestOpenAIWSHTTPBridgeDelegationBootstrapAndWarmup(t *testing.T) {
	for _, tt := range []struct {
		name     string
		previous string
		warmup   bool
	}{
		{"new connection", "", false},
		{"explicit null checkpoint", `,"previous_response_id":null`, false},
		{"warmup then cross lane fork", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bodies := make(chan []byte, 1)
			var calls atomic.Int32
			server, _, _ := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return
				}
				bodies <- body
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_bootstrap\",\"status\":\"completed\",\"model\":\"gpt-5.1\",\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n")
			}))
			conn := dialIsolationBridge(t, server)
			readCompleted := func() []byte {
				t.Helper()
				for {
					event := readIsolationEvent(t, conn)
					require.NotEqual(t, "error", gjson.GetBytes(event, "type").String(), string(event))
					if gjson.GetBytes(event, "type").String() == "response.completed" {
						return event
					}
				}
			}
			generate := ""
			if tt.warmup {
				generate = `,"generate":false`
			}
			writeIsolationFrame(t, conn, `{"type":"response.create","stream_id":"source","model":"gpt-5.1","store":false`+tt.previous+generate+`,"input":[{"type":"function_call_output","namespace":"codex_tui","name":"create_thread","call_id":"","output":"`+delegationEnvelope+`"}]}`)
			completed := readCompleted()
			if tt.warmup {
				require.Zero(t, calls.Load(), "warmup must not perform model inference")
				parentID := gjson.GetBytes(completed, "response.id").String()
				writeIsolationFrame(t, conn, fmt.Sprintf(`{"type":"response.create","stream_id":"fork","model":"gpt-5.1","store":false,"previous_response_id":%q,"input":[{"role":"user","content":"continue"}]}`, parentID))
				completed = readCompleted()
				require.Equal(t, "fork", gjson.GetBytes(completed, "stream_id").String())
			}
			body := <-bodies
			require.Equal(t, "message", gjson.GetBytes(body, "input.0.type").String())
			require.Equal(t, "user", gjson.GetBytes(body, "input.0.role").String())
			require.Equal(t, delegationEnvelope, gjson.GetBytes(body, "input.0.content.0.text").String())
			require.EqualValues(t, 1, calls.Load())
			require.NoError(t, conn.Close(coderws.StatusNormalClosure, "complete"))
		})
	}
}
