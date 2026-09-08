package handler

import (
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSHTTPBridgeInputSemanticsHistoryAndNotifications(t *testing.T) {
	for _, typ := range []string{"web_search_call", "file_search_call", "code_interpreter_call", "image_generation_call", "mcp_call", "future_hosted_call", "compaction"} {
		t.Run(typ, func(t *testing.T) {
			bodies := make(chan []byte, 3)
			calls := 0
			server, _, _ := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				bodies <- body
				calls++
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_semantic_%d\",\"status\":\"completed\",\"model\":\"gpt-5.1\",\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n", calls)
			}))
			conn := dialIsolationBridge(t, server)
			readBody := func() []byte {
				t.Helper()
				for {
					event := readIsolationEvent(t, conn)
					require.NotEqual(t, "error", gjson.GetBytes(event, "type").String(), string(event))
					if gjson.GetBytes(event, "type").String() == "response.completed" {
						return <-bodies
					}
				}
			}
			// Hosted history has an item ID, not a client call_id. Its payload
			// must be preserved independently of the following notification.
			history := fmt.Sprintf(`{"type":%q,"id":"ws_history","status":"completed","action":{"type":"search","query":"fixture"},"encrypted_content":"opaque-fixture"}`, typ)
			writeIsolationFrame(t, conn, `{"type":"response.create","model":"gpt-5.1","input":[`+history+`,{"type":"function_call","call_id":"fc_old","name":"fixture","arguments":"{}"},{"type":"function_call_output","call_id":"fc_old","output":"kept"},{"type":"function_call_output","namespace":"codex_app","name":"send_message_to_thread","output":"`+delegationEnvelope+`"}]}`)
			first := readBody()
			require.Equal(t, typ, gjson.GetBytes(first, "input.0.type").String())
			require.Equal(t, "opaque-fixture", gjson.GetBytes(first, "input.0.encrypted_content").String())
			require.Equal(t, delegationEnvelope, gjson.GetBytes(first, "input.3.content.0.text").String())
			writeIsolationFrame(t, conn, `{"type":"response.create","previous_response_id":"resp_semantic_1","input":[{"type":"function_call_output","namespace":"codex_app","name":"automation_update","output":"<heartbeat><automation_id>fixture</automation_id></heartbeat>"}]}`)
			second := readBody()
			require.Equal(t, delegationEnvelope, gjson.GetBytes(second, "input.3.content.0.text").String())
			require.Equal(t, "user", gjson.GetBytes(second, "input.4.role").String())
			writeIsolationFrame(t, conn, `{"type":"response.create","previous_response_id":"resp_semantic_2","input":[{"role":"user","content":"continue"}]}`)
			third := readBody()
			require.Equal(t, 6, len(gjson.GetBytes(third, "input").Array()))
			require.Equal(t, delegationEnvelope, gjson.GetBytes(third, "input.3.content.0.text").String())
		})
	}
}

func TestOpenAIWSHTTPBridgeInputSemanticsNativeStandalone(t *testing.T) {
	for _, item := range []string{
		`{"type":"function_call_output","id":"fco_client","name":"external_notification","namespace":"external_namespace","output":"native standalone input"}`,
		`{"type":"function_call_output","name":"send_message_to_thread","namespace":"codex_app","output":"literal <div>markup</div> without a delegation envelope"}`,
		`{"type":"function_call_output","name":"external_notification","namespace":"external_namespace","output":[{"type":"input_text","text":"native structured input"}]}`,
		`{"type":"tool_search_output","execution":"server","tools":[]}`,
	} {
		t.Run(gjson.Get(item, "type").String(), func(t *testing.T) {
			bodies := make(chan []byte, 1)
			server, _, _ := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				bodies <- body
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_native\",\"status\":\"completed\",\"model\":\"gpt-5.1\",\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n")
			}))
			conn := dialIsolationBridge(t, server)
			writeIsolationFrame(t, conn, `{"type":"response.create","model":"gpt-5.1","input":[`+item+`]}`)
			for {
				event := readIsolationEvent(t, conn)
				require.NotEqual(t, "error", gjson.GetBytes(event, "type").String(), string(event))
				if gjson.GetBytes(event, "type").String() == "response.completed" {
					break
				}
			}
			body := <-bodies
			require.Equal(t, gjson.Get(item, "type").String(), gjson.GetBytes(body, "input.0.type").String())
			require.False(t, gjson.GetBytes(body, "input.0.call_id").Exists(), "never invent pairing from item ID")
			if gjson.Get(item, "namespace").Exists() {
				require.Equal(t, gjson.Get(item, "namespace").String(), gjson.GetBytes(body, "input.0.namespace").String())
			}
			if original := gjson.Get(item, "output"); original.Exists() {
				require.JSONEq(t, original.Raw, gjson.GetBytes(body, "input.0.output").Raw)
			}
		})
	}
}

func TestOpenAIWSHTTPBridgeInputSemanticsOpaqueValidationRemainsUpstream(t *testing.T) {
	bodies := make(chan []byte, 1)
	server, _, _ := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		bodies <- body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"type":"invalid_request_error","code":"invalid_compaction","message":"upstream rejected fixture ciphertext"}}`)
	}))
	conn := dialIsolationBridge(t, server)
	writeIsolationFrame(t, conn, `{"type":"response.create","model":"gpt-5.1","store":false,"input":[{"type":"compaction","encrypted_content":"opaque-fixture"},{"type":"function_call_output","call_id":"call_opaque","output":"do not drop"}]}`)
	event := readIsolationEvent(t, conn)
	require.Equal(t, "error", gjson.GetBytes(event, "type").String())
	require.EqualValues(t, 400, gjson.GetBytes(event, "status").Int())
	require.Len(t, bodies, 1, "the upstream must validate opaque state; local absence isn't proof of a missing call")
	body := <-bodies
	require.Equal(t, "opaque-fixture", gjson.GetBytes(body, "input.0.encrypted_content").String())
	require.Equal(t, "call_opaque", gjson.GetBytes(body, "input.1.call_id").String())
	require.Equal(t, "do not drop", gjson.GetBytes(body, "input.1.output").String())
}
