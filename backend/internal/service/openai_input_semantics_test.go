package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const semanticDelegation = `<codex_delegation><source_thread_id>source</source_thread_id><input>preserve full report</input></codex_delegation>`

func TestOpenAIInputSemanticsAuthorizedPersistedHistory(t *testing.T) {
	for _, tt := range []struct {
		name, store, output  string
		authorized, accepted bool
	}{
		{"owned upstream parent", "true", `{"type":"function_call_output","call_id":"call_upstream","output":"result"}`, true, true},
		{"unknown owner", "true", `{"type":"function_call_output","call_id":"call_upstream","output":"result"}`, false, false},
		{"store false cannot hydrate", "false", `{"type":"function_call_output","call_id":"call_upstream","output":"result"}`, true, false},
		{"missing id stays invalid", "true", `{"type":"function_call_output","output":"result"}`, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(`{"type":"response.create","model":"gpt-5.1","store":` + tt.store + `,"previous_response_id":"resp_owned","input":[` + tt.output + `]}`)
			prepared, _, known, _, _, requestErr := prepareOpenAIWSConnectionRequest(context.Background(), body,
				&openAIWSConnectionLane{}, &openAIWSConnectionCache{},
				func(context.Context, string) bool { return tt.authorized }, NormalizeCodexStandaloneInputs)
			if !tt.accepted {
				require.NotNil(t, requestErr)
				return
			}
			require.Nil(t, requestErr)
			require.False(t, known, "partial input must never be published as a full local checkpoint")
			require.Equal(t, "resp_owned", gjson.GetBytes(prepared, "previous_response_id").String())
			require.Equal(t, "call_upstream", gjson.GetBytes(prepared, "input.0.call_id").String())
		})
	}
}

func TestOpenAIInputSemanticsOpaqueCompactionDefersMatchingUpstream(t *testing.T) {
	for _, tt := range []struct {
		name, contextItem, output string
		accepted                  bool
	}{
		{"opaque window", `{"type":"compaction","encrypted_content":"opaque-upstream-state"}`, `{"type":"function_call_output","call_id":"call_in_compaction","output":"result"}`, true},
		{"legacy opaque window", `{"type":"compaction_summary","encrypted_content":"opaque-upstream-state"}`, `{"type":"function_call_output","call_id":"call_in_compaction","output":"result"}`, true},
		{"empty ciphertext", `{"type":"compaction","encrypted_content":""}`, `{"type":"function_call_output","call_id":"orphan","output":"result"}`, false},
		{"reasoning is not conversation history", `{"type":"reasoning","encrypted_content":"reasoning-only"}`, `{"type":"function_call_output","call_id":"orphan","output":"result"}`, false},
		{"trigger is not history", `{"type":"compaction_trigger"}`, `{"type":"function_call_output","call_id":"orphan","output":"result"}`, false},
		{"missing call id", `{"type":"compaction","encrypted_content":"opaque-upstream-state"}`, `{"type":"function_call_output","output":"result"}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(`{"type":"response.create","model":"gpt-5.1","store":false,"input":[` + tt.contextItem + `,` + tt.output + `]}`)
			prepared, _, known, _, _, requestErr := prepareOpenAIWSConnectionRequest(context.Background(), body,
				&openAIWSConnectionLane{}, &openAIWSConnectionCache{}, nil, NormalizeCodexStandaloneInputs)
			if !tt.accepted {
				require.NotNil(t, requestErr)
				return
			}
			require.Nil(t, requestErr)
			require.True(t, known, "the opaque window is retained for full replay, not locally decrypted")
			require.JSONEq(t, tt.contextItem, gjson.GetBytes(prepared, "input.0").Raw)
			require.JSONEq(t, tt.output, gjson.GetBytes(prepared, "input.1").Raw)
		})
	}
}

func TestOpenAIInputSemanticsNormalizationPreservesOtherItems(t *testing.T) {
	for _, historical := range []string{
		`{"type":"web_search_call","id":"ws_1","action":{"type":"search","query":"fixture"}}`,
		`{"type":"future_hosted_call","id":"hosted_1","payload":{"integer":9007199254740993}}`,
		`{"type":"function_call_output","call_id":"real-orphan","output":"must not disappear"}`,
		`{"type":"function_call"}`,
		`{"type":"computer_call"}`,
		`{"type":"computer_call_output","output":"must remain for upstream validation"}`,
		`{"type":"tool_search_output","output":"must remain for pairing validation"}`,
		`{"type":"function_call_output","namespace":"codex_app","name":"send_message_to_thread","output":"not a delegation envelope"}`,
		`{"type":"function_call_output","name":"send_message_to_thread","namespace":"codex_app","call_id":"paired-call","output":"` + semanticDelegation + `"}`,
	} {
		body := []byte(`{"previous_response_id":null,"input":[` + historical + `,{"type":"function_call_output","name":"send_message_to_thread","namespace":"codex_app","output":"` + semanticDelegation + `"},{"type":"function_call_output","name":"automation_update","namespace":"codex_app","output":"<heartbeat><automation_id>example</automation_id></heartbeat>"}]}`)
		got, changed := NormalizeCodexStandaloneInputs(body)
		require.True(t, changed)
		require.JSONEq(t, historical, gjson.GetBytes(got, "input.0").Raw)
		require.Equal(t, semanticDelegation, gjson.GetBytes(got, "input.1.content.0.text").String())
		require.Equal(t, "user", gjson.GetBytes(got, "input.2.role").String())
		again, changed := NormalizeCodexStandaloneInputs(got)
		require.False(t, changed)
		require.Equal(t, got, again)
	}
}

func TestOpenAIInputSemanticsCoverageMatrix(t *testing.T) {
	for _, tt := range []struct {
		name, input     string
		paired, covered bool
	}{
		{"ordinary paired", `{"type":"function_call","call_id":"call_1"},{"type":"function_call_output","call_id":"call_1","output":"ok"}`, true, true},
		{"ordinary orphan", `{"type":"function_call_output","call_id":"call_missing","output":"bad"}`, true, false},
		{"anonymous missing id", `{"type":"function_call_output","output":"bad"}`, true, false},
		{"named standalone", `{"type":"function_call_output","name":"notification","output":"ok"}`, false, false},
		{"named null id", `{"type":"function_call_output","name":"notification","call_id":null,"output":"ok"}`, false, false},
		{"named ordinary orphan", `{"type":"function_call_output","name":"notification","call_id":"missing","output":"bad"}`, true, false},
		{"numeric ids never pair", `{"type":"function_call","call_id":42},{"type":"function_call_output","name":"notification","call_id":42,"output":"bad"}`, true, false},
		{"server search", `{"type":"tool_search_call","execution":"server"},{"type":"tool_search_output","execution":"server","tools":[]}`, false, false},
		{"client search paired", `{"type":"tool_search_call","execution":"client","call_id":"search"},{"type":"tool_search_output","execution":"client","call_id":"search","tools":[]}`, true, true},
		{"client search missing", `{"type":"tool_search_output","execution":"client","tools":[]}`, true, false},
		{"server cannot satisfy client", `{"type":"tool_search_call","execution":"server","call_id":"search"},{"type":"tool_search_output","execution":"client","call_id":"search","tools":[]}`, true, false},
		{"custom tool paired", `{"type":"custom_tool_call","call_id":"custom"},{"type":"custom_tool_call_output","call_id":"custom","output":"ok"}`, true, true},
		{"item reference", `{"type":"item_reference","id":"call_1"},{"type":"function_call_output","call_id":"call_1","output":"ok"}`, true, true},
		{"duplicate namespace", `{"type":"function_call_output","name":"notification","namespace":"one","namespace":"two","output":"bad"}`, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(`{"input":[` + tt.input + `]}`)
			coverage := AnalyzeToolCallOutputContextCoverageBytes(body)
			require.Equal(t, tt.paired, coverage.HasFunctionCallOutput)
			require.Equal(t, tt.covered, coverage.ContextCoversAllCallIDs)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(body, &decoded))
			signals := AnalyzeToolContinuationSignals(decoded)
			if tt.name != "duplicate namespace" {
				require.Equal(t, tt.paired, signals.HasFunctionCallOutput)
			}
		})
	}
}

func TestOpenAIInputSemanticsStandaloneMetadataPreserved(t *testing.T) {
	item := map[string]any{"type": "function_call_output", "id": "fco_client",
		"name": "external_notification", "namespace": "external", "output": "unmodified text"}
	input := []any{item}
	request := map[string]any{"input": input}
	require.False(t, sanitizeOpenAIResponsesOrphanToolOutputs(request, input, false))
	filtered := filterCodexInput(input, true)
	require.Len(t, filtered, 1)
	require.NotContains(t, filtered[0], "call_id")
	filteredItem, ok := filtered[0].(map[string]any)
	require.True(t, ok, "standalone input must remain an object")
	require.Equal(t, "external_notification", filteredItem["name"])
	body, err := json.Marshal(request)
	require.NoError(t, err)
	for _, keepNamespaces := range []bool{false, true} {
		stripped, err := stripOpenAIResponsesInputNamespaces(body, keepNamespaces)
		require.NoError(t, err)
		require.Equal(t, "external", gjson.GetBytes(stripped, "input.0.namespace").String())
	}
	require.Equal(t, "fco_client", item["id"], "normalization must not mutate the original map")
}

func TestOpenAIInputSemanticsValidationRepresentationsAgree(t *testing.T) {
	for _, standalone := range []string{
		`{"type":"function_call_output","name":"notification","output":"independent"}`,
		`{"type":"function_call_output","name":"notification","call_id":null,"output":"independent"}`,
		`{"type":"tool_search_output","execution":"server","tools":[]}`,
	} {
		for _, paired := range []string{
			`{"type":"function_call_output","call_id":"call_1","output":"result"},{"type":"item_reference","id":"call_1"}`,
			`{"type":"function_call_output","call_id":"call_missing","output":"result"}`,
			`{"type":"function_call_output","output":"invalid"}`,
		} {
			body := []byte(`{"input":[` + standalone + `,` + paired + `]}`)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(body, &decoded))
			raw := ValidateFunctionCallOutputContextBytes(body)
			require.Equal(t, raw, ValidateFunctionCallOutputContext(decoded), string(body))
		}
	}
}

func TestOpenAIInputSemanticsOpaqueToolHistoryIsNotRewritten(t *testing.T) {
	for _, typ := range []string{"web_search_call", "file_search_call", "image_generation_call", "computer_call", "computer_call_output", "shell_call", "shell_call_output", "apply_patch_call", "apply_patch_call_output", "local_shell_call_output", "future_tool_call", "compaction"} {
		item := map[string]any{"type": typ, "id": "opaque_id", "payload": "unchanged"}
		if typ == "web_search_call" {
			item["id"] = "ws_opaque"
		}
		if typ == "computer_call" || typ == "computer_call_output" || typ == "shell_call" || typ == "shell_call_output" || typ == "apply_patch_call" || typ == "apply_patch_call_output" || typ == "future_tool_call" {
			item["call_id"] = "opaque_call_id"
		}
		for _, preserveReferences := range []bool{false, true} {
			filtered := filterCodexInput([]any{item}, preserveReferences)
			require.Equal(t, []any{item}, filtered, "%s preserve=%v", typ, preserveReferences)
		}
	}
}

func TestOpenAIInputSemanticsHostedToolsDoNotUseClientCallIDs(t *testing.T) {
	for _, typ := range []string{"web_search_call", "file_search_call", "code_interpreter_call", "image_generation_call", "mcp_call"} {
		item := map[string]any{"type": typ, "id": "ws_opaque", "call_id": "stray_client_id"}
		filtered := filterCodexInput([]any{item}, true)
		require.NotContains(t, filtered[0], "call_id", typ)
		filteredItem, ok := filtered[0].(map[string]any)
		require.True(t, ok, "%s input must remain an object", typ)
		require.Equal(t, "ws_opaque", filteredItem["id"], typ)
		require.Equal(t, "stray_client_id", item["call_id"], "input map was mutated")
	}
}

func TestOpenAIInputSemanticsCompactionPreservesIdentifiers(t *testing.T) {
	for _, contextItem := range []map[string]any{
		{"type": "compaction", "encrypted_content": "opaque-state"},
		{"type": "compaction_trigger"},
	} {
		input := []any{contextItem, map[string]any{"type": "function_call_output", "call_id": "call_opaque", "output": "preserved"}}
		request := map[string]any{"model": "gpt-5.1", "input": input}
		if contextItem["type"] == "compaction" {
			require.False(t, sanitizeOpenAIResponsesOrphanToolOutputs(request, input, false))
		}
		applyCodexOAuthTransform(request, false, false)
		items, ok := request["input"].([]any)
		require.True(t, ok, "compaction history must remain an array")
		require.Len(t, items, 2)
		output, ok := items[1].(map[string]any)
		require.True(t, ok, "tool output must remain an object")
		require.Equal(t, "call_opaque", output["call_id"])
	}
	request := map[string]any{"model": "gpt-5.1", "input": []any{
		map[string]any{"type": "function_call", "call_id": "call_before_compaction", "name": "tool", "arguments": "{}"},
	}}
	applyCodexOAuthTransform(request, false, true)
	items, ok := request["input"].([]any)
	require.True(t, ok, "tool history must remain an array")
	call, ok := items[0].(map[string]any)
	require.True(t, ok, "tool call must remain an object")
	require.Equal(t, "call_before_compaction", call["call_id"])
}

func TestOpenAIInputSemanticsNativeAndLegacyPaths(t *testing.T) {
	for _, accountType := range []string{AccountTypeOAuth, AccountTypeAPIKey} {
		account := &Account{Platform: PlatformOpenAI, Type: accountType}
		body := []byte(`{"type":"response.create","input":[{"type":"web_search_call","id":"ws_1"},{"type":"function_call_output","namespace":"codex_app","name":"send_message_to_thread","output":"` + semanticDelegation + `"}]}`)
		native, _, err := normalizeOpenAIResponsesWebSocketCompatibilityBody(body, account, false)
		require.NoError(t, err)
		require.Equal(t, semanticDelegation, gjson.GetBytes(native, "input.1.content.0.text").String())
		legacy, err := prepareOpenAIWSHTTPBridgeBody(account, body)
		require.NoError(t, err)
		require.Equal(t, semanticDelegation, gjson.GetBytes(legacy, "input.1.content.0.text").String())
	}
}
