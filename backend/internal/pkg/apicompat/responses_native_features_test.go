package apicompat

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestResponsesNativeFeatures_PreserveTypedHistoryAndCallIDs(t *testing.T) {
	input := `[
		{"type":"function_call","call_id":"call_pending:original-42","name":"lookup","arguments":"{}","async":true},
		{"role":"user","content":"Continue independent work"},
		{"type":"configuration_update","reasoning":{"effort":"high"}},
		{"type":"function_call_output","call_id":"call_pending:original-42","output":[{"type":"input_text","text":"arrived later","prompt_cache_breakpoint":{"mode":"explicit"}}]},
		{"type":"custom_tool_call","call_id":"custom:original-99","name":"shell","input":"pwd","async":false},
		{"type":"custom_tool_call_output","call_id":"custom:original-99","output":"/workspace"},
		{"type":"function_call_output","call_id":"call_empty-original","output":""}
	]`
	var items []ResponsesInputItem
	require.NoError(t, json.Unmarshal([]byte(input), &items))
	encoded, err := json.Marshal(items)
	require.NoError(t, err)
	require.JSONEq(t, input, string(encoded))
}

func TestResponsesNativeFeatures_RejectLossyProtocolConversions(t *testing.T) {
	cases := []struct{ name, extra, feature string }{
		{"function", `"tools":[{"type":"function","name":"lookup","async":true}]`, "async tools"},
		{"custom", `"tools":[{"type":"custom","name":"shell","async":true}]`, "async tools"},
		{"namespace", `"tools":[{"type":"namespace","name":"jobs","tools":[{"type":"function","name":"lookup","async":true}]}]`, "async tools"},
		{"additional tools", `"input":[{"type":"additional_tools","tools":[{"type":"function","name":"lookup","async":true}]}]`, "async tools"},
		{"pro", `"reasoning":{"mode":"pro","effort":"medium"}`, "reasoning.mode=pro"},
		{"context", `"reasoning":{"context":"all_turns"}`, "reasoning.context=all_turns"},
		{"configuration", `"input":[{"type":"configuration_update","reasoning":{"effort":"high"}},{"role":"user","content":"continue"}]`, "configuration_update"},
		{"pending async history", `"input":[{"type":"function_call","call_id":"call_pending:original-42","name":"lookup","arguments":"{}","async":true},{"role":"user","content":"Continue independent work"}]`, "async tool call history"},
		{"late custom output", `"input":[{"type":"custom_tool_call","call_id":"custom:original-99","name":"shell","input":"pwd","async":true},{"role":"user","content":"Another turn"},{"type":"custom_tool_call_output","call_id":"custom:original-99","output":"/workspace"}]`, "async tool call history"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var req ResponsesRequest
			require.NoError(t, json.Unmarshal([]byte(`{"model":"gpt-6-astra",`+tc.extra+`}`), &req))
			before, err := json.Marshal(req)
			require.NoError(t, err)
			chat, err := ResponsesToChatCompletionsRequest(&req)
			require.Nil(t, chat)
			require.ErrorContains(t, err, tc.feature)
			anthropic, err := ResponsesToAnthropicRequest(&req)
			require.Nil(t, anthropic)
			require.ErrorContains(t, err, tc.feature)
			after, err := json.Marshal(req)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after), "rejection must not consume pending calls or rewrite call IDs")
		})
	}
}

func TestResponsesNativeFeatures_ClientToolAdaptationPreservesAsync(t *testing.T) {
	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(`{"model":"gpt-6-astra","tools":[{"type":"custom","name":"shell","async":true}],"input":[{"type":"custom_tool_call","call_id":"original:call-17","name":"shell","input":"pwd","async":true},{"role":"user","content":"Independent follow-up"},{"type":"custom_tool_call_output","call_id":"original:call-17","output":"/workspace"},{"type":"configuration_update","reasoning":{"effort":"high"}}]}`), &body))
	mapping, changed, err := AdaptResponsesClientTools(body)
	require.NoError(t, err)
	require.True(t, changed)
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	require.True(t, gjson.GetBytes(encoded, "tools.0.async").Bool())
	require.True(t, gjson.GetBytes(encoded, "input.0.async").Bool())
	require.Equal(t, "original:call-17", gjson.GetBytes(encoded, "input.0.call_id").String())
	require.Equal(t, "original:call-17", gjson.GetBytes(encoded, "input.2.call_id").String())
	require.Equal(t, "high", gjson.GetBytes(encoded, "input.3.reasoning.effort").String())

	var event ResponsesStreamEvent
	require.NoError(t, json.Unmarshal([]byte(`{"type":"response.output_item.added","item":{"type":"function_call","id":"fc_test","call_id":"original:call-17","name":"shell","arguments":"","async":true}}`), &event))
	restored := NewResponsesClientToolStreamRestorer(mapping).Restore(event)
	require.Len(t, restored, 1)
	encoded, err = json.Marshal(restored[0])
	require.NoError(t, err)
	require.Equal(t, "custom_tool_call", gjson.GetBytes(encoded, "item.type").String())
	require.True(t, gjson.GetBytes(encoded, "item.async").Bool())
	require.Equal(t, "original:call-17", gjson.GetBytes(encoded, "item.call_id").String())
}

func TestResponsesNativeFeatures_CacheOptionsAndBreakpointsSurviveConversion(t *testing.T) {
	var chat ChatCompletionsRequest
	require.NoError(t, json.Unmarshal([]byte(`{"model":"gpt-5.6-sol","prompt_cache_key":"shared-prefix","prompt_cache_retention":"24h","prompt_cache_options":{"mode":"explicit","ttl":"30m","comparison_response_id":"resp_prior"},"safety_identifier":"hashed-user","messages":[{"role":"developer","content":[{"type":"text","text":"Stable instructions","prompt_cache_breakpoint":{"mode":"explicit"}}]},{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/input.png","detail":"original"},"prompt_cache_breakpoint":{"mode":"explicit"}}]}]}`), &chat))
	resp, err := ChatCompletionsToResponses(&chat)
	require.NoError(t, err)
	encoded, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Equal(t, "developer", gjson.GetBytes(encoded, "input.0.role").String())
	require.Equal(t, "explicit", gjson.GetBytes(encoded, "input.0.content.0.prompt_cache_breakpoint.mode").String())
	require.Equal(t, "original", gjson.GetBytes(encoded, "input.1.content.0.detail").String())

	converted, err := ResponsesToChatCompletionsRequest(resp)
	require.NoError(t, err)
	require.Equal(t, chat.PromptCacheKey, converted.PromptCacheKey)
	require.Equal(t, chat.SafetyIdentifier, converted.SafetyIdentifier)
	require.Equal(t, chat.PromptCacheRetention, converted.PromptCacheRetention)
	require.JSONEq(t, string(chat.PromptCacheOptions), string(converted.PromptCacheOptions))
	encoded, err = json.Marshal(converted)
	require.NoError(t, err)
	require.Equal(t, "explicit", gjson.GetBytes(encoded, "messages.0.content.0.prompt_cache_breakpoint.mode").String())
	require.Equal(t, "original", gjson.GetBytes(encoded, "messages.1.content.0.image_url.detail").String())
}

func TestResponsesNativeFeatures_AssistantAndToolCacheBreakpoints(t *testing.T) {
	var chat ChatCompletionsRequest
	require.NoError(t, json.Unmarshal([]byte(`{"model":"gpt-5.6-sol","tools":[{"type":"function","function":{"name":"lookup"}}],"messages":[{"role":"assistant","content":[{"type":"text","text":"Stable completed work","prompt_cache_breakpoint":{"mode":"explicit"}}],"tool_calls":[{"id":"original_call_42","type":"function","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"original_call_42","content":[{"type":"text","text":"Stable tool result","prompt_cache_breakpoint":{"mode":"explicit"}}]}]}`), &chat))
	resp, err := ChatCompletionsToResponses(&chat)
	require.NoError(t, err)
	encoded, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Equal(t, "explicit", gjson.GetBytes(encoded, "input.0.content.0.prompt_cache_breakpoint.mode").String())
	require.Equal(t, "original_call_42", gjson.GetBytes(encoded, "input.2.call_id").String())
	require.Equal(t, "explicit", gjson.GetBytes(encoded, "input.2.output.0.prompt_cache_breakpoint.mode").String())
	converted, err := ResponsesToChatCompletionsRequest(resp)
	require.NoError(t, err)
	encoded, err = json.Marshal(converted)
	require.NoError(t, err)
	require.Equal(t, "explicit", gjson.GetBytes(encoded, "messages.0.content.0.prompt_cache_breakpoint.mode").String())
	require.Equal(t, "original_call_42", gjson.GetBytes(encoded, "messages.1.tool_call_id").String())
	require.Equal(t, "explicit", gjson.GetBytes(encoded, "messages.1.content.0.prompt_cache_breakpoint.mode").String())
}
