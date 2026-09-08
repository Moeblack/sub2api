package service

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestPrepareOpenAIRemoteCompactV2BodyBuildsCurrentCodexWire(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.6-sol",
		"instructions":"compact-test",
		"input":[
			{"type":"message","role":"user","content":"hello"},
			{"type":"compaction_trigger"},
			{"type":"message","role":"user","content":"latest"},
			{"type":"compaction_trigger"}
		],
		"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}],
		"parallel_tool_calls":true,
		"reasoning":{"effort":"max","context":"all_turns"},
		"service_tier":"priority",
		"text":{"verbosity":"low"}
	}`)

	prepared, changed, err := prepareOpenAIRemoteCompactV2Body(body)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, gjson.ValidBytes(prepared))
	require.Equal(t, "gpt-5.6-sol", gjson.GetBytes(prepared, "model").String())
	require.Equal(t, "compact-test", gjson.GetBytes(prepared, "instructions").String())
	require.Equal(t, 3, len(gjson.GetBytes(prepared, "input").Array()))
	input := gjson.GetBytes(prepared, "input").Array()
	require.Equal(t, "compaction_trigger", input[len(input)-1].Get("type").String())
	require.Len(t, gjson.GetBytes(prepared, `input.#(type=="compaction_trigger")#`).Array(), 1)
	require.True(t, gjson.GetBytes(prepared, "stream").Bool())
	require.False(t, gjson.GetBytes(prepared, "store").Bool())
	require.Equal(t, "auto", gjson.GetBytes(prepared, "tool_choice").String())
	require.True(t, gjson.GetBytes(prepared, "parallel_tool_calls").Bool())
	require.Equal(t, "max", gjson.GetBytes(prepared, "reasoning.effort").String())
	require.Equal(t, "all_turns", gjson.GetBytes(prepared, "reasoning.context").String())
	require.Equal(t, "reasoning.encrypted_content", gjson.GetBytes(prepared, "include.0").String())
	require.Equal(t, "priority", gjson.GetBytes(prepared, "service_tier").String())
	require.Equal(t, "low", gjson.GetBytes(prepared, "text.verbosity").String())
}

func TestPrepareOpenAIRemoteCompactV2BodyIsIdempotent(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.6-sol",
		"instructions":"compact-test",
		"input":[{"type":"message","role":"user","content":"hello"},{"type":"compaction_trigger"}],
		"tools":[],
		"tool_choice":"auto",
		"parallel_tool_calls":false,
		"reasoning":{"effort":"medium"},
		"include":["reasoning.encrypted_content"],
		"store":false,
		"stream":true
	}`)

	prepared, changed, err := prepareOpenAIRemoteCompactV2Body(body)
	require.NoError(t, err)
	require.False(t, changed)
	require.JSONEq(t, string(body), string(prepared))
}

func TestApplyOpenAIRemoteCompactV2HeadersMergesFeatureAndDropsLegacyBeta(t *testing.T) {
	headers := http.Header{
		"OpenAI-Beta":           []string{"responses=experimental, future=v1"},
		"x-codex-beta-features": []string{"responses_websockets_v2", "REMOTE_COMPACTION_V2"},
	}

	applyOpenAIRemoteCompactV2Headers(headers)

	require.Equal(t, "text/event-stream", headers.Get("Accept"))
	require.Equal(t, "future=v1", headers.Get("OpenAI-Beta"))
	require.Equal(t, "responses_websockets_v2, REMOTE_COMPACTION_V2", headers.Get("x-codex-beta-features"))
}

func TestValidateOpenAIRemoteCompactV2Response(t *testing.T) {
	valid := []byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"x\"}}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n")
	require.NoError(t, validateOpenAIRemoteCompactV2Response(valid))

	require.ErrorContains(t,
		validateOpenAIRemoteCompactV2Response([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n")),
		"exactly one compaction",
	)
	require.ErrorContains(t,
		validateOpenAIRemoteCompactV2Response([]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\"}}\n\n")),
		"before response.completed",
	)
	require.ErrorContains(t,
		validateOpenAIRemoteCompactV2Response([]byte("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"overloaded\"}}}\n\n")),
		"overloaded",
	)
}
