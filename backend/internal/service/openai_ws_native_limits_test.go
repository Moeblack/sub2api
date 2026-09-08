package service

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIWSNativeLimits_SeventeenthLaneWaitsForActiveCapacity(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"lane_0","model":"gpt-6-astra","input":"first"}`, 0)
	for i := 1; i <= 16; i++ {
		nativeRuntimeTestSend(h.client, fmt.Sprintf(`{"type":"response.create","stream_id":"lane_%d","model":"gpt-6-astra","input":"independent"}`, i))
	}
	for i := 0; i < 16; i++ {
		request := nativeRuntimeTestRead(t, h.upstream)
		require.Equal(t, fmt.Sprintf("lane_%d", i), request.Get("stream_id").String())
	}
	// The local rejection is an acknowledgement that the preceding 17th
	// create has already reached the connection loop and entered its queue.
	nativeRuntimeTestSend(h.client, `{"type":"response.inject","response_id":"resp_unknown","input":[]}`)
	barrier := nativeRuntimeTestRead(t, h.client)
	require.Equal(t, "response.inject.failed", barrier.Get("type").String())
	require.Equal(t, 16, h.upstream.writeCount(), "only 16 independent responses may be active on one socket")

	h.upstreamEvent(t, nativeRuntimeTestCreated("lane_0", "resp_first", "gpt-6-astra"))
	h.upstreamEvent(t, nativeRuntimeTestCompleted("lane_0", "resp_first", "gpt-6-astra", 5, 2, 0))
	finished := h.outcome(t)
	require.NoError(t, finished.err)
	require.Equal(t, "resp_first", finished.result.ResponseID)
	queued := nativeRuntimeTestRead(t, h.upstream)
	require.Equal(t, "lane_16", queued.Get("stream_id").String())
	require.Equal(t, 17, h.upstream.writeCount(), "finishing one response admits exactly the waiting 17th lane")
	h.stop(t)
}

func TestOpenAIWSNativeLimits_DefaultLaneDoesNotConsumeNamedStreamCapacity(t *testing.T) {
	h := newNativeRuntimeTestHarness(t, `{"type":"response.create","model":"gpt-6-astra","input":"default lane"}`, 0)
	for i := 0; i < 33; i++ {
		nativeRuntimeTestSend(h.client, fmt.Sprintf(`{"type":"response.create","stream_id":"named_%d","model":"gpt-6-astra","input":"named lane"}`, i))
	}
	rejected := nativeRuntimeTestRead(t, h.client)
	require.Equal(t, "websocket_stream_limit_reached", rejected.Get("error.code").String())
	require.Equal(t, "named_32", rejected.Get("stream_id").String(), "32 named streams must be admitted in addition to the implicit default lane")
	require.Equal(t, "stream_id", rejected.Get("error.param").String())

	seen := make(map[string]bool)
	for i := 0; i < 33; i++ {
		request := nativeRuntimeTestRead(t, h.upstream)
		lane := request.Get("stream_id").String()
		require.False(t, seen[lane], "each admitted request is forwarded once")
		seen[lane] = true
		responseID := fmt.Sprintf("resp_admitted_%d", i)
		var created, completed string
		if lane == "" {
			require.False(t, request.Get("stream_id").Exists(), "default-lane requests omit stream_id entirely")
			created = fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"model":"gpt-6-astra"}}`, responseID)
			completed = fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"model":"gpt-6-astra","usage":{"input_tokens":3,"output_tokens":1}}}`, responseID)
		} else {
			created = nativeRuntimeTestCreated(lane, responseID, "gpt-6-astra")
			completed = nativeRuntimeTestCompleted(lane, responseID, "gpt-6-astra", 3, 1, 0)
		}
		h.upstreamEvent(t, created)
		terminal := h.upstreamEvent(t, completed)
		if lane == "" {
			require.False(t, terminal.Get("stream_id").Exists(), "default-lane terminal metadata also omits stream_id")
		}
		outcome := h.outcome(t)
		require.NoError(t, outcome.err)
		require.Equal(t, responseID, outcome.result.ResponseID)
	}
	require.True(t, seen[""])
	for i := 0; i < 32; i++ {
		require.True(t, seen[fmt.Sprintf("named_%d", i)])
	}
	require.False(t, seen["named_32"])
	h.stop(t)
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, 33, "the rejected 33rd named stream must never be executed or billed")
	require.Equal(t, 33, h.upstream.writeCount())
}

func TestOpenAIWSNativeLimits_WarmupForwardsExactlyAndKeepsContinuationID(t *testing.T) {
	first := `{"type":"response.create","stream_id":"warm","model":"gpt-6-astra","generate":false,"store":false,"input":[{"role":"user","content":"draft a plan"}]}`
	h := newNativeRuntimeTestHarness(t, first, 0)
	require.Equal(t, first, nativeRuntimeTestRead(t, h.upstream).Raw)
	h.upstreamEvent(t, nativeRuntimeTestCreated("warm", "resp_warmup", "gpt-6-astra"))
	terminal := h.upstreamEvent(t, `{"type":"response.completed","stream_id":"warm","response":{"id":"resp_warmup","model":"gpt-6-astra","status":"completed","output":[],"usage":{"input_tokens":0,"output_tokens":0,"input_tokens_details":{"cached_tokens":0}}}}`)
	require.Empty(t, terminal.Get("response.output").Array())
	require.Equal(t, "resp_warmup", terminal.Get("response.id").String())
	warmup := h.outcome(t)
	require.NoError(t, warmup.err)
	require.Nil(t, warmup.result, "a no-output warmup must not create a billable result, including under per-request pricing")

	followup := `{"type":"response.create","stream_id":"warm","model":"gpt-6-astra","store":false,"previous_response_id":"resp_warmup","input":[{"role":"user","content":"now write the plan"}]}`
	nativeRuntimeTestSend(h.client, followup)
	forwarded := nativeRuntimeTestRead(t, h.upstream)
	require.Equal(t, followup, forwarded.Raw)
	require.Equal(t, "resp_warmup", forwarded.Get("previous_response_id").String())
	require.False(t, forwarded.Get("generate").Exists(), "the warmup flag must not leak into its followup")
	h.upstreamEvent(t, nativeRuntimeTestCreated("warm", "resp_generated", "gpt-6-astra"))
	h.upstreamEvent(t, nativeRuntimeTestCompleted("warm", "resp_generated", "gpt-6-astra", 11, 5, 3))
	generated := h.outcome(t)
	require.NoError(t, generated.err)
	require.Equal(t, "resp_generated", generated.result.ResponseID)
	require.Equal(t, OpenAIUsage{InputTokens: 11, OutputTokens: 5, CacheReadInputTokens: 3}, generated.result.Usage)
	h.stop(t)
	outcomes, _ := h.snapshot()
	require.Len(t, outcomes, 2)
	require.Equal(t, 2, h.upstream.writeCount())
}

func TestOpenAIWSNativeLimits_WarmupFlagCannotHideActualGeneration(t *testing.T) {
	for _, tc := range []struct {
		name         string
		output       string
		inputTokens  int
		outputTokens int
		images       int
	}{
		{name: "reported_tokens", output: `[]`, inputTokens: 11, outputTokens: 5},
		{name: "text_without_usage", output: `[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"generated despite the warmup flag"}]}]`},
		{name: "generated_image", output: `[{"type":"image_generation_call","id":"ig_unexpected","status":"completed","result":"generated-image","size":"1024x1024"}]`, images: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newNativeRuntimeTestHarness(t, `{"type":"response.create","stream_id":"warm","model":"gpt-6-astra","generate":false,"input":"warmup"}`, 0)
			nativeRuntimeTestRead(t, h.upstream)
			h.upstreamEvent(t, nativeRuntimeTestCreated("warm", "resp_generated", "gpt-6-astra"))
			terminal := fmt.Sprintf(`{"type":"response.completed","stream_id":"warm","response":{"id":"resp_generated","model":"gpt-6-astra","output":%s,"usage":{"input_tokens":%d,"output_tokens":%d}}}`, tc.output, tc.inputTokens, tc.outputTokens)
			h.upstreamEvent(t, terminal)
			actual := h.outcome(t)
			require.NoError(t, actual.err)
			require.NotNil(t, actual.result, "observed generation must retain its authoritative result even if the client requested warmup")
			require.Equal(t, "resp_generated", actual.result.ResponseID)
			require.Equal(t, OpenAIUsage{InputTokens: tc.inputTokens, OutputTokens: tc.outputTokens}, actual.result.Usage)
			require.Equal(t, tc.images, actual.result.ImageCount)
			h.stop(t)
			outcomes, _ := h.snapshot()
			require.Len(t, outcomes, 1)
		})
	}
}
