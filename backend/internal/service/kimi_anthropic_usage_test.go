package service

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/stretchr/testify/require"
)

func TestParseSSEUsagePassthroughNormalizesKimiPromptUsage(t *testing.T) {
	usage := &ClaudeUsage{}

	parseSSEUsagePassthrough(`{"type":"message_start","message":{"usage":{"input_tokens":173306,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":0,"prompt_tokens":173306,"cached_tokens":0}}}`, usage)
	require.Equal(t, 173306, usage.InputTokens)
	require.Zero(t, usage.CacheReadInputTokens)

	parseSSEUsagePassthrough(`{"type":"message_delta","usage":{"input_tokens":250,"cache_creation_input_tokens":0,"cache_read_input_tokens":173056,"output_tokens":166,"prompt_tokens":173306,"cached_tokens":173056}}`, usage)
	require.Equal(t, 250, usage.InputTokens, "Kimi message_delta input_tokens is already the uncached bucket")
	require.Equal(t, 173056, usage.CacheReadInputTokens)
	require.Equal(t, 166, usage.OutputTokens)
}

func TestParseSSEUsagePassthroughKimiFullyCachedInputReplacesStartTotal(t *testing.T) {
	usage := &ClaudeUsage{}

	parseSSEUsagePassthrough(`{"type":"message_start","message":{"usage":{"input_tokens":173306,"prompt_tokens":173306}}}`, usage)
	parseSSEUsagePassthrough(`{"type":"message_delta","usage":{"input_tokens":0,"cache_read_input_tokens":173306,"output_tokens":8,"prompt_tokens":173306,"cached_tokens":173306}}`, usage)

	require.Zero(t, usage.InputTokens, "an explicit zero uncached bucket must not retain message_start's total")
	require.Equal(t, 173306, usage.CacheReadInputTokens)
}

func TestParseClaudeUsageFromResponseBodyNormalizesCNProviderAliases(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		wantInput     int
		wantCacheRead int
		wantOutput    int
	}{
		{
			name:          "Kimi top-level cached_tokens",
			body:          `{"usage":{"input_tokens":173306,"output_tokens":166,"cache_read_input_tokens":173056,"prompt_tokens":173306,"cached_tokens":173056}}`,
			wantInput:     250,
			wantCacheRead: 173056,
			wantOutput:    166,
		},
		{
			name:          "GLM nested prompt cache details",
			body:          `{"usage":{"input_tokens":1200,"output_tokens":300,"prompt_tokens":1200,"prompt_tokens_details":{"cached_tokens":800}}}`,
			wantInput:     400,
			wantCacheRead: 800,
			wantOutput:    300,
		},
		{
			name:          "DeepSeek prompt cache hit and miss buckets",
			body:          `{"usage":{"input_tokens":1200,"output_tokens":300,"prompt_cache_hit_tokens":800,"prompt_cache_miss_tokens":400}}`,
			wantInput:     400,
			wantCacheRead: 800,
			wantOutput:    300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			usage := parseClaudeUsageFromResponseBody([]byte(tt.body))
			require.Equal(t, tt.wantInput, usage.InputTokens)
			require.Equal(t, tt.wantCacheRead, usage.CacheReadInputTokens)
			require.Equal(t, tt.wantOutput, usage.OutputTokens)
		})
	}
}

func TestParseSSEUsagePassthroughNormalizesGLMAndDeepSeekAliases(t *testing.T) {
	tests := []struct {
		name          string
		data          string
		wantInput     int
		wantCacheRead int
	}{
		{
			name:          "GLM",
			data:          `{"type":"message_delta","usage":{"input_tokens":1200,"output_tokens":30,"prompt_tokens":1200,"prompt_tokens_details":{"cached_tokens":800}}}`,
			wantInput:     400,
			wantCacheRead: 800,
		},
		{
			name:          "DeepSeek",
			data:          `{"type":"message_delta","usage":{"input_tokens":1200,"output_tokens":30,"prompt_cache_hit_tokens":800,"prompt_cache_miss_tokens":400}}`,
			wantInput:     400,
			wantCacheRead: 800,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			usage := &ClaudeUsage{}
			parseSSEUsagePassthrough(tt.data, usage)
			require.Equal(t, tt.wantInput, usage.InputTokens)
			require.Equal(t, tt.wantCacheRead, usage.CacheReadInputTokens)
			require.Equal(t, 30, usage.OutputTokens)
		})
	}
}

func TestMergeAnthropicUsageNormalizesKimiStreamForOpenAIBilling(t *testing.T) {
	var start apicompat.AnthropicStreamEvent
	require.NoError(t, json.Unmarshal([]byte(`{"type":"message_start","message":{"usage":{"input_tokens":173306,"prompt_tokens":173306,"cached_tokens":0}}}`), &start))
	var delta apicompat.AnthropicStreamEvent
	require.NoError(t, json.Unmarshal([]byte(`{"type":"message_delta","usage":{"input_tokens":250,"cache_read_input_tokens":173056,"output_tokens":166,"prompt_tokens":173306,"cached_tokens":173056}}`), &delta))

	usage := &ClaudeUsage{}
	mergeAnthropicUsage(usage, start.Message.Usage)
	mergeAnthropicUsage(usage, *delta.Usage)
	require.Equal(t, 250, usage.InputTokens)
	require.Equal(t, 173056, usage.CacheReadInputTokens)

	openAIUsage := claudeUsageToOpenAIUsage(usage)
	require.Equal(t, 173306, openAIUsage.InputTokens, "OpenAI gateway expects an inclusive input total")
	require.Equal(t, 250, openAIUsage.InputTokens-openAIUsage.CacheReadInputTokens-openAIUsage.CacheCreationInputTokens)
	require.Equal(t, 166, openAIUsage.OutputTokens)
}

func TestMergeAnthropicUsageNormalizesGLMAndDeepSeekAliases(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "GLM",
			raw:  `{"input_tokens":1200,"output_tokens":30,"prompt_tokens":1200,"prompt_tokens_details":{"cached_tokens":800}}`,
		},
		{
			name: "DeepSeek",
			raw:  `{"input_tokens":1200,"output_tokens":30,"prompt_cache_hit_tokens":800,"prompt_cache_miss_tokens":400}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var src apicompat.AnthropicUsage
			require.NoError(t, json.Unmarshal([]byte(tt.raw), &src))

			usage := &ClaudeUsage{}
			mergeAnthropicUsage(usage, src)
			require.Equal(t, 400, usage.InputTokens)
			require.Equal(t, 800, usage.CacheReadInputTokens)

			openAIUsage := claudeUsageToOpenAIUsage(usage)
			require.Equal(t, 1200, openAIUsage.InputTokens)
			require.Equal(t, 400, openAIUsage.InputTokens-openAIUsage.CacheReadInputTokens-openAIUsage.CacheCreationInputTokens)
		})
	}
}

func TestClaudeUsageToOpenAIUsagePreservesCNProviderNativeAnthropicBuckets(t *testing.T) {
	tests := []struct {
		name         string
		usage        ClaudeUsage
		wantTotal    int
		wantUncached int
	}{
		{
			name: "GLM",
			usage: ClaudeUsage{
				InputTokens:              2,
				OutputTokens:             302,
				CacheCreationInputTokens: 733,
				CacheReadInputTokens:     376156,
			},
			wantTotal:    376891,
			wantUncached: 2,
		},
		{
			name: "DeepSeek",
			usage: ClaudeUsage{
				InputTokens:          400,
				OutputTokens:         30,
				CacheReadInputTokens: 800,
			},
			wantTotal:    1200,
			wantUncached: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			openAIUsage := claudeUsageToOpenAIUsage(&tt.usage)
			require.Equal(t, tt.wantTotal, openAIUsage.InputTokens)
			require.Equal(t, tt.wantUncached, openAIUsage.InputTokens-openAIUsage.CacheReadInputTokens-openAIUsage.CacheCreationInputTokens)
			require.Equal(t, tt.usage.CacheReadInputTokens, openAIUsage.CacheReadInputTokens)
			require.Equal(t, tt.usage.CacheCreationInputTokens, openAIUsage.CacheCreationInputTokens)
		})
	}
}

func TestCNProviderAnthropicUsageBillsUncachedInput(t *testing.T) {
	tests := []struct {
		name      string
		model     string
		body      string
		wantInput int
	}{
		{
			name:      "Kimi",
			model:     "k3",
			body:      `{"usage":{"input_tokens":173306,"output_tokens":166,"prompt_tokens":173306,"cached_tokens":173056}}`,
			wantInput: 250,
		},
		{
			name:      "GLM",
			model:     "glm-5.2",
			body:      `{"usage":{"input_tokens":1200,"output_tokens":30,"prompt_tokens":1200,"prompt_tokens_details":{"cached_tokens":800}}}`,
			wantInput: 400,
		},
		{
			name:      "DeepSeek",
			model:     "deepseek-v4-flash",
			body:      `{"usage":{"input_tokens":1200,"output_tokens":30,"prompt_cache_hit_tokens":800,"prompt_cache_miss_tokens":400}}`,
			wantInput: 400,
		},
	}

	billing := NewBillingService(&config.Config{}, nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claudeUsage := parseClaudeUsageFromResponseBody([]byte(tt.body))
			openAIUsage := claudeUsageToOpenAIUsage(claudeUsage)
			uncachedInput := max(openAIUsage.InputTokens-openAIUsage.CacheReadInputTokens-openAIUsage.CacheCreationInputTokens, 0)
			require.Equal(t, tt.wantInput, uncachedInput)

			cost, err := billing.CalculateCost(tt.model, UsageTokens{
				InputTokens:         uncachedInput,
				OutputTokens:        openAIUsage.OutputTokens,
				CacheCreationTokens: openAIUsage.CacheCreationInputTokens,
				CacheReadTokens:     openAIUsage.CacheReadInputTokens,
			}, 1)
			require.NoError(t, err)
			require.Positive(t, cost.InputCost, "uncached input must contribute to the final charge")

			pricing, err := billing.GetModelPricing(tt.model)
			require.NoError(t, err)
			require.InDelta(t, float64(tt.wantInput)*pricing.InputPricePerToken, cost.InputCost, 1e-12)
		})
	}
}

func TestMergeAnthropicUsageCacheCreationBilling(t *testing.T) {
	pricing := &ModelPricing{
		SupportsCacheBreakdown: true,
		CacheCreation5mPrice:   4e-6,
		CacheCreation1hPrice:   5e-6,
	}
	for _, tt := range []struct {
		name     string
		raw      string
		want5m   int
		want1h   int
		wantCost float64
	}{
		{
			name:     "mixed TTL details retain distinct prices",
			raw:      `{"input_tokens":20,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":70}}`,
			want5m:   30,
			want1h:   70,
			wantCost: 30*4e-6 + 70*5e-6,
		},
		{
			name:     "missing aggregate falls back to details",
			raw:      `{"input_tokens":20,"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":70}}`,
			want5m:   30,
			want1h:   70,
			wantCost: 30*4e-6 + 70*5e-6,
		},
		{
			name:     "missing details retains aggregate 5m billing fallback",
			raw:      `{"input_tokens":20,"cache_creation_input_tokens":100}`,
			wantCost: 100 * 4e-6,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var src apicompat.AnthropicUsage
			require.NoError(t, json.Unmarshal([]byte(tt.raw), &src))
			usage := &ClaudeUsage{}
			mergeAnthropicUsage(usage, src)
			require.Equal(t, 100, usage.CacheCreationInputTokens)
			require.Equal(t, tt.want5m, usage.CacheCreation5mTokens)
			require.Equal(t, tt.want1h, usage.CacheCreation1hTokens)
			tokens := UsageTokens{
				CacheCreationTokens:   usage.CacheCreationInputTokens,
				CacheCreation5mTokens: usage.CacheCreation5mTokens,
				CacheCreation1hTokens: usage.CacheCreation1hTokens,
			}
			billing := &BillingService{}
			require.InDelta(t, tt.wantCost, billing.computeCacheCreationCost(pricing, tokens, 0, 1), 1e-12)
		})
	}
}

func TestMergeAnthropicUsageCacheCreationSSESelectiveUpdates(t *testing.T) {
	usage := &ClaudeUsage{}
	for _, tt := range []struct {
		raw    string
		want5m int
		want1h int
	}{
		{
			raw:    `{"type":"message_start","message":{"usage":{"input_tokens":20,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":70}}}}`,
			want5m: 30,
			want1h: 70,
		},
		{
			raw:    `{"type":"message_delta","usage":{"output_tokens":8}}`,
			want5m: 30,
			want1h: 70,
		},
		{
			raw:    `{"type":"message_delta","usage":{"output_tokens":9,"cache_creation":{}}}`,
			want5m: 30,
			want1h: 70,
		},
		{
			raw:    `{"type":"message_delta","usage":{"cache_creation_input_tokens":110,"cache_creation":{"ephemeral_1h_input_tokens":80}}}`,
			want5m: 30,
			want1h: 80,
		},
		{
			raw:    `{"type":"message_delta","usage":{"cache_creation_input_tokens":30,"cache_creation":{"ephemeral_1h_input_tokens":0}}}`,
			want5m: 30,
			want1h: 0,
		},
		{
			raw:    `{"type":"message_delta","usage":{"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}}`,
			want5m: 0,
			want1h: 0,
		},
	} {
		var event apicompat.AnthropicStreamEvent
		require.NoError(t, json.Unmarshal([]byte(tt.raw), &event))
		if event.Message != nil {
			mergeAnthropicUsage(usage, event.Message.Usage)
		} else {
			require.NotNil(t, event.Usage)
			mergeAnthropicUsage(usage, *event.Usage)
		}
		require.Equal(t, 20, usage.InputTokens)
		require.Equal(t, tt.want5m, usage.CacheCreation5mTokens)
		require.Equal(t, tt.want1h, usage.CacheCreation1hTokens)
		require.Equal(t, tt.want5m+tt.want1h, usage.CacheCreationInputTokens)
	}
	require.Equal(t, 9, usage.OutputTokens)
}

func TestMergeAnthropicUsageCacheCreationPreservesProviderZeroInput(t *testing.T) {
	for _, raw := range []string{
		`{"input_tokens":0,"prompt_tokens":1000,"cached_tokens":900,"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":70}}`,
		`{"input_tokens":1000,"prompt_tokens":1000,"prompt_tokens_details":{"cached_tokens":900},"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":70}}`,
		`{"input_tokens":1000,"prompt_cache_hit_tokens":900,"prompt_cache_miss_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":70}}`,
	} {
		usage := &ClaudeUsage{InputTokens: 1000}
		var src apicompat.AnthropicUsage
		require.NoError(t, json.Unmarshal([]byte(raw), &src))
		mergeAnthropicUsage(usage, src)
		require.Zero(t, usage.InputTokens, "authoritative provider buckets must replace a previous nonzero input total")
		require.Equal(t, 900, usage.CacheReadInputTokens)
		require.Equal(t, 100, usage.CacheCreationInputTokens)
		require.Equal(t, 30, usage.CacheCreation5mTokens)
		require.Equal(t, 70, usage.CacheCreation1hTokens)
	}
}

func TestMergeAnthropicUsageCacheCreationOmissionKeepsExplicitAggregate(t *testing.T) {
	usage := &ClaudeUsage{}
	for _, raw := range []string{
		`{"input_tokens":20,"cache_creation_input_tokens":120,"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":70}}`,
		`{"output_tokens":8}`,
		`{"output_tokens":9,"cache_creation":{}}`,
	} {
		var src apicompat.AnthropicUsage
		require.NoError(t, json.Unmarshal([]byte(raw), &src))
		mergeAnthropicUsage(usage, src)
		require.Equal(t, 120, usage.CacheCreationInputTokens, "an omitted breakdown must not replace the authoritative aggregate with a partial detail sum")
		require.Equal(t, 30, usage.CacheCreation5mTokens)
		require.Equal(t, 70, usage.CacheCreation1hTokens)
	}
}
