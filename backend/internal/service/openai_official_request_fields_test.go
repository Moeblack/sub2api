//go:build unit

package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestNormalizeOpenAIResponsesReasoningMode_CurrentModelsAndAliases(t *testing.T) {
	for _, model := range []string{"gpt-5.6", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.6-2026-08-01", "gpt-6-astra", "my-account-alias", "gpt-7"} {
		t.Run(model, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{"model": model, "reasoning": map[string]any{"mode": "pro", "context": "all_turns"}})
			require.NoError(t, err)
			normalized, changed, err := normalizeOpenAIResponsesReasoningMode(body)
			require.NoError(t, err)
			require.False(t, changed)
			require.JSONEq(t, string(body), string(normalized))
		})
	}
}

func TestForward_OfficialResponsesFieldsSurviveOAuthTransforms(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		name := "transformed"
		if passthrough {
			name = "passthrough"
		}
		t.Run(name, func(t *testing.T) {
			s := newAstraOAuthSetup(t, passthrough)
			s.upstream.resp = &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(codexCompletedSSE(`{"id":"resp_test","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))),
			}
			body := []byte(`{"model":"gpt-6-astra","stream":true,"instructions":"Test request","reasoning":{"mode":"standard","effort":"low","context":"all_turns"},"prompt_cache_options":{"mode":"explicit","ttl":"30m"},"tools":[{"type":"function","name":"lookup","async":true,"parameters":{"type":"object","properties":{}}}],"input":[{"type":"function_call","call_id":"call_original:late-42","name":"lookup","arguments":"{}","async":true},{"role":"user","content":"Continue independent work"},{"type":"function_call_output","call_id":"call_original:late-42","output":[{"type":"input_text","text":"late result","prompt_cache_breakpoint":{"mode":"explicit"}}]},{"type":"configuration_update","reasoning":{"effort":"high"}},{"role":"user","content":"Review the result"}]}`)
			result, err := s.svc.Forward(context.Background(), s.c, s.account, body)
			require.NoError(t, err)
			require.NotNil(t, result)
			forwarded := s.upstream.lastBody
			for _, path := range []string{"reasoning", "prompt_cache_options", "tools.0", "input.2.output", "input.3.reasoning"} {
				require.JSONEq(t, gjson.GetBytes(body, path).Raw, gjson.GetBytes(forwarded, path).Raw, path)
			}
			require.Equal(t, int64(5), gjson.GetBytes(forwarded, "input.#").Int())
			require.True(t, gjson.GetBytes(forwarded, "input.0.async").Bool())
			require.Equal(t, "call_original:late-42", gjson.GetBytes(forwarded, "input.0.call_id").String())
			require.Equal(t, "call_original:late-42", gjson.GetBytes(forwarded, "input.2.call_id").String())
			require.Equal(t, "configuration_update", gjson.GetBytes(forwarded, "input.3.type").String())
		})
	}
}

func TestForward_NativeCNResponsesPreservesOutputLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, platform := range []string{PlatformKimi, PlatformMiniMax, PlatformDeepseek} {
		t.Run(platform, func(t *testing.T) {
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"id":"resp_limit","output":[],"usage":{"input_tokens":1,"output_tokens":2}}`)),
			}}
			cfg := &config.Config{}
			cfg.Security.URLAllowlist.Enabled = false
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
			account := &Account{ID: 4, Platform: platform, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"api_key": "test-key", "base_url": "https://example.com", "api_protocol": APIProtocolResponses,
			}}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
			result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"native-model","stream":false,"max_output_tokens":256,"input":"hi"}`))
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, int64(256), gjson.GetBytes(upstream.lastBody, "max_output_tokens").Int())
		})
	}
}

func TestForward_NativeCallIDsRemainOpaque(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		for _, callID := range []string{"call_original-42", "ctc_original:custom-17", "call_" + strings.Repeat("late-original-", 10)} {
			t.Run(callID+"/passthrough="+strconv.FormatBool(passthrough), func(t *testing.T) {
				s := newAstraOAuthSetup(t, passthrough)
				s.upstream.resp = &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(codexCompletedSSE(`{"id":"resp_test","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)))}
				body, err := json.Marshal(map[string]any{
					"model": "gpt-6-astra", "stream": true, "instructions": "Test request",
					"tools": []any{map[string]any{"type": "function", "name": "lookup", "async": true}},
					"input": []any{
						map[string]any{"type": "function_call", "name": "lookup", "arguments": "{}", "call_id": callID, "async": true},
						map[string]any{"role": "user", "content": "Continue independent work"},
						map[string]any{"type": "function_call_output", "call_id": callID, "output": "late result"},
					},
				})
				require.NoError(t, err)
				_, err = s.svc.Forward(context.Background(), s.c, s.account, body)
				require.NoError(t, err)
				require.Equal(t, callID, gjson.GetBytes(s.upstream.lastBody, "input.0.call_id").String())
				require.Equal(t, callID, gjson.GetBytes(s.upstream.lastBody, "input.2.call_id").String())
			})
		}
	}
}
