//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func protocolFidelityContext(beta string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request.Header.Set("Anthropic-Beta", beta)
	return c
}

func TestProtocolFidelityExplicitCapabilities(t *testing.T) {
	svc := &GatewayService{cfg: &config.Config{}}
	for _, token := range []string{
		claude.BetaStructuredOutputs,
		claude.BetaMidConversationOutputConfig,
		claude.BetaMidConversationToolChanges,
		claude.BetaInlineTools,
		claude.BetaDangerousToolUse,
	} {
		for _, tc := range []struct {
			name, incoming string
			drop           bool
			want           bool
		}{
			{"explicit", token, false, true},
			{"duplicate", token + ", " + token, false, true},
			{"absent", "", false, false},
			{"not a prefix match", token + "-other", false, false},
			{"policy drop wins", token, true, false},
		} {
			t.Run(token+"/"+tc.name, func(t *testing.T) {
				h := http.Header{}
				h.Set("Anthropic-Beta", tc.incoming+",unknown-beta")
				var drop map[string]struct{}
				if tc.drop {
					drop = map[string]struct{}{token: {}}
				}
				final, set := svc.computeFinalAnthropicBeta("oauth", true, "custom-model", h, []byte(`{}`), drop)
				require.True(t, set)
				require.Equal(t, tc.want, anthropicBetaTokensContains(final, token))
				require.False(t, anthropicBetaTokensContains(final, "unknown-beta"))
				if tc.want {
					require.Equal(t, 1, strings.Count(final, token))
				}
			})
		}
	}
	// Companion tokens must not be inferred from tool changes or body fields.
	h := http.Header{"Anthropic-Beta": []string{claude.BetaInlineTools}}
	final, _ := svc.computeFinalAnthropicBeta("oauth", true, "custom-model", h,
		[]byte(`{"safeguards":{},"messages":[{"role":"system","output_config":{}}]}`), nil)
	require.False(t, anthropicBetaTokensContains(final, claude.BetaMidConversationToolChanges))
	require.False(t, anthropicBetaTokensContains(final, claude.BetaMidConversationOutputConfig))
	require.False(t, anthropicBetaTokensContains(final, claude.BetaDangerousToolUse))
}

func TestProtocolFidelityOutputConfigControlMessages(t *testing.T) {
	for _, content := range []string{"", `,"content":null`, `,"content":""`, `,"content":[]`, `,"content":[{"type":"text","text":""}]`} {
		body := []byte(`{"output_config":{"effort":"high"},"messages":[{"role":"system","output_config":{"effort":"low"}` + content + `},{"role":"user","content":"keep"},{"role":"system","content":[],"output_config":{}}]}`)
		preserved, changed := sanitizeAnthropicBodyForBetaTokens(body, claude.BetaMidConversationOutputConfig)
		require.False(t, changed)
		require.Equal(t, body, preserved)
		out, changed := sanitizeAnthropicBodyForBetaTokens(body, "")
		require.True(t, changed)
		require.Len(t, gjson.GetBytes(out, "messages").Array(), 1)
		require.Equal(t, "keep", gjson.GetBytes(out, "messages.0.content").String())
		require.Equal(t, "high", gjson.GetBytes(out, "output_config.effort").String())
		again, changed := sanitizeAnthropicBodyForBetaTokens(out, "")
		require.False(t, changed)
		require.Equal(t, out, again)
	}
	for _, msg := range []string{
		`{"role":"system","content":"instructions","output_config":{}}`,
		`{"role":"system","content":[{"type":"future","opaque":42}],"output_config":{}}`,
		`{"role":"system","content":[],"safeguards":{"future":true},"output_config":{}}`,
		`{"role":"system","content":[{"type":"text","text":"","future":42}],"output_config":{}}`,
		`{"role":"user","content":[],"output_config":{}}`,
		`{"role":"assistant","content":[],"output_config":{}}`,
	} {
		body := []byte(`{"messages":[` + msg + `]}`)
		out, changed := sanitizeAnthropicBodyForBetaTokens(body, "")
		require.True(t, changed)
		require.Len(t, gjson.GetBytes(out, "messages").Array(), 1)
		require.False(t, gjson.GetBytes(out, "messages.0.output_config").Exists())
	}
	body := []byte(`{"output_config":{"effort":"low"},"messages":[{"role":"system","content":[]}]}`)
	out, changed := sanitizeAnthropicBodyForBetaTokens(body, "")
	require.False(t, changed)
	require.Equal(t, body, out)
}

func TestProtocolFidelityFinalSafetyCapabilities(t *testing.T) {
	svc := &GatewayService{cfg: &config.Config{}}
	body := []byte(`{"model":"custom-model","safeguards":{"future_schema":[{"tool_use_id":"toolu_1","opaque":true}]},"messages":[{"role":"user","content":"hi"}]}`)
	for _, mode := range []string{"oauth", "apikey", "passthrough", "count oauth", "count passthrough"} {
		for _, blocked := range []bool{false, true} {
			t.Run(mode+"/"+map[bool]string{false: "paired", true: "filtered"}[blocked], func(t *testing.T) {
				c := protocolFidelityContext(claude.BetaDangerousToolUse)
				if blocked {
					c.Set(betaPolicyFilterSetKey, map[string]struct{}{claude.BetaDangerousToolUse: {}})
				}
				account := &Account{ID: 701, Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
				var req *http.Request
				var err error
				switch mode {
				case "oauth", "count oauth":
					account.Type = AccountTypeOAuth
					if mode == "oauth" {
						req, _, err = svc.buildUpstreamRequest(context.Background(), c, account, body, "test-token", "oauth", "custom-model", false, true)
					} else {
						req, _, err = svc.buildCountTokensRequest(context.Background(), c, account, body, "test-token", "oauth", "custom-model", true)
					}
				case "apikey":
					req, _, err = svc.buildUpstreamRequest(context.Background(), c, account, body, "test-token", "apikey", "custom-model", false, false)
				case "passthrough":
					req, _, err = svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "test-token")
				case "count passthrough":
					req, err = svc.buildCountTokensRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "test-token")
				}
				if blocked {
					require.Error(t, err)
					var local *BetaBlockedError
					require.ErrorAs(t, err, &local)
					require.Contains(t, err.Error(), "without removing safety checks")
					require.Nil(t, req)
					return
				}
				require.NoError(t, err)
				wire, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Equal(t, gjson.GetBytes(body, "safeguards").Raw, gjson.GetBytes(wire, "safeguards").Raw)
				require.True(t, anthropicBetaTokensContains(getHeaderRaw(req.Header, "anthropic-beta"), claude.BetaDangerousToolUse))
			})
		}
	}
	// Unknown schemas remain opaque, including null/array shapes. The gateway
	// only checks capability pairing; upstream schema validation is authoritative.
	for _, raw := range []string{`null`, `[]`, `{"unknown":{"version":999}}`} {
		b := []byte(`{"safeguards":` + raw + `}`)
		require.NoError(t, validateAnthropicSafetyCapabilities(b, claude.BetaDangerousToolUse))
		require.Error(t, validateAnthropicSafetyCapabilities(b, ""))
		out, changed := sanitizeAnthropicBodyForBetaTokens(b, "")
		require.False(t, changed)
		require.Equal(t, b, out)
	}
	_, err := PrepareBedrockRequestBody(body, "us.anthropic.claude-sonnet-4-6-v1", claude.BetaDangerousToolUse)
	require.Error(t, err, "backend whitelist must not silently downgrade safeguards")
	_, err = svc.buildUpstreamRequestAnthropicVertex(context.Background(), protocolFidelityContext(claude.BetaDangerousToolUse),
		&Account{Platform: PlatformAnthropic, Type: AccountTypeServiceAccount}, body, "test-token", "custom-model", false)
	require.ErrorContains(t, err, "safeguards requires")
}

func TestProtocolFidelityOverrideCannotUndoPolicyDrop(t *testing.T) {
	svc := &GatewayService{cfg: &config.Config{}}
	for _, endpoint := range []string{"messages", "count", "passthrough", "count passthrough"} {
		t.Run(endpoint, func(t *testing.T) {
			c := protocolFidelityContext(claude.BetaDangerousToolUse)
			c.Set(betaPolicyFilterSetKey, map[string]struct{}{claude.BetaDangerousToolUse: {}})
			account := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Credentials: map[string]any{
				credKeyHeaderOverrideEnabled: true,
				credKeyHeaderOverrides:       map[string]any{"anthropic-beta": claude.BetaDangerousToolUse},
			}}
			for _, safety := range []bool{false, true} {
				body := []byte(`{"model":"custom-model","messages":[]}`)
				if safety {
					body = []byte(`{"model":"custom-model","safeguards":{},"messages":[]}`)
				}
				var req *http.Request
				var err error
				switch endpoint {
				case "messages":
					req, _, err = svc.buildUpstreamRequest(context.Background(), c, account, body, "k", "apikey", "custom-model", false, false)
				case "count":
					req, _, err = svc.buildCountTokensRequest(context.Background(), c, account, body, "k", "apikey", "custom-model", false)
				case "passthrough":
					req, _, err = svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "k")
				case "count passthrough":
					req, err = svc.buildCountTokensRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "k")
				}
				if safety {
					require.ErrorContains(t, err, "safeguards requires")
				} else {
					require.NoError(t, err)
					require.Empty(t, getHeaderRaw(req.Header, "anthropic-beta"))
				}
			}
		})
	}
}

func TestProtocolFidelityAPIKeyFeatureAndAgentHeaders(t *testing.T) {
	svc := &GatewayService{cfg: &config.Config{}}
	account := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	for _, supplied := range []bool{true, false} {
		c := protocolFidelityContext(claude.BetaDangerousToolUse)
		for _, key := range []string{"Authorization", "X-Api-Key", "X-Goog-Api-Key", "Cookie"} {
			c.Request.Header.Set(key, "inbound-secret")
		}
		features := []string{"X-Claude-Code-Agent-Id", "X-Claude-Code-Parent-Agent-Id", "Anthropic-Auto-Mode-Feature", "X-Claude-Code-Feature"}
		if supplied {
			for _, key := range features {
				c.Request.Header.Set(key, "client-owned")
			}
		}
		body := []byte(`{"model":"custom-model","safeguards":{"opaque":true},"messages":[]}`)
		req, _, err := svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "upstream-key")
		require.NoError(t, err)
		count, err := svc.buildCountTokensRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "upstream-key")
		require.NoError(t, err)
		for _, r := range []*http.Request{req, count} {
			for _, key := range features {
				if supplied {
					require.Equal(t, "client-owned", getHeaderRaw(r.Header, key))
				} else {
					require.Empty(t, getHeaderRaw(r.Header, key), "never synthesize identity/feature headers")
				}
			}
			require.Equal(t, "upstream-key", getHeaderRaw(r.Header, "x-api-key"))
			for _, key := range []string{"Authorization", "X-Goog-Api-Key", "Cookie"} {
				require.Empty(t, getHeaderRaw(r.Header, key))
			}
		}
	}
	require.True(t, allowedHeaders["x-claude-code-agent-id"])
	require.True(t, allowedHeaders["x-claude-code-parent-agent-id"])
}

func TestProtocolFidelitySafeResponseHeaders(t *testing.T) {
	src := http.Header{
		"X-Should-Retry":                     []string{"false"},
		"Anthropic-Ratelimit-Unified-Status": []string{"allowed"},
		"Anthropic-Organization-Id":          []string{"private-org"},
		"Set-Cookie":                         []string{"private-cookie"},
	}
	for _, filter := range []*responseheaders.CompiledHeaderFilter{nil, responseheaders.CompileHeaderFilter(config.ResponseHeaderConfig{})} {
		dst := http.Header{}
		writeAnthropicPassthroughResponseHeaders(dst, src, filter)
		require.Equal(t, "false", dst.Get("X-Should-Retry"))
		require.Equal(t, "allowed", dst.Get("Anthropic-Ratelimit-Unified-Status"))
		require.Empty(t, dst.Get("Anthropic-Organization-Id"))
		require.Empty(t, dst.Get("Set-Cookie"))
	}
	filter := responseheaders.CompileHeaderFilter(config.ResponseHeaderConfig{
		Enabled: true, ForceRemove: []string{"x-should-retry", "anthropic-ratelimit-unified-status"},
	})
	dst := http.Header{}
	writeAnthropicPassthroughResponseHeaders(dst, src, filter)
	require.Empty(t, dst)
}

func TestProtocolFidelityOptionalCapabilitiesOnWire(t *testing.T) {
	svc := &GatewayService{cfg: &config.Config{}}
	betas := strings.Join([]string{claude.BetaStructuredOutputs, claude.BetaMidConversationOutputConfig,
		claude.BetaMidConversationToolChanges, claude.BetaInlineTools}, ",")
	body := []byte(`{"model":"private-custom-model","output_config":{"effort":"high"},"output_format":{"type":"json_schema","schema":{"type":"object"}},"messages":[{"role":"system","content":[],"output_config":{"effort":"low"}},{"role":"user","content":[{"type":"tool_addition","tool":{"type":"tool_reference","tool_name":"lookup"}},{"type":"text","text":"hi"}]}]}`)
	for _, dropped := range []bool{false, true} {
		c := protocolFidelityContext(betas)
		if dropped {
			c.Set(betaPolicyFilterSetKey, map[string]struct{}{claude.BetaMidConversationOutputConfig: {}})
		}
		account := &Account{Platform: PlatformAnthropic, Type: AccountTypeOAuth}
		req, wire, err := svc.buildUpstreamRequest(context.Background(), c, account, body, "test-token", "oauth", "private-custom-model", false, true)
		require.NoError(t, err)
		require.Equal(t, "private-custom-model", gjson.GetBytes(wire, "model").String())
		require.Equal(t, "high", gjson.GetBytes(wire, "output_config.effort").String())
		require.Equal(t, gjson.GetBytes(body, "output_format").Raw, gjson.GetBytes(wire, "output_format").Raw)
		messages := gjson.GetBytes(wire, "messages").Array()
		if dropped {
			require.Len(t, messages, 1)
		} else {
			require.Len(t, messages, 2)
			require.Equal(t, "low", messages[0].Get("output_config.effort").String())
		}
		require.Equal(t, gjson.GetBytes(body, "messages.1.content").Raw, messages[len(messages)-1].Get("content").Raw)
		final := getHeaderRaw(req.Header, "anthropic-beta")
		require.Equal(t, !dropped, anthropicBetaTokensContains(final, claude.BetaMidConversationOutputConfig))
		for _, token := range []string{claude.BetaStructuredOutputs, claude.BetaMidConversationToolChanges, claude.BetaInlineTools} {
			require.True(t, anthropicBetaTokensContains(final, token))
		}
	}
}

func TestProtocolFidelitySafeguardResultsAreOpaque(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request.Header.Set("Anthropic-Beta", claude.BetaDangerousToolUse)
	body := []byte(`{"model":"private-custom-model","safeguards":{"opaque":{"action_id":"toolu_1"}},"messages":[{"role":"user","content":"hi"}]}`)
	response := `{"id":"msg_1","type":"message","safeguard_results":{"toolu_1":{"decision":"deny","unknown":42}},"content":[{"type":"tool_use","id":"toolu_1","name":"probe","input":{}}],"usage":{"input_tokens":12,"output_tokens":7}}`
	upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(response)),
	}}
	svc := &GatewayService{cfg: &config.Config{}, httpUpstream: upstream, rateLimitService: &RateLimitService{}}
	_, err := svc.forwardAnthropicAPIKeyPassthrough(context.Background(), c, newAnthropicAPIKeyAccountForTest(), body,
		"private-custom-model", "private-custom-model", false, time.Now())
	require.NoError(t, err)
	require.Equal(t, response, recorder.Body.String(), "never synthesize or reinterpret safety decisions or tool IDs")
	require.Equal(t, gjson.GetBytes(body, "safeguards").Raw, gjson.GetBytes(upstream.lastBody, "safeguards").Raw)
}
