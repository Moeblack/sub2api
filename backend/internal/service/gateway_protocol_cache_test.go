//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestProtocolCacheClientSystemBreakpoints(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet-4-6","system":[{"type":"text","text":"one","cache_control":{"type":"ephemeral","ttl":"5m","opaque":"keep"}},{"type":"text","text":"two"},{"type":"text","text":"three","cache_control":{"type":"ephemeral","ttl":"1h"}}],"messages":[{"role":"user","content":"hi"}]}`)
	out, _ := normalizeClaudeOAuthRequestBody(body, "claude-sonnet-4-6", claudeOAuthNormalizeOptions{})
	require.Equal(t, gjson.GetBytes(body, "system").Raw, gjson.GetBytes(out, "system").Raw)
	require.False(t, gjson.GetBytes(out, "system.1.cache_control").Exists())

	// Normalization must not erase breakpoints introduced by the configured
	// system injection either; the outbound limit is the only count guard.
	injected := rewriteSystemForNonClaudeCode([]byte(`{"messages":[{"role":"user","content":"hi"}]}`), "instructions")
	out, _ = normalizeClaudeOAuthRequestBody(injected, "claude-sonnet-4-6", claudeOAuthNormalizeOptions{})
	_, _, _, paths := collectCacheControlPaths(out)
	require.NotEmpty(t, paths)
}

func TestProtocolCacheTTLOrder(t *testing.T) {
	// Deliberately put JSON fields in the opposite order from Anthropic's cache
	// traversal: tools -> system -> messages -> automatic top-level breakpoint.
	body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"long","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"text","text":"short","cache_control":{"type":"ephemeral","ttl":"5m"}}]}],"system":[{"type":"text","text":"sys","cache_control":{"type":"ephemeral","ttl":"5m","opaque":true}}],"tools":[{"name":"tool","cache_control":{"type":"ephemeral"}}]}`)
	out := enforceCacheControlLimit(body)
	require.Equal(t, "1h", gjson.GetBytes(out, "tools.0.cache_control.ttl").String())
	require.Equal(t, "1h", gjson.GetBytes(out, "system.0.cache_control.ttl").String())
	require.True(t, gjson.GetBytes(out, "system.0.cache_control.opaque").Bool())
	require.Equal(t, "1h", gjson.GetBytes(out, "messages.0.content.0.cache_control.ttl").String())
	require.Equal(t, "5m", gjson.GetBytes(out, "messages.0.content.1.cache_control.ttl").String())
	require.Equal(t, out, enforceCacheControlLimit(out), "normalization is idempotent")

	for _, raw := range []string{
		`{"messages":[{"role":"user","content":"hi"}]}`,
		`{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"1h"}}],"messages":[{"role":"user","content":[{"type":"text","text":"m","cache_control":{"type":"ephemeral"}}]}]}`,
		`{"tools":[{"name":"t","cache_control":{"type":"ephemeral"}}],"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"5m"}}]}`,
	} {
		require.Equal(t, raw, string(enforceCacheControlLimit([]byte(raw))), "valid ordering is a byte no-op")
	}
	automatic := []byte(`{"cache_control":{"type":"ephemeral","ttl":"1h"},"system":[{"type":"text","text":"sys","cache_control":{"type":"ephemeral"}}],"messages":[]}`)
	require.Equal(t, "1h", gjson.GetBytes(normalizeCacheControlTTLOrder(automatic), "system.0.cache_control.ttl").String())
}

func TestProtocolCacheLimitBeforeTTLPromotion(t *testing.T) {
	body := []byte(`{"system":[{"type":"text","text":"sys","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":[{"type":"text","text":"removed long","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"text","text":"a","cache_control":{"type":"ephemeral"}},{"type":"text","text":"b","cache_control":{"type":"ephemeral"}},{"type":"text","text":"c","cache_control":{"type":"ephemeral"}}]}],"tools":[{"name":"t","cache_control":{"type":"ephemeral"}}]}`)
	out := enforceCacheControlLimit(body)
	require.False(t, gjson.GetBytes(out, "tools.0.cache_control").Exists())
	require.False(t, gjson.GetBytes(out, "messages.0.content.0.cache_control").Exists())
	require.False(t, gjson.GetBytes(out, "system.0.cache_control.ttl").Exists(), "removed 1h breakpoint must not promote the surviving prefix")
	_, messages, tools, system := collectCacheControlPaths(out)
	require.Equal(t, 4, len(messages)+len(tools)+len(system))
}

func TestProtocolCacheCountTokensOutboundLimitAndTTL(t *testing.T) {
	for _, threeMessages := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", nil)
		c.Request.Header.Set("User-Agent", "third-party-client/1.0")
		messages := `{"role":"user","content":[{"type":"text","text":"one","cache_control":{"type":"ephemeral","ttl":"1h"}}]}`
		if threeMessages {
			messages += `,{"role":"assistant","content":[{"type":"text","text":"two","cache_control":{"type":"ephemeral"}}]},{"role":"user","content":[{"type":"text","text":"three","cache_control":{"type":"ephemeral"}}]}`
		}
		body := []byte(`{"model":"claude-sonnet-4-6","system":[{"type":"text","text":"stable prefix","cache_control":{"type":"ephemeral"}}],"tools":[{"name":"probe","input_schema":{"type":"object"}}],"messages":[` + messages + `]}`)
		upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"input_tokens":42}`)),
		}}
		svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream, rateLimitService: &RateLimitService{}}
		account := &Account{ID: 401, Platform: PlatformAnthropic, Type: AccountTypeSetupToken, Concurrency: 1,
			Credentials: map[string]any{"access_token": "test-token"}, Status: StatusActive, Schedulable: true}
		parsed := &ParsedRequest{Body: NewRequestBodyRef(body), Model: "claude-sonnet-4-6"}
		require.NoError(t, svc.ForwardCountTokens(context.Background(), c, account, parsed))
		_, messagesPaths, tools, system := collectCacheControlPaths(upstream.lastBody)
		require.LessOrEqual(t, len(messagesPaths)+len(tools)+len(system), 4)
		require.NotEmpty(t, system, "count_tokens must retain client system breakpoints")
		paths := append(append(append([]string{}, tools...), system...), messagesPaths...)
		seenShort := false
		for _, path := range paths {
			if gjson.GetBytes(upstream.lastBody, path+".ttl").String() == "1h" {
				require.False(t, seenShort, "1h cannot follow a shorter TTL")
			} else {
				seenShort = true
			}
		}
	}
}

func TestProtocolSafetyCountTokensRejectsLocally(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", nil)
		c.Request.Header.Set("Anthropic-Beta", claude.BetaDangerousToolUse)
		c.Set(betaPolicyFilterSetKey, map[string]struct{}{claude.BetaDangerousToolUse: {}})
		account := &Account{ID: 402, Platform: PlatformAnthropic, Type: AccountTypeSetupToken,
			Credentials: map[string]any{"access_token": "test-token"}}
		svc := &GatewayService{cfg: &config.Config{}}
		body := []byte(`{"model":"claude-sonnet-4-6","safeguards":{"opaque":true},"messages":[{"role":"user","content":"hi"}]}`)
		var err error
		if passthrough {
			account.Type = AccountTypeAPIKey
			account.Credentials = map[string]any{"api_key": "test-key"}
			err = svc.forwardCountTokensAnthropicAPIKeyPassthrough(context.Background(), c, account, body)
		} else {
			err = svc.ForwardCountTokens(context.Background(), c, account, &ParsedRequest{Body: NewRequestBodyRef(body), Model: "claude-sonnet-4-6"})
		}
		require.Error(t, err)
		require.Equal(t, http.StatusBadRequest, recorder.Code)
		require.Contains(t, recorder.Body.String(), "safeguards requires")
		// No upstream service is configured: this must return before any send.
	}
}

func TestProtocolCacheAPIKeyCountBuildersEnforceLimit(t *testing.T) {
	svc := &GatewayService{cfg: &config.Config{}}
	account := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey}
	body := []byte(`{"model":"private-model","tools":[{"name":"t","cache_control":{"type":"ephemeral"}}],"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":[{"type":"text","text":"a","cache_control":{"type":"ephemeral"}},{"type":"text","text":"b","cache_control":{"type":"ephemeral"}},{"type":"text","text":"c","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}`)
	c := protocolFidelityContext("")
	req, err := svc.buildCountTokensRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "key")
	require.NoError(t, err)
	normal, _, err := svc.buildCountTokensRequest(context.Background(), c, account, body, "key", "apikey", "private-model", false)
	require.NoError(t, err)
	for _, request := range []*http.Request{req, normal} {
		wire, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		_, messages, tools, system := collectCacheControlPaths(wire)
		require.Equal(t, 4, len(messages)+len(tools)+len(system))
		require.Empty(t, tools, "private breakpoint priority must be retained")
		require.Equal(t, "1h", gjson.GetBytes(wire, "system.0.cache_control.ttl").String())
		require.Equal(t, "private-model", gjson.GetBytes(wire, "model").String())
	}
}
