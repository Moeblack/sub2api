//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type gatewayModelErrorRepo struct {
	modelNotFoundAccountRepoStub
	errorCalls int
}

func (r *gatewayModelErrorRepo) SetError(context.Context, int64, string) error {
	r.errorCalls++
	return nil
}

func gatewayModelErrorAccount(platform string, status int) *Account {
	account := openAIModelNotFoundTempAccount()
	account.Platform = platform
	account.Concurrency = 1
	account.Credentials["api_key"] = "test-key"
	account.Credentials["temp_unschedulable_rules"] = []any{map[string]any{
		"error_code": float64(status), "keywords": []any{"blocked"}, "duration_minutes": float64(10),
	}}
	return account
}

func gatewayModelErrorResponse(status int) *http.Response {
	if status == http.StatusNotFound {
		return &http.Response{
			StatusCode: status,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"model_not_found","message":"model not found"}}`)),
		}
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"blocked"}}`)),
	}
}

func gatewayModelErrorContext(body []byte, path string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	return c
}

func TestGatewayCountTokensErrorsUseActualModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
	for _, passthrough := range []bool{false, true} {
		name := "normal"
		if passthrough {
			name = "passthrough"
		}
		t.Run(name, func(t *testing.T) {
			for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusUnauthorized} {
				repo := &gatewayModelErrorRepo{}
				upstream := &anthropicHTTPUpstreamRecorder{resp: gatewayModelErrorResponse(status)}
				svc := &GatewayService{cfg: cfg, httpUpstream: upstream, rateLimitService: &RateLimitService{accountRepo: repo, cfg: cfg}, responseHeaderFilter: compileResponseHeaderFilter(cfg)}
				account := gatewayModelErrorAccount(PlatformAnthropic, status)
				account.Credentials["base_url"] = "https://api.anthropic.com"
				account.Credentials["model_mapping"] = map[string]any{"public-model": "private-upstream-model"}
				if passthrough {
					account.Extra = map[string]any{"anthropic_passthrough": true}
				}
				body := []byte(`{"model":"public-model","messages":[{"role":"user","content":"hello"}]}`)
				parsed := &ParsedRequest{Model: "public-model", Body: NewRequestBodyRef(body)}
				c := gatewayModelErrorContext(body, "/v1/messages/count_tokens")
				require.Error(t, svc.ForwardCountTokens(context.Background(), c, account, parsed))
				require.NotNil(t, upstream.lastReq)
				require.Equal(t, "private-upstream-model", gjson.GetBytes(upstream.lastBody, "model").String())
				require.Zero(t, repo.tempCalls)
				if status == http.StatusUnauthorized {
					// Private 401 handling remains account-scoped; no new scope schema.
					require.Equal(t, 1, repo.errorCalls)
					require.Empty(t, repo.modelRateLimitCalls)
				} else {
					require.Zero(t, repo.errorCalls)
					require.Len(t, repo.modelRateLimitCalls, 1)
					require.Equal(t, "private-upstream-model", repo.modelRateLimitCalls[0].scope)
				}
			}
		})
	}
}

func TestOpenAICountTokensErrorsUseActualModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
	for _, messages := range []bool{false, true} {
		repo := &gatewayModelErrorRepo{}
		upstream := &httpUpstreamRecorder{resp: gatewayModelErrorResponse(http.StatusForbidden)}
		svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream, rateLimitService: &RateLimitService{accountRepo: repo, cfg: cfg}}
		account := gatewayModelErrorAccount(PlatformOpenAI, http.StatusForbidden)
		account.Credentials["model_mapping"] = map[string]any{"public-model": "private-upstream-model"}
		var err error
		if messages {
			body := []byte(`{"model":"public-model","messages":[{"role":"user","content":"hello"}]}`)
			err = svc.ForwardCountTokensAsAnthropic(context.Background(), gatewayModelErrorContext(body, "/v1/messages/count_tokens"), account, body, "private-upstream-model")
		} else {
			body := []byte(`{"model":"public-model","input":"hello"}`)
			err = svc.ForwardResponsesInputTokens(context.Background(), gatewayModelErrorContext(body, "/v1/responses/input_tokens"), account, body)
		}
		require.Error(t, err)
		require.Equal(t, "private-upstream-model", gjson.GetBytes(upstream.lastBody, "model").String())
		require.Zero(t, repo.errorCalls)
		require.Zero(t, repo.tempCalls)
		require.Len(t, repo.modelRateLimitCalls, 1)
		require.Equal(t, "private-upstream-model", repo.modelRateLimitCalls[0].scope)
	}
}

func TestBedrockErrorsUseSchedulerModelNotProviderID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound} {
		repo := &gatewayModelErrorRepo{}
		upstream := &anthropicHTTPUpstreamRecorder{resp: gatewayModelErrorResponse(status)}
		svc := &GatewayService{cfg: cfg, httpUpstream: upstream, rateLimitService: &RateLimitService{accountRepo: repo, cfg: cfg}}
		account := gatewayModelErrorAccount(PlatformAnthropic, status)
		account.Type = AccountTypeBedrock
		account.Credentials["auth_mode"] = "apikey"
		body := []byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hello"}]}`)
		parsed := &ParsedRequest{Model: "claude-sonnet-4-5", Body: NewRequestBodyRef(body)}
		_, err := svc.forwardBedrock(context.Background(), gatewayModelErrorContext(body, "/v1/messages"), account, parsed, time.Now())
		require.Error(t, err)
		require.NotNil(t, upstream.lastReq)
		require.Contains(t, upstream.lastReq.URL.Path, "anthropic.claude-sonnet-4-5-")
		require.Zero(t, repo.errorCalls)
		require.Zero(t, repo.tempCalls)
		require.Len(t, repo.modelRateLimitCalls, 1)
		call := repo.modelRateLimitCalls[0]
		require.Equal(t, "claude-sonnet-4-5", call.scope)
		account.Extra = map[string]any{modelRateLimitsKey: map[string]any{
			call.scope: map[string]any{"rate_limit_reset_at": call.resetAt.UTC().Format(time.RFC3339)},
		}}
		require.False(t, account.IsSchedulableForModelWithContext(context.Background(), "claude-sonnet-4-5"))
		require.True(t, account.IsSchedulableForModelWithContext(context.Background(), "claude-opus-4-6"))
	}
}

func TestOAuthRetryExhaustedKeepsNon401CooldownModelScoped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, renderError := range []bool{false, true} {
		repo := &gatewayModelErrorRepo{}
		cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
		svc := &GatewayService{cfg: cfg, rateLimitService: &RateLimitService{accountRepo: repo, cfg: cfg}}
		account := gatewayModelErrorAccount(PlatformAnthropic, http.StatusForbidden)
		account.Type = AccountTypeOAuth
		resp := gatewayModelErrorResponse(http.StatusForbidden)
		if renderError {
			_, err := svc.handleRetryExhaustedError(context.Background(), resp, gatewayModelErrorContext(nil, "/v1/messages"), account, "private-upstream-model")
			require.Error(t, err)
		} else {
			svc.handleRetryExhaustedSideEffects(context.Background(), resp, account, "private-upstream-model")
		}
		require.Zero(t, repo.errorCalls)
		require.Zero(t, repo.tempCalls)
		require.Len(t, repo.modelRateLimitCalls, 1)
		require.Equal(t, "private-upstream-model", repo.modelRateLimitCalls[0].scope)
	}
}
