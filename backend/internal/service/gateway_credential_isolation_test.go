//go:build unit

package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestAnthropicMessagesCredentialAllowed(t *testing.T) {
	for _, tc := range []struct {
		platform string
		typ      string
		allowed  bool
	}{
		{PlatformAnthropic, AccountTypeOAuth, true},
		{PlatformAnthropic, AccountTypeSetupToken, true},
		{PlatformAnthropic, AccountTypeServiceAccount, true},
		{PlatformAnthropic, AccountTypeBedrock, true},
		{PlatformOpenAI, AccountTypeAPIKey, true},
		{PlatformDeepseek, AccountTypeAPIKey, true},
		{PlatformOpenAI, AccountTypeOAuth, false},
		{PlatformOpenAI, AccountTypeSetupToken, false},
		{PlatformGrok, AccountTypeOAuth, false},
		{PlatformGemini, AccountTypeOAuth, false},
		{PlatformAntigravity, AccountTypeOAuth, false},
		{"unknown-provider", AccountTypeOAuth, false},
	} {
		t.Run(tc.platform+"/"+tc.typ, func(t *testing.T) {
			require.Equal(t, tc.allowed, anthropicMessagesCredentialAllowed(&Account{Platform: tc.platform, Type: tc.typ}))
		})
	}
}

func TestGatewayForeignOAuthRejectedBeforeTokenOrUpstreamSideEffects(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, platform := range []string{PlatformOpenAI, PlatformGrok, PlatformGemini, PlatformAntigravity} {
		for _, typ := range []string{AccountTypeOAuth, AccountTypeSetupToken} {
			t.Run(platform+"/"+typ, func(t *testing.T) {
				account := &Account{ID: 25, Platform: platform, Type: typ, Status: StatusActive, Schedulable: true}
				body := []byte(`{"model":"gpt-custom-private","messages":[]}`)
				parsed := &ParsedRequest{Model: "gpt-custom-private", Body: NewRequestBodyRef(body)}
				// Nil token, HTTP and rate-limit services deliberately make any
				// attempted outbound call or account penalty fail this test.
				svc := &GatewayService{}
				assertLocalFailure := func(err error) {
					t.Helper()
					var failover *UpstreamFailoverError
					require.ErrorAs(t, err, &failover)
					require.True(t, failover.ShouldRetryNextAccount())
					require.False(t, failover.RetryableOnSameAccount)
					require.False(t, failover.IsCredentialFailure())
					require.Equal(t, anthropicMessagesForeignOAuthReason, failover.Reason)
				}
				req, _, err := svc.buildUpstreamRequest(context.Background(), nil, account, body, "foreign-token", "oauth", parsed.Model, false, false)
				require.Nil(t, req)
				assertLocalFailure(err)
				req, _, err = svc.buildCountTokensRequest(context.Background(), nil, account, body, "foreign-token", "oauth", parsed.Model, false)
				require.Nil(t, req)
				assertLocalFailure(err)
				result, err := svc.Forward(context.Background(), nil, account, parsed)
				require.Nil(t, result)
				assertLocalFailure(err)

				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", nil)
				require.NoError(t, svc.ForwardCountTokens(context.Background(), c, account, parsed))
				require.Equal(t, http.StatusNotFound, rec.Code)
				require.Equal(t, StatusActive, account.Status)
				require.True(t, account.Schedulable)
				require.Equal(t, string(body), string(parsed.Body.Bytes()))
			})
		}
	}
}

func TestGatewayAnthropicProtocolBaseURLAndAuthStayAligned(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &GatewayService{cfg: &config.Config{}}
	for _, tc := range []struct {
		name     string
		platform string
		base     string
		protocol string
		want     string
		bearer   bool
	}{
		{"adaptive override", PlatformDeepseek, "https://api.deepseek.com", "https://anthropic.example.com", "https://anthropic.example.com", false},
		{"provider fallback", PlatformDeepseek, "https://api.deepseek.com", "", "https://api.deepseek.com/anthropic", false},
		{"ordinary anthropic", PlatformAnthropic, "https://custom-anthropic.example.com", "", "https://custom-anthropic.example.com", false},
		{"selected ollama", PlatformDeepseek, "https://api.deepseek.com", "https://ollama.com", "https://ollama.com", true},
		{"non-ollama override", PlatformDeepseek, "https://ollama.com", "https://anthropic.example.com", "https://anthropic.example.com", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := &Account{ID: 8079, Platform: tc.platform, Type: AccountTypeAPIKey, Credentials: map[string]any{
				"api_key": "test-key", "base_url": tc.base,
			}}
			if tc.platform == PlatformDeepseek {
				account.Credentials["api_protocol"] = APIProtocolAdaptive
				account.Credentials["api_base_urls"] = map[string]any{
					APIProtocolAnthropic: tc.protocol, APIProtocolChatCompletions: "https://chat.example.com",
				}
			}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			body := []byte(`{"model":"deepseek-v4-flash","max_tokens":256000,"messages":[{"role":"user","content":"hi"}]}`)
			assertRequest := func(req *http.Request, endpoint string, wire []byte) {
				t.Helper()
				require.Equal(t, tc.want+endpoint+"?beta=true", req.URL.String())
				if tc.bearer {
					require.Equal(t, "Bearer test-key", getHeaderRaw(req.Header, "authorization"))
					require.Empty(t, getHeaderRaw(req.Header, "x-api-key"))
				} else {
					require.Equal(t, "test-key", getHeaderRaw(req.Header, "x-api-key"))
					require.Empty(t, getHeaderRaw(req.Header, "authorization"))
				}
				if wire != nil {
					wantTokens := int64(256000)
					if tc.bearer {
						wantTokens = 65535
					}
					require.Equal(t, wantTokens, gjson.GetBytes(wire, "max_tokens").Int())
				}
			}
			req, wire, err := svc.buildUpstreamRequest(context.Background(), c, account, body, "test-key", "apikey", "deepseek-v4-flash", false, false)
			require.NoError(t, err)
			assertRequest(req, "/v1/messages", wire)
			req, wire, err = svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "test-key")
			require.NoError(t, err)
			assertRequest(req, "/v1/messages", wire)
			req, _, err = svc.buildCountTokensRequest(context.Background(), c, account, body, "test-key", "apikey", "deepseek-v4-flash", false)
			require.NoError(t, err)
			assertRequest(req, "/v1/messages/count_tokens", nil)
			req, err = svc.buildCountTokensRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, body, "test-key")
			require.NoError(t, err)
			assertRequest(req, "/v1/messages/count_tokens", nil)
		})
	}
}

func TestAnthropicCredentialGuardDoesNotBlockNativeOpenAIOAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, typ := range []string{AccountTypeOAuth, AccountTypeSetupToken} {
		account := &Account{ID: 25, Platform: PlatformOpenAI, Type: typ, Credentials: map[string]any{"chatgpt_account_id": "native-account"}}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		body := []byte(`{"model":"gpt-custom-private","input":"hello"}`)
		req, err := (&OpenAIGatewayService{cfg: &config.Config{}}).buildUpstreamRequest(context.Background(), c, account, body, "openai-token", false, "", false)
		require.NoError(t, err)
		require.Equal(t, "chatgpt.com", req.URL.Host)
		require.Equal(t, "Bearer openai-token", req.Header.Get("Authorization"))
	}
}
