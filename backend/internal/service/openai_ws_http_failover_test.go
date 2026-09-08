//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIWSHTTPExecutionPreservesPreOutputFailover(t *testing.T) {
	planGate := `{"detail":"The 'gpt-5.6-sol' model is not supported when using Codex with a ChatGPT account."}`
	for _, test := range []struct {
		name     string
		bridge   bool
		status   int
		body     string
		failover bool
	}{
		{"bridge plan gate", true, 400, planGate, true},
		{"ordinary HTTP plan gate unchanged", false, 400, planGate, false},
		{"bridge transient upstream failure", true, 502, `{"error":{"code":"server_error","message":"upstream unavailable"}}`, true},
		{"bridge malformed request is terminal", true, 400, `{"error":{"code":"invalid_request_error","message":"Invalid schema for response_format"}}`, false},
		{"bridge cyber policy is terminal", true, 502, `{"error":{"code":"cyber_policy","message":"blocked"}}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: test.status,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(test.body)),
			}}
			svc := newOpenAIImageGenerationControlTestService(upstream)
			c, recorder := newOpenAIImageGenerationControlTestContext(true, "codex_cli_rs/0.153.4")
			account := newOpenAIImageGenerationControlTestAccount()
			account.Type = AccountTypeOAuth
			account.Credentials = map[string]any{"access_token": "test-only-oauth-token"}
			account.Extra = map[string]any{"openai_passthrough": true}
			ctx := context.Background()
			if test.bridge {
				ctx = context.WithValue(WithOpenAIWSConnectionScope(ctx, "independent-socket", ""), openAIWSBridgeExecutionKey{}, true)
			}
			c.Request = c.Request.WithContext(ctx)
			_, err := svc.Forward(ctx, c, account, []byte(`{"model":"gpt-5.6-sol","stream":true,"store":false,"instructions":"test","input":"hello"}`))
			require.Error(t, err)
			var failover *UpstreamFailoverError
			if test.failover {
				require.ErrorAs(t, err, &failover)
				require.False(t, c.Writer.Written(), "an account retry must not commit downstream output")
				require.Empty(t, recorder.Body.String())
			} else {
				require.NotErrorAs(t, err, &failover)
				require.True(t, c.Writer.Written(), "a terminal rejection must be delivered without account retries")
			}
		})
	}
}
