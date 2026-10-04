//go:build unit

package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestBuildAntigravityClientErrorBody_ScrubsPoolIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := []byte(`{"error":{"code":403,"message":"Permission denied on resource project projects/123456789 for consumer: projects/123456789; caller pool-sa@my-gcp-proj.iam.gserviceaccount.com","status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","metadata":{"consumer":"projects/123456789","service":"cloudcode-pa.googleapis.com"}}]}}`)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Data(http.StatusForbidden, "application/json", buildAntigravityClientErrorBody(http.StatusForbidden, upstream))

	require.Equal(t, http.StatusForbidden, rec.Code)
	out := rec.Body.String()
	require.NotContains(t, out, "123456789")
	require.NotContains(t, out, "pool-sa@")
	require.NotContains(t, out, "gserviceaccount.com")
	require.NotContains(t, out, "details")

	var parsed struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &parsed))
	require.Equal(t, 403, parsed.Error.Code)
	require.Equal(t, "PERMISSION_DENIED", parsed.Error.Status)
	require.True(t, strings.Contains(parsed.Error.Message, "Permission denied"))
}

func TestBuildAntigravityClientErrorBody_NonJSONBody(t *testing.T) {
	out := string(buildAntigravityClientErrorBody(http.StatusTooManyRequests, []byte("quota exceeded for consumer 987654321 sa@x.iam.gserviceaccount.com")))
	require.NotContains(t, out, "987654321")
	require.NotContains(t, out, "gserviceaccount")
	require.Contains(t, out, `"status":"RESOURCE_EXHAUSTED"`)
	require.Contains(t, out, `"code":429`)
}

func TestAntigravityMappedErrorsScrubPoolIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const upstreamMessage = "Invalid request for projects/private-pool-123; consumer: 987654321; caller pool-sa@private-pool.iam.gserviceaccount.com"

	for _, tc := range []struct {
		name           string
		claude         bool
		upstreamStatus int
		wantStatus     int
		wantType       string
		passthrough    bool
		customMessage  bool
		allowlisted    bool
		disabledRule   bool
	}{
		{name: "chat bad request", upstreamStatus: 400, wantStatus: 400, wantType: "upstream_error"},
		{name: "responses forbidden", upstreamStatus: 403, wantStatus: 403, wantType: "upstream_error"},
		{name: "compat rate limit", upstreamStatus: 429, wantStatus: 429, wantType: "upstream_error"},
		{name: "compat server failure", upstreamStatus: 500, wantStatus: 502, wantType: "upstream_error"},
		{name: "claude bad request", claude: true, upstreamStatus: 400, wantStatus: 400, wantType: "invalid_request_error"},
		{name: "claude unauthorized", claude: true, upstreamStatus: 401, wantStatus: 502, wantType: "authentication_error"},
		{name: "claude forbidden", claude: true, upstreamStatus: 403, wantStatus: 502, wantType: "permission_error"},
		{name: "claude rate limit", claude: true, upstreamStatus: 429, wantStatus: 429, wantType: "rate_limit_error"},
		{name: "claude overloaded", claude: true, upstreamStatus: 529, wantStatus: 503, wantType: "overloaded_error"},
		{name: "claude server failure", claude: true, upstreamStatus: 500, wantStatus: 502, wantType: "upstream_error"},
		{name: "claude passthrough body", claude: true, upstreamStatus: 400, wantStatus: 418, wantType: "upstream_error", passthrough: true},
		{name: "claude custom rule message", claude: true, upstreamStatus: 400, wantStatus: 418, wantType: "upstream_error", passthrough: true, customMessage: true},
		{name: "compat allowlisted message", upstreamStatus: 400, wantStatus: 400, wantType: "upstream_error", allowlisted: true},
		{name: "claude allowlisted message", claude: true, upstreamStatus: 400, wantStatus: 400, wantType: "invalid_request_error", allowlisted: true},
		{name: "claude disabled rule", claude: true, upstreamStatus: 400, wantStatus: 400, wantType: "invalid_request_error", passthrough: true, disabledRule: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			message := upstreamMessage
			if tc.allowlisted {
				message = "prompt is too long: " + message
			}
			body, err := json.Marshal(map[string]any{"error": map[string]any{
				"message": message,
				"details": []any{map[string]any{"project": "private-pool-123"}},
			}})
			require.NoError(t, err)
			if tc.passthrough {
				rule := newNonFailoverPassthroughRule(tc.upstreamStatus, "Invalid request", tc.wantStatus, upstreamMessage)
				rule.PassthroughBody = !tc.customMessage
				rule.Enabled = !tc.disabledRule
				rules := &ErrorPassthroughService{}
				rules.setLocalCache([]*model.ErrorPassthroughRule{rule})
				BindErrorPassthroughService(c, rules)
			}
			svc := &AntigravityGatewayService{}
			account := &Account{ID: 1, Platform: PlatformAntigravity}
			var writeErr error
			if tc.claude {
				writeErr = svc.WriteMappedClaudeError(c, account, tc.upstreamStatus, "request-id", body)
			} else {
				writeErr = svc.writeMappedAntigravityCompatError(c, account, tc.upstreamStatus, "request-id", body)
			}
			require.Error(t, writeErr)
			require.Equal(t, tc.wantStatus, rec.Code)
			require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
			for _, secret := range []string{"private-pool-123", "987654321", "pool-sa@", "gserviceaccount.com"} {
				require.NotContains(t, rec.Body.String(), secret)
				require.NotContains(t, writeErr.Error(), secret)
			}
			var response map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			errorBody, ok := response["error"].(map[string]any)
			require.True(t, ok)
			require.Equal(t, tc.wantType, errorBody["type"])
			require.NotContains(t, errorBody, "details")
			if tc.claude {
				require.Equal(t, "error", response["type"])
			} else {
				require.NotContains(t, response, "type")
				require.Contains(t, errorBody, "param")
				require.Nil(t, errorBody["param"])
				require.Contains(t, errorBody, "code")
				require.Nil(t, errorBody["code"])
			}
			if tc.allowlisted || (tc.passthrough && !tc.disabledRule) {
				require.Contains(t, errorBody["message"], "Invalid request")
				require.Contains(t, errorBody["message"], "projects/***")
			} else {
				wantMessage := "Upstream request failed"
				if tc.claude {
					wantMessage = map[int]string{
						400: "Invalid request",
						401: "Upstream authentication failed",
						403: "Upstream access forbidden",
						429: "Upstream rate limit exceeded",
						529: "Upstream service overloaded",
						500: "Upstream request failed",
					}[tc.upstreamStatus]
				}
				require.Equal(t, wantMessage, errorBody["message"])
			}
		})
	}
}
