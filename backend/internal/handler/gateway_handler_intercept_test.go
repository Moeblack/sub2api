package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestDetectInterceptType_MaxTokensOneHaikuRequiresClaudeCodeClient(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]}`)

	notClaudeCode := detectInterceptType(body, "claude-haiku-4-5", 1, false)
	require.Equal(t, InterceptTypeNone, notClaudeCode)

	isClaudeCode := detectInterceptType(body, "claude-haiku-4-5", 1, true)
	require.Equal(t, InterceptTypeMaxTokensOneHaiku, isClaudeCode)
}

func TestDetectInterceptType_SuggestionModeRequiresClaudeCodeClient(t *testing.T) {
	body := []byte(`{
		"messages":[{
			"role":"user",
			"content":[{"type":"text","text":"[SUGGESTION MODE:foo]"}]
		}],
		"system":[]
	}`)

	got := detectInterceptType(body, "claude-sonnet-4-5", 256, false)
	require.Equal(t, InterceptTypeNone, got)
	require.Equal(t, InterceptTypeSuggestionMode, detectInterceptType(body, "claude-sonnet-4-5", 256, true))
}

func TestDetectInterceptType_AuxiliaryRequestBoundaries(t *testing.T) {
	message := func(text string) string {
		encoded, err := json.Marshal(text)
		require.NoError(t, err)
		return `{"role":"user","content":[{"type":"text","text":` + string(encoded) + `}]}`
	}
	warmup := message("Warmup")
	title := message("Please write a 5-10 word title for the following conversation:\nhello")
	suggestion := message("[SUGGESTION MODE: suggest the next prompt]")
	for _, tt := range []struct {
		name string
		body string
		want InterceptType
	}{
		{"standalone warmup", `{"messages":[` + warmup + `]}`, InterceptTypeWarmup},
		{"standalone title", `{"messages":[` + title + `]}`, InterceptTypeWarmup},
		{"suggestion", `{"messages":[` + suggestion + `]}`, InterceptTypeSuggestionMode},
		{"ordinary", `{"messages":[` + message("Explain how Warmup works") + `]}`, InterceptTypeNone},
		{"quoted warmup", `{"messages":[` + message("\"Warmup\"") + `]}`, InterceptTypeNone},
		{"quoted title", `{"messages":[` + message("Explain: Please write a 5-10 word title for the following conversation:\nhello") + `]}`, InterceptTypeNone},
		{"historical warmup", `{"messages":[` + warmup + `,{"role":"assistant","content":[{"type":"text","text":"ready"}]},` + message("Continue") + `]}`, InterceptTypeNone},
		{"historical title", `{"messages":[` + title + `,` + message("Answer my real question") + `]}`, InterceptTypeNone},
		{"warmup in conversation", `{"messages":[` + message("Help me") + `,` + warmup + `]}`, InterceptTypeNone},
		{"assistant warmup", `{"messages":[{"role":"assistant","content":[{"type":"text","text":"Warmup"}]}]}`, InterceptTypeNone},
		{"extra instruction block", `{"messages":[{"role":"user","content":[{"type":"text","text":"Warmup"},{"type":"text","text":"Explain a bug"}]}]}`, InterceptTypeNone},
		{"extra image block", `{"messages":[{"role":"user","content":[{"type":"text","text":"[SUGGESTION MODE:x]"},{"type":"image"}]}]}`, InterceptTypeNone},
		{"quoted suggestion", `{"messages":[` + message("Explain [SUGGESTION MODE:x]") + `]}`, InterceptTypeNone},
		{"incomplete suggestion", `{"messages":[` + message("[SUGGESTION MODE:") + `]}`, InterceptTypeNone},
		{"quoted system title", `{"system":[{"text":"Explain Analyze if this message indicates a new conversation topic. If it does, extract a 2-3 word title"}],"messages":[` + message("hello") + `]}`, InterceptTypeNone},
		{"topic extraction", `{"system":[{"text":"Analyze if this message indicates a new conversation topic. If it does, extract a 2-3 word title"}],"messages":[` + message("hello") + `]}`, InterceptTypeWarmup},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, detectInterceptType([]byte(tt.body), "claude-sonnet-4-5", 256, true))
			require.Equal(t, InterceptTypeNone, detectInterceptType([]byte(tt.body), "claude-sonnet-4-5", 256, false))
			require.Equal(t, InterceptTypeNone, detectInterceptType([]byte(tt.body), "claude-sonnet-4-5", 1, true), "non-haiku probes must not be mocked")
		})
	}
}

func TestSendMockInterceptResponse_MaxTokensOneHaiku(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)

	sendMockInterceptResponse(ctx, "claude-haiku-4-5", InterceptTypeMaxTokensOneHaiku)

	require.Equal(t, http.StatusOK, rec.Code)

	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "max_tokens", response["stop_reason"])

	id, ok := response["id"].(string)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(id, "msg_01"))

	content, ok := response["content"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, content)

	firstBlock, ok := content[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "#", firstBlock["text"])

	usage, ok := response["usage"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(1), usage["output_tokens"])
}

func TestHandleClaudeCodeOnlyErrorStreamLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%t", committed), func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			if committed {
				c.Header("Content-Type", "text/event-stream")
				c.Writer.WriteHeaderNow()
			}
			h := &GatewayHandler{}
			require.True(t, h.handleClaudeCodeOnlyError(c, fmt.Errorf("select: %w", service.ErrClaudeCodeOnly), true))
			if committed {
				require.Equal(t, http.StatusOK, rec.Code)
				require.Contains(t, rec.Body.String(), `data: {"type":"error"`)
			} else {
				require.Equal(t, http.StatusForbidden, rec.Code)
				require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
			}
			require.Contains(t, rec.Body.String(), `"type":"permission_error"`)
			require.Contains(t, rec.Body.String(), service.ErrClaudeCodeOnly.Error())
			reason, _ := c.Get(service.OpsClientBusinessLimitedReasonKey)
			require.Equal(t, service.OpsClientBusinessLimitedReasonLocalPolicyDenied, reason)
		})
	}
}

func TestHandleClaudeCodeOnlyErrorIgnoresCapacityErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	require.False(t, (&GatewayHandler{}).handleClaudeCodeOnlyError(c, fmt.Errorf("no available accounts"), false))
	require.False(t, c.Writer.Written())
	require.False(t, service.HasOpsClientBusinessLimited(c))
}
