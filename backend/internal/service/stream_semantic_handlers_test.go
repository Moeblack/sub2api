package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestStreamingHandlers_SemanticHeartbeatTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{Gateway: config.GatewayConfig{
		StreamSemanticIdleTimeout: 1,
		// Transport idle stays independent; pings would keep it alive forever.
		StreamDataIntervalTimeout: 30,
		MaxLineSize:               defaultMaxLineSize,
	}}
	gateway := &GatewayService{cfg: cfg, rateLimitService: &RateLimitService{}}
	openAI := &OpenAIGatewayService{cfg: cfg, toolCorrector: NewCodexToolCorrector()}
	ag := &AntigravityGatewayService{settingService: &SettingService{cfg: cfg}}
	account := &Account{ID: 1}
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		run  func(*http.Response, *gin.Context) error
	}{
		{"Anthropic OAuth", func(resp *http.Response, c *gin.Context) error {
			_, err := gateway.handleStreamingResponse(ctx, resp, c, account, time.Now(), "model", "model", false)
			return err
		}},
		{"Anthropic API key passthrough", func(resp *http.Response, c *gin.Context) error {
			_, err := gateway.handleStreamingResponseAnthropicAPIKeyPassthrough(ctx, resp, c, account, time.Now(), "model")
			return err
		}},
		{"native passthrough", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.handleNativeAnthropicStreamingResponse(ctx, resp, c, account, "model", "model", "model", nil, time.Now())
			return err
		}},
		{"native to CC", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.handleCCStreamingFromNativeAnthropic(resp, c, "model", "model", "model", nil, time.Now(), true)
			return err
		}},
		{"native buffered CC", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.handleCCBufferedFromNativeAnthropic(resp, c, "model", "model", "model", nil, time.Now())
			return err
		}},
		{"native to Responses", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.handleResponsesStreamingFromNativeAnthropic(resp, c, "model", "model", "model", nil, time.Now(), apicompat.ResponsesClientToolMapping{})
			return err
		}},
		{"native buffered Responses", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.handleResponsesBufferedFromNativeAnthropic(resp, c, "model", "model", "model", nil, time.Now(), apicompat.ResponsesClientToolMapping{})
			return err
		}},
		{"OpenAI Responses", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.handleStreamingResponse(ctx, resp, c, account, time.Now(), "model", "model")
			return err
		}},
		{"OpenAI to Anthropic", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.handleAnthropicStreamingResponse(resp, c, account, "model", "model", "model", time.Now())
			return err
		}},
		{"OpenAI to CC", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.handleChatStreamingResponse(resp, c, account, "model", "model", "model", time.Now(), 0)
			return err
		}},
		{"OpenAI buffered", func(resp *http.Response, c *gin.Context) error {
			_, _, _, err := openAI.readOpenAICompatBufferedTerminal(resp, c, "semantic test", "request")
			return err
		}},
		{"OpenAI raw CC", func(resp *http.Response, c *gin.Context) error {
			_, err := openAI.streamRawChatCompletions(c, resp, account, "model", "model", "model", nil, nil, time.Now(), 0)
			return err
		}},
		{"OpenAI CC conversion scanner", func(resp *http.Response, c *gin.Context) error {
			return openAI.scanCCStream(c, resp, "semantic test", "request", time.Now(), func(*apicompat.ChatCompletionsChunk) {}).Err
		}},
		{"antigravity Gemini", func(resp *http.Response, c *gin.Context) error {
			_, err := ag.handleGeminiStreamingResponse(c, resp, time.Now())
			return err
		}},
		{"antigravity Claude", func(resp *http.Response, c *gin.Context) error {
			_, err := ag.handleClaudeStreamingResponse(c, resp, time.Now(), "model")
			return err
		}},
		{"antigravity buffered Gemini", func(resp *http.Response, c *gin.Context) error {
			_, err := ag.handleGeminiStreamToNonStreaming(c, resp, time.Now())
			return err
		}},
		{"antigravity buffered Claude", func(resp *http.Response, c *gin.Context) error {
			_, _, err := ag.collectClaudeStreamResponse(c, resp, time.Now(), "model")
			return err
		}},
		{"antigravity compat scanner", func(resp *http.Response, c *gin.Context) error {
			events, stop, _ := ag.startAntigravityCompatScanner(resp)
			defer stop()
			for event := range events {
				if event.err != nil {
					return event.err
				}
			}
			return nil
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body, writer := newSemanticTrackedPipe(t)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
			beginUpstreamResponseModelObservation(c)
			resp := &http.Response{StatusCode: http.StatusOK, Body: body, Header: http.Header{}, Request: c.Request}
			done := make(chan error, 1)
			started := time.Now()
			go func() { done <- tt.run(resp, c) }()
			writerDone := writeSemanticHeartbeats(writer, ": ping\nevent: ping\ndata: {\"type\":\"ping\"}\n\n")
			require.Error(t, waitSemanticScan(t, done), "heartbeat-only stream must not complete successfully")
			require.GreaterOrEqual(t, time.Since(started), 500*time.Millisecond, "not an early parse failure or empty-delta completion")
			waitSemanticSignal(t, body.closed)
			waitSemanticSignal(t, writerDone)
			require.Zero(t, body.activeReads.Load())
		})
	}
}

func TestAnthropicPassthrough_SemanticTimeoutAfterClientDisconnectKeepsUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body, writer := newSemanticTrackedPipe(t)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	clientCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(clientCtx)
	c.Writer = &failWriteResponseWriter{ResponseWriter: c.Writer}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       body,
		Request:    c.Request.WithContext(context.WithoutCancel(clientCtx)),
	}
	svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{StreamSemanticIdleTimeout: 1}}}
	type outcome struct {
		result *streamingResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := svc.handleStreamingResponseAnthropicAPIKeyPassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "model")
		done <- outcome{result: result, err: err}
	}()
	_, err := io.WriteString(writer, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":8}}}\n\n")
	require.NoError(t, err)
	cancel()
	writerDone := writeSemanticHeartbeats(writer, ": ping\n\n")
	select {
	case outcome := <-done:
		require.ErrorIs(t, outcome.err, errStreamSemanticIdle)
		require.NotNil(t, outcome.result)
		require.True(t, outcome.result.clientDisconnect)
		require.Equal(t, 8, outcome.result.usage.InputTokens)
	case <-time.After(5 * time.Second):
		t.Fatal("detached usage drain remained pinned by heartbeats")
	}
	waitSemanticSignal(t, body.closed)
	waitSemanticSignal(t, writerDone)
	require.Zero(t, body.activeReads.Load())
}

func TestAntigravityUpstream_SemanticTimeoutEmitsErrorAndReleasesReader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body, writer := newSemanticTrackedPipe(t)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	beginUpstreamResponseModelObservation(c)
	svc := &AntigravityGatewayService{settingService: &SettingService{cfg: &config.Config{
		Gateway: config.GatewayConfig{StreamSemanticIdleTimeout: 1},
	}}}
	resp := &http.Response{Body: body, Header: http.Header{}, Request: c.Request}
	done := make(chan *antigravityStreamResult, 1)
	go func() { done <- svc.streamUpstreamResponse(c, resp, time.Now()) }()
	_, err := io.WriteString(writer, "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":8}}}\n\n")
	require.NoError(t, err)
	writerDone := writeSemanticHeartbeats(writer, ": ping\n\n")
	select {
	case result := <-done:
		require.NotNil(t, result)
		require.Equal(t, 8, result.usage.InputTokens)
		require.Contains(t, recorder.Body.String(), "event: error")
		require.Contains(t, recorder.Body.String(), "Upstream semantic idle timeout")
	case <-time.After(5 * time.Second):
		t.Fatal("antigravity passthrough did not release its usage drain")
	}
	waitSemanticSignal(t, body.closed)
	waitSemanticSignal(t, writerDone)
	require.Zero(t, body.activeReads.Load())
}
