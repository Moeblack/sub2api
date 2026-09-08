package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestBuildOpenAIWSCurrentTurnRetryPayloadRejectsOrphanToolOutput(t *testing.T) {
	payload := []byte(`{"type":"response.create","model":"mapped-model","previous_response_id":"resp_old"}`)
	fullInput := []json.RawMessage{
		json.RawMessage(`{"type":"function_call_output","call_id":"missing_call","output":"done"}`),
	}

	retryPayload, retrySafe, err := buildOpenAIWSCurrentTurnRetryPayload(payload, fullInput, true, "gpt-5.6-sol")

	require.NoError(t, err)
	require.False(t, retrySafe)
	require.Nil(t, retryPayload)
}

func TestProxyOpenAIWSHTTPBridgeTurnLaterTurn429FailsOverBeforeClientWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": []string{"60"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached"}}`)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	account := &Account{ID: 129, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	payload := []byte(`{"type":"response.create","model":"gpt-5.6-sol","previous_response_id":"resp_old","input":[{"role":"user","content":"continue"}]}`)
	writes := 0

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(
		context.Background(), c, account, "access-token", payload, len(payload),
		"gpt-5.6-sol", "", "", "", "", 281,
		func([]byte) error {
			writes++
			return nil
		},
	)

	require.Nil(t, result)
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, http.StatusTooManyRequests, failoverErr.StatusCode)
	require.Zero(t, writes)
}

func TestProxyOpenAIWSHTTPBridgeTurnLaterTurnDoesNotFailOverAfterDownstreamOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(
			"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n" +
				"data: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"limited\"}}\n\n",
		)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	account := &Account{ID: 10, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	payload := []byte(`{"type":"response.create","model":"gpt-5","input":"hi"}`)
	var writes [][]byte

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(
		context.Background(), c, account, "sk-test", payload, len(payload),
		"gpt-5", "", "", "", "", 281,
		func(message []byte) error {
			writes = append(writes, append([]byte(nil), message...))
			return nil
		},
	)

	require.NotNil(t, result)
	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.Len(t, writes, 2)
	require.Equal(t, "response.output_text.delta", gjson.GetBytes(writes[0], "type").String())
	// P0.1: after downstream output, retain the upstream error and synthesize
	// the official Responses terminal so Codex clients do not see a bare error
	// followed by a closed stream.
	require.Equal(t, "response.failed", gjson.GetBytes(writes[1], "type").String())
}

func TestOpenAIWSHTTPBridgeLaterTurn429RetriesCurrentTurnOnReplacementAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3

	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type":          []string{"text/event-stream"},
				openAIWSTurnStateHeader: []string{"old-account-state"},
			},
			Body: io.NopCloser(strings.NewReader(
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_first\",\"output\":[{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"first-ok\"}]},{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"inspect\",\"arguments\":\"{}\"}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
			)),
		},
		{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": []string{"60"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached"}}`)),
		},
		{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: io.NopCloser(strings.NewReader(
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_second\",\"output\":[{\"id\":\"msg_2\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"second-ok\"}]}],\"usage\":{\"input_tokens\":4,\"output_tokens\":1}}}\n\n",
			)),
		},
	}}
	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     upstream,
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
	}
	account := &Account{
		ID: 129, Name: "limited", Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Extra:       map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeHTTPBridge},
		Credentials: map[string]any{"chatgpt_account_id": "account-a", "chatgpt_user_id": "user-a"},
	}
	nextAccount := *account
	nextAccount.ID = 130
	nextAccount.Name = "replacement"
	nextAccount.Credentials = map[string]any{"chatgpt_account_id": "account-b", "chatgpt_user_id": "user-b"}

	serverErrCh := make(chan error, 1)
	failoverCh := make(chan []byte, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		ginCtx.Request = r.Clone(r.Context())
		readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		_, firstMessage, readErr := conn.Read(readCtx)
		cancel()
		if readErr != nil {
			serverErrCh <- readErr
			return
		}
		proxyErr := svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "access-token-a", firstMessage, nil)
		var failoverErr *UpstreamFailoverError
		if !errors.As(proxyErr, &failoverErr) {
			serverErrCh <- proxyErr
			return
		}
		retryPayload, retryCurrentTurn := OpenAIWSCurrentTurnRetryPayload(proxyErr)
		if !retryCurrentTurn || len(retryPayload) == 0 {
			serverErrCh <- errors.New("missing current-turn retry payload")
			return
		}
		failoverCh <- retryPayload
		serverErrCh <- svc.ProxyResponsesWebSocketFromClient(
			r.Context(), ginCtx, conn, &nextAccount, "access-token-b", retryPayload, nil,
		)
	}))
	defer wsServer.Close()

	dialCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := websocket.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	cancel()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-5.6-sol","input":[{"role":"user","content":"first"}]}`))
	cancel()
	require.NoError(t, err)

	readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, completed, err := clientConn.Read(readCtx)
	cancel()
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(completed, "type").String())

	writeCtx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-5.6-sol","previous_response_id":"resp_first","prompt_cache_key":"client-session","client_metadata":{"session_id":"client-session","thread_id":"client-thread"},"input":[{"type":"function_call_output","call_id":"call_1","output":"second"}]}`))
	cancel()
	require.NoError(t, err)

	readCtx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	_, retriedCompleted, err := clientConn.Read(readCtx)
	cancel()
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(retriedCompleted, "type").String())
	require.Equal(t, "resp_second", gjson.GetBytes(retriedCompleted, "response.id").String())
	_ = clientConn.Close(websocket.StatusNormalClosure, "done")

	select {
	case retryPayload := <-failoverCh:
		require.NotEmpty(t, retryPayload)
		require.False(t, gjson.GetBytes(retryPayload, "previous_response_id").Exists())
		require.Equal(t, "gpt-5.6-sol", gjson.GetBytes(retryPayload, "model").String())
		input := gjson.GetBytes(retryPayload, "input")
		require.True(t, input.IsArray())
		require.Len(t, input.Array(), 4)
		require.Contains(t, input.Raw, "first")
		require.Contains(t, input.Raw, "first-ok")
		require.Contains(t, input.Raw, "second")
		require.Equal(t, 1, strings.Count(input.Raw, `"id":"fc_1"`))
		require.Equal(t, 2, strings.Count(input.Raw, `"call_id":"call_1"`))
		require.Equal(t, "client-session", gjson.GetBytes(retryPayload, "client_metadata.session_id").String())
		require.Equal(t, "client-thread", gjson.GetBytes(retryPayload, "client_metadata.thread_id").String())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for current-turn failover")
	}

	select {
	case proxyErr := <-serverErrCh:
		require.NoError(t, proxyErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for replacement-account completion")
	}
	require.Len(t, upstream.bodies, 3)
	require.Contains(t, string(upstream.bodies[0]), "first")
	require.Equal(t, scopeCodexAccountIdentityValue(account, 0, "session", "client-session"), gjson.GetBytes(upstream.bodies[1], "client_metadata.session_id").String())
	require.Equal(t, scopeCodexAccountIdentityValue(account, 0, "thread", "client-thread"), gjson.GetBytes(upstream.bodies[1], "client_metadata.thread_id").String())
	require.NotContains(t, string(upstream.bodies[2]), "previous_response_id")
	require.Contains(t, string(upstream.bodies[2]), "second")
	require.Equal(t, scopeCodexAccountIdentityValue(&nextAccount, 0, "session", "client-session"), gjson.GetBytes(upstream.bodies[2], "client_metadata.session_id").String())
	require.Equal(t, scopeCodexAccountIdentityValue(&nextAccount, 0, "thread", "client-thread"), gjson.GetBytes(upstream.bodies[2], "client_metadata.thread_id").String())
	require.Empty(t, upstream.requests[2].Header.Get(openAIWSTurnStateHeader))
}

func TestOpenAIWSHTTPBridgeIncrementalTurnsPreserveCompleteOutputHistory(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, delivery := range []struct {
		name           string
		itemEvents     bool
		terminalOutput bool
	}{
		{name: "output_item_done_with_empty_terminal", itemEvents: true},
		{name: "terminal_output_only", terminalOutput: true},
		{name: "item_events_and_terminal_are_not_duplicated", itemEvents: true, terminalOutput: true},
	} {
		t.Run(delivery.name, func(t *testing.T) {
			firstOutput := []json.RawMessage{
				json.RawMessage(`{"id":"rs_history","type":"reasoning","encrypted_content":"opaque-reasoning","summary":[]}`),
				json.RawMessage(`{"id":"msg_history","type":"message","role":"assistant","phase":"commentary","status":"completed","content":[{"type":"output_text","text":"I have selected the plan; only inspection remains."}]}`),
				json.RawMessage(`{"id":"fc_history","type":"function_call","call_id":"call_history","name":"inspect","arguments":"{}","status":"completed"}`),
			}
			secondOutput := []json.RawMessage{
				json.RawMessage(`{"id":"msg_finished","type":"message","role":"assistant","phase":"final_answer","status":"completed","content":[{"type":"output_text","text":"Inspection is complete; the plan is unchanged."}]}`),
			}
			makeResponse := func(id string, output []json.RawMessage) *http.Response {
				var stream strings.Builder
				writeEvent := func(event any) {
					encoded, err := json.Marshal(event)
					require.NoError(t, err)
					stream.WriteString("data: ")
					stream.Write(encoded)
					stream.WriteString("\n\n")
				}
				if delivery.itemEvents {
					for index, item := range output {
						writeEvent(map[string]any{"type": "response.output_item.done", "output_index": index, "item": item})
					}
				}
				terminalOutput := []json.RawMessage{}
				if delivery.terminalOutput {
					terminalOutput = output
				}
				writeEvent(map[string]any{
					"type": "response.completed",
					"response": map[string]any{
						"id": id, "status": "completed", "output": terminalOutput,
						"usage": map[string]int{"input_tokens": 1, "output_tokens": 1},
					},
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(stream.String())),
				}
			}
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				makeResponse("resp_history_first", firstOutput),
				makeResponse("resp_history_second", secondOutput),
				makeResponse("resp_history_third", nil),
				makeResponse("resp_history_reset", nil),
			}}
			cfg := &config.Config{}
			cfg.Security.URLAllowlist.Enabled = false
			cfg.Gateway.OpenAIWS.Enabled = true
			cfg.Gateway.OpenAIWS.OAuthEnabled = true
			cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
			cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
			svc := &OpenAIGatewayService{
				cfg: cfg, httpUpstream: upstream, cache: &stubGatewayCache{},
				openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
				toolCorrector:    NewCodexToolCorrector(),
			}
			account := &Account{
				ID: 129, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Status: StatusActive, Schedulable: true, Concurrency: 1,
				Extra: map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeHTTPBridge},
			}
			serverErrCh := make(chan error, 1)
			wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					serverErrCh <- err
					return
				}
				defer func() { _ = conn.CloseNow() }()
				ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
				ginCtx.Request = r.Clone(r.Context())
				readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				_, firstMessage, err := conn.Read(readCtx)
				cancel()
				if err != nil {
					serverErrCh <- err
					return
				}
				serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "access-token", firstMessage, nil)
			}))
			defer wsServer.Close()
			dialCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			client, _, err := websocket.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
			cancel()
			require.NoError(t, err)
			defer func() { _ = client.CloseNow() }()

			seed := `{"type":"message","role":"user","content":"Choose a plan and inspect it."}`
			toolOutput := `{"type":"function_call_output","call_id":"call_history","output":"inspection passed"}`
			followup := `{"type":"message","role":"user","content":"Recall the plan you already selected."}`
			reset := `{"type":"message","role":"user","content":"Start a separate task."}`
			for _, payload := range []string{
				`{"type":"response.create","model":"gpt-5.6-sol","input":[` + seed + `]}`,
				`{"type":"response.create","model":"gpt-5.6-sol","previous_response_id":"resp_history_first","input":[` + toolOutput + `]}`,
				`{"type":"response.create","model":"gpt-5.6-sol","previous_response_id":"resp_history_second","input":[` + followup + `]}`,
				`{"type":"response.create","model":"gpt-5.6-sol","input":[` + reset + `]}`,
			} {
				writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				err := client.Write(writeCtx, websocket.MessageText, []byte(payload))
				cancel()
				require.NoError(t, err)
				for {
					readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, event, err := client.Read(readCtx)
					cancel()
					require.NoError(t, err)
					eventType := gjson.GetBytes(event, "type").String()
					require.NotEqual(t, "response.failed", eventType)
					require.NotEqual(t, "error", eventType)
					if eventType == "response.completed" {
						break
					}
				}
			}
			_ = client.Close(websocket.StatusNormalClosure, "done")
			select {
			case err := <-serverErrCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for bridge shutdown")
			}

			require.Len(t, upstream.bodies, 4)
			expectedSecond := []json.RawMessage{json.RawMessage(seed)}
			expectedSecond = append(expectedSecond, firstOutput...)
			expectedSecond = append(expectedSecond, json.RawMessage(toolOutput))
			expectedThird := append(append([]json.RawMessage(nil), expectedSecond...), secondOutput...)
			expectedThird = append(expectedThird, json.RawMessage(followup))
			for i, expected := range [][]json.RawMessage{
				{json.RawMessage(seed)},
				expectedSecond,
				expectedThird,
				{json.RawMessage(reset)},
			} {
				expectedJSON, err := json.Marshal(expected)
				require.NoError(t, err)
				require.JSONEq(t, string(expectedJSON), gjson.GetBytes(upstream.bodies[i], "input").Raw, "HTTP input for turn %d", i+1)
				require.False(t, gjson.GetBytes(upstream.bodies[i], "previous_response_id").Exists())
			}
		})
	}
}

func TestProxyOpenAIWSHTTPBridgeTurnPlanGatedModelFailsOverWithoutClientError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	planError := `{"detail":"The 'gpt-6-astra' model is not supported when using Codex with a ChatGPT account."}`
	for _, tt := range []struct {
		name         string
		accountType  string
		turn         int
		responseBody string
		wantFailover bool
	}{
		{"first OAuth turn", AccountTypeOAuth, 1, planError, true},
		{"later OAuth turn", AccountTypeOAuth, 2, planError, true},
		{"API key is not a ChatGPT plan", AccountTypeAPIKey, 1, planError, false},
		{"unrelated bad request", AccountTypeOAuth, 1, `{"error":{"message":"Invalid input"}}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := &tempUnschedulableOpenAIAccountRepo{}
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusBadRequest,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(tt.responseBody)),
			}}
			svc := &OpenAIGatewayService{
				cfg: &config.Config{}, httpUpstream: upstream,
				rateLimitService: &RateLimitService{accountRepo: repo},
			}
			account := &Account{ID: 22, Platform: PlatformOpenAI, Type: tt.accountType, Concurrency: 1}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
			payload := []byte(`{"type":"response.create","model":"gpt-6-astra","input":"continue"}`)
			var writes [][]byte
			result, err := svc.proxyOpenAIWSHTTPBridgeTurn(
				context.Background(), c, account, "access-token", payload, len(payload),
				"gpt-6-astra", "", "", "", "", tt.turn,
				func(event []byte) error {
					writes = append(writes, append([]byte(nil), event...))
					return nil
				},
			)
			require.Nil(t, result)
			require.Error(t, err)
			var failoverErr *UpstreamFailoverError
			if tt.wantFailover {
				require.ErrorAs(t, err, &failoverErr)
				require.Equal(t, http.StatusBadRequest, failoverErr.StatusCode)
				require.False(t, failoverErr.RetryableOnSameAccount)
				require.Empty(t, writes, "the replacement account must answer before any client-visible error")
				require.Equal(t, account.ID, repo.modelRateLimitAccountID)
				require.Equal(t, "gpt-6-astra", repo.modelRateLimitKey)
			} else {
				require.False(t, errors.As(err, &failoverErr))
				require.Len(t, writes, 1)
				require.Zero(t, repo.modelRateLimitAccountID)
				require.Empty(t, repo.modelRateLimitKey)
			}
		})
	}
}
