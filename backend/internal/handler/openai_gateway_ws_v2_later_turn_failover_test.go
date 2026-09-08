package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIResponsesWebSocket_LaterTurnUsageLimitReplaysOnlyCurrentTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)

	firstAccountPayloads := make(chan []byte, 3)
	secondAccountPayload := make(chan []byte, 1)

	firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()

		for turn := 1; turn <= 3; turn++ {
			readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
			_, payload, readErr := conn.Read(readCtx)
			cancelRead()
			if readErr != nil {
				return
			}
			firstAccountPayloads <- append([]byte(nil), payload...)

			var event string
			switch turn {
			case 1:
				event = `{"type":"response.completed","response":{"id":"resp_later_turn_1","model":"gpt-5.1","output":[{"id":"msg_later_turn_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"first answer"}]}],"usage":{"input_tokens":10,"output_tokens":2}}}`
			case 2:
				event = `{"type":"response.completed","response":{"id":"resp_later_turn_2","model":"gpt-5.1","output":[{"id":"msg_later_turn_2","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"second answer"}]}],"usage":{"input_tokens":12,"output_tokens":2}}}`
			default:
				event = `{"type":"response.failed","response":{"id":"resp_later_turn_failed","status":"failed","error":{"code":"rate_limit_exceeded","type":"usage_limit_reached","message":"The usage limit has been reached"}}}`
			}
			writeCtx, cancelWrite := context.WithTimeout(r.Context(), 3*time.Second)
			writeErr := conn.Write(writeCtx, coderws.MessageText, []byte(event))
			cancelWrite()
			if writeErr != nil {
				return
			}
		}
	}))
	defer firstUpstream.Close()

	secondUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()

		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		_, payload, readErr := conn.Read(readCtx)
		cancelRead()
		if readErr != nil {
			return
		}
		secondAccountPayload <- append([]byte(nil), payload...)

		writeCtx, cancelWrite := context.WithTimeout(r.Context(), 3*time.Second)
		_ = conn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_later_turn_recovered","model":"gpt-5.1","output":[{"id":"msg_later_turn_recovered","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"recovered"}]}],"usage":{"input_tokens":20,"output_tokens":2}}}`))
		cancelWrite()
		_ = conn.Close(coderws.StatusNormalClosure, "done")
	}))
	defer secondUpstream.Close()

	groupID := int64(4212)
	accounts := []service.Account{
		{
			ID:          9922,
			Name:        "openai-ws-later-turn-rate-limited",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeAPIKey,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 1,
			Priority:    1,
			Credentials: map[string]any{"api_key": "sk-first", "base_url": firstUpstream.URL},
			Extra: map[string]any{
				"openai_apikey_responses_websockets_v2_enabled": true,
				"openai_apikey_responses_websockets_v2_mode":    service.OpenAIWSIngressModePassthrough,
			},
		},
		{
			ID:          9923,
			Name:        "openai-ws-later-turn-healthy",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeAPIKey,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 1,
			Priority:    2,
			Credentials: map[string]any{"api_key": "sk-second", "base_url": secondUpstream.URL},
			Extra: map[string]any{
				"openai_apikey_responses_websockets_v2_enabled": true,
				"openai_apikey_responses_websockets_v2_mode":    service.OpenAIWSIngressModePassthrough,
			},
		},
	}

	cfg := &config.Config{}
	cfg.RunMode = config.RunModeSimple
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	cfg.Gateway.MaxAccountSwitches = 3

	accountRepo := &openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}
	rateLimitSvc := service.NewRateLimitService(accountRepo, nil, cfg, nil, nil)
	billingCacheSvc := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	gatewaySvc := service.NewOpenAIGatewayService(
		accountRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg,
		nil,
		nil,
		service.NewBillingService(cfg, nil),
		rateLimitSvc,
		billingCacheSvc,
		nil,
		&service.DeferredService{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	cache := &concurrencyCacheMock{
		acquireUserSlotFn: func(ctx context.Context, userID int64, maxConcurrency int, requestID string) (bool, error) {
			return true, nil
		},
		acquireAccountSlotFn: func(ctx context.Context, accountID int64, maxConcurrency int, requestID string) (bool, error) {
			return true, nil
		},
	}
	h := &OpenAIGatewayHandler{
		gatewayService:      gatewaySvc,
		billingCacheService: billingCacheSvc,
		apiKeyService:       &service.APIKeyService{},
		concurrencyHelper:   NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Second),
		maxAccountSwitches:  3,
	}

	apiKey := &service.APIKey{
		ID:      1812,
		GroupID: &groupID,
		User:    &service.User{ID: 1712, Status: service.StatusActive},
		Group:   &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive},
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.User.ID, Concurrency: 1})
		c.Next()
	})
	router.GET("/openai/v1/responses", h.ResponsesWebSocket)
	handlerServer := httptest.NewServer(router)
	defer handlerServer.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(
		dialCtx,
		"ws"+strings.TrimPrefix(handlerServer.URL, "http")+"/openai/v1/responses",
		&coderws.DialOptions{CompressionMode: coderws.CompressionContextTakeover},
	)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	turnPayloads := [][]byte{
		[]byte(`{"type":"response.create","model":"gpt-5.1","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"turn one"}]}]}`),
		[]byte(`{"type":"response.create","model":"gpt-5.1","previous_response_id":"resp_later_turn_1","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"turn two"}]}]}`),
		[]byte(`{"type":"response.create","model":"gpt-5.1","previous_response_id":"resp_later_turn_2","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"turn three"}]}]}`),
	}
	wantResponseIDs := []string{"resp_later_turn_1", "resp_later_turn_2", "resp_later_turn_recovered"}
	for index, payload := range turnPayloads {
		writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
		err = clientConn.Write(writeCtx, coderws.MessageText, payload)
		cancelWrite()
		require.NoError(t, err)

		readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
		_, event, readErr := clientConn.Read(readCtx)
		cancelRead()
		require.NoError(t, readErr)
		require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
		require.Equal(t, wantResponseIDs[index], gjson.GetBytes(event, "response.id").String())
	}

	for index := 0; index < 3; index++ {
		select {
		case payload := <-firstAccountPayloads:
			require.Equal(t, gjson.GetBytes(turnPayloads[index], "input.0.content.0.text").String(), gjson.GetBytes(payload, "input.0.content.0.text").String())
		case <-time.After(3 * time.Second):
			t.Fatalf("等待第一个上游收到第 %d 个 turn 超时", index+1)
		}
	}

	select {
	case replayPayload := <-secondAccountPayload:
		require.False(t, gjson.GetBytes(replayPayload, "previous_response_id").Exists())
		require.Equal(t, "gpt-5.1", gjson.GetBytes(replayPayload, "model").String())
		input := gjson.GetBytes(replayPayload, "input")
		require.True(t, input.IsArray())
		require.Len(t, input.Array(), 5)
		require.Equal(t, "turn one", input.Get("0.content.0.text").String())
		require.Equal(t, "msg_later_turn_1", input.Get("1.id").String())
		require.Equal(t, "turn two", input.Get("2.content.0.text").String())
		require.Equal(t, "msg_later_turn_2", input.Get("3.id").String())
		require.Equal(t, "turn three", input.Get("4.content.0.text").String())
	case <-time.After(3 * time.Second):
		t.Fatal("等待替换账号收到当前 turn 重放超时")
	}
	require.Equal(t, []int64{int64(9922)}, accountRepo.rateLimitedIDs)
}
