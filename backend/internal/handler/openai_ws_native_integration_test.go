package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

func TestOpenAIWSNativeHandlerMultiplexSteeringAndExactCallID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const inject = `{"type":"response.inject","response_id":"resp_native_a","input":[{"type":"function_call_output","call_id":"call_exact_Mixed-CASE_001","output":"tool-result"}]}`
	upstreamFrames := make(chan string, 8)
	upstreamDone := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := coderws.Accept(w, req, nil)
		if err != nil {
			upstreamDone <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		ctx, cancel := context.WithTimeout(req.Context(), 8*time.Second)
		defer cancel()
		for i := 0; i < 5; i++ {
			_, frame, readErr := conn.Read(ctx)
			if readErr != nil {
				upstreamDone <- readErr
				return
			}
			upstreamFrames <- string(frame)
			var events []string
			switch gjson.GetBytes(frame, "type").String() {
			case "response.create":
				lane := gjson.GetBytes(frame, "stream_id").String()
				events = []string{fmt.Sprintf(`{"type":"response.created","stream_id":%q,"response":{"id":"resp_native_%s","model":"gpt-5.4"}}`, lane, lane)}
				if lane == "warmup" {
					events = append(events, `{"type":"response.completed","stream_id":"warmup","response":{"id":"resp_native_warmup","model":"gpt-5.4","output":[],"usage":{"input_tokens":0,"output_tokens":0}}}`)
				}
			case "response.inject":
				events = []string{`{"type":"response.injected","stream_id":"a","response_id":"resp_native_a"}`}
			case "response.steer":
				events = []string{
					`{"type":"response.steer.accepted","stream_id":"a","steer":{"id":"steer_exact_1","previous_response_id":"resp_native_a","input":"Continue carefully"}}`,
					`{"type":"response.completed","stream_id":"b","response":{"id":"resp_native_b","model":"gpt-5.4","usage":{"input_tokens":2,"output_tokens":1}}}`,
					`{"type":"response.incomplete","stream_id":"a","response":{"id":"resp_native_a","model":"gpt-5.4","incomplete_details":{"reason":"steered"},"usage":{"input_tokens":3,"output_tokens":2}}}`,
					`{"type":"response.created","stream_id":"a","response":{"id":"resp_native_a2","model":"gpt-5.4"}}`,
					`{"type":"response.completed","stream_id":"a","response":{"id":"resp_native_a2","model":"gpt-5.4","usage":{"input_tokens":4,"output_tokens":3}}}`,
				}
			default:
				upstreamDone <- fmt.Errorf("unexpected frame %s", frame)
				return
			}
			for _, event := range events {
				if err := conn.Write(ctx, coderws.MessageText, []byte(event)); err != nil {
					upstreamDone <- err
					return
				}
			}
		}
		upstreamDone <- nil
		// The client closes after verifying terminal events and accounting.
		_, _, _ = conn.Read(ctx)
	}))
	t.Cleanup(upstream.Close)

	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 8
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	accountRepo := &openAIWSUsageHandlerAccountRepoStub{account: service.Account{ID: 9501, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 4,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": upstream.URL},
		Extra:       map[string]any{"openai_apikey_responses_websockets_v2_enabled": true, "openai_apikey_responses_websockets_v2_mode": service.OpenAIWSIngressModePassthrough}}}
	usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 8)}
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	var userAcquires, accountAcquires atomic.Int32
	cache := &concurrencyCacheMock{
		acquireUserSlotFn:    func(context.Context, int64, int, string) (bool, error) { userAcquires.Add(1); return true, nil },
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { accountAcquires.Add(1); return true, nil },
	}
	groupID := int64(9502)
	perRequestPrice := 0.25
	channelSvc := service.NewChannelService(&openAIWSUsageHandlerChannelRepoStub{
		channels: []service.Channel{{ID: 9505, Name: "native-per-request", Status: service.StatusActive, GroupIDs: []int64{groupID},
			ModelPricing: []service.ChannelModelPricing{{Platform: service.PlatformOpenAI, Models: []string{"gpt-5.4"}, BillingMode: service.BillingModePerRequest, PerRequestPrice: &perRequestPrice}}}},
		groupPlatforms: map[int64]string{groupID: service.PlatformOpenAI},
	}, nil, nil, nil, nil)
	concurrency := service.NewConcurrencyService(cache)
	billingService := service.NewBillingService(cfg, nil)
	gateway := service.NewOpenAIGatewayService(accountRepo, usageRepo, nil, nil, nil, nil, nil, cfg, nil, concurrency, billingService, nil, billing, nil, &service.DeferredService{}, nil, nil, service.NewModelPricingResolver(channelSvc, billingService), channelSvc, nil, nil, nil)
	t.Cleanup(gateway.CloseOpenAIWSPool)
	h := NewOpenAIGatewayHandler(gateway, concurrency, billing, &service.APIKeyService{}, nil, nil, nil, nil, cfg)
	group := wsAllowlistGroup(true, "gpt-5.4")
	group.ID, group.RateMultiplier = groupID, 1
	apiKey := &service.APIKey{ID: 9503, GroupID: &groupID, User: &service.User{ID: 9504, Status: service.StatusActive}, Group: group}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.User.ID, Concurrency: 4})
		c.Next()
	})
	router.GET("/openai/v1/responses", h.ResponsesWebSocket)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", &coderws.DialOptions{HTTPHeader: http.Header{openAIWSNativeModeHeader: {"native"}}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	write := func(frame string) {
		t.Helper()
		require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(frame)))
	}
	read := func(kind string) {
		t.Helper()
		_, frame, readErr := conn.Read(ctx)
		require.NoError(t, readErr)
		require.Equal(t, kind, gjson.GetBytes(frame, "type").String(), string(frame))
	}
	write(`{"type":"response.create","stream_id":"warmup","model":"gpt-5.4","generate":false,"input":"preload"}`)
	read("response.created")
	read("response.completed")
	write(`{"type":"response.create","stream_id":"a","model":"gpt-5.4","input":"start","tools":[{"type":"function","name":"task","parameters":{"type":"object","properties":{}},"defer_loading":true}]}`)
	read("response.created")
	write(inject)
	read("response.injected")
	write(`{"type":"response.create","stream_id":"b","model":"gpt-5.4","input":"independent"}`)
	read("response.created")
	write(`{"type":"response.steer","previous_response_id":"resp_native_a","input":"Continue carefully"}`)
	read("response.steer.accepted")
	read("response.completed")
	read("response.incomplete")
	read("response.created")
	read("response.completed")
	require.NoError(t, <-upstreamDone)
	warmup := <-upstreamFrames
	require.Equal(t, "false", gjson.Get(warmup, "generate").Raw)
	first := <-upstreamFrames
	require.Equal(t, "a", gjson.Get(first, "stream_id").String())
	require.Equal(t, inject, <-upstreamFrames, "async tool output call_id must remain byte-for-byte unchanged")
	models := make(map[string]int)
	for i := 0; i < 3; i++ {
		select {
		case entry := <-usageRepo.created:
			models[entry.RequestID]++
			require.InDelta(t, perRequestPrice, entry.TotalCost, 1e-9)
		case <-ctx.Done():
			t.Fatal("missing native response usage")
		}
	}
	require.Equal(t, map[string]int{"resp_native_a": 1, "resp_native_b": 1, "resp_native_a2": 1}, models)
	select {
	case unexpected := <-usageRepo.created:
		t.Fatalf("warmup must not create a per-request usage charge: %+v", unexpected)
	default:
	}
	require.EqualValues(t, 3, userAcquires.Load(), "warmup reserves capacity, while steering successor inherits capacity")
	require.EqualValues(t, 3, accountAcquires.Load())
	require.EqualValues(t, 3, atomic.LoadInt32(&cache.releaseUserCalled))
	require.EqualValues(t, 3, atomic.LoadInt32(&cache.releaseAccountCalled))
}
