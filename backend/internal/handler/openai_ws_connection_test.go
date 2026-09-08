package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Route the real HTTP request to a local Responses server without replacing its
// body, cancellation context, SSE parser, admission path, or billing pipeline.
type isolationHTTPUpstream struct {
	service.HTTPUpstream
	target *url.URL
	client *http.Client
}

func (u *isolationHTTPUpstream) Do(request *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.URL.Scheme = u.target.Scheme
	request.URL.Host = u.target.Host
	request.Host = u.target.Host
	return u.client.Do(request)
}

func newIsolationBridgeServer(t *testing.T, upstream http.Handler, configure ...func(*OpenAIGatewayHandler)) (*httptest.Server, <-chan *service.UsageLog, func()) {
	return newIsolationBridgeServerWithGroup(t, upstream, nil, configure...)
}

func newIsolationBridgeServerWithGroup(t *testing.T, upstream http.Handler, group *service.Group, configure ...func(*OpenAIGatewayHandler)) (*httptest.Server, <-chan *service.UsageLog, func()) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	origin := httptest.NewServer(upstream)
	t.Cleanup(origin.Close)
	target, err := url.Parse(origin.URL)
	require.NoError(t, err)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeHTTPBridge
	cfg.Gateway.OpenAIWS.ForceHTTP = true
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 2
	accountRepo := &openAIWSUsageHandlerAccountRepoStub{account: service.Account{
		ID: 901, Name: "isolation-oauth", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, Concurrency: 16,
		Credentials: map[string]any{"access_token": "test-only-access-token"},
		Extra:       map[string]any{"openai_oauth_responses_websockets_v2_mode": service.OpenAIWSIngressModeHTTPBridge},
	}}
	usage := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 16)}
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	gateway := service.NewOpenAIGatewayService(
		accountRepo, usage, nil, nil, nil, nil, nil, cfg, nil, nil,
		service.NewBillingService(cfg, nil), nil, billing,
		&isolationHTTPUpstream{target: target, client: origin.Client()},
		&service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil,
	)
	t.Cleanup(gateway.CloseOpenAIWSPool)
	cache := &concurrencyCacheMock{
		acquireUserSlotFn:    func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(cache), billing, &service.APIKeyService{}, nil, nil, nil, nil, cfg)
	for _, apply := range configure {
		apply(h)
	}
	groupID := int64(902)
	key := &service.APIKey{ID: 903, GroupID: &groupID, User: &service.User{ID: 904, Status: service.StatusActive}, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive}}
	if group != nil {
		key.Group = group
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: key.User.ID, Concurrency: 16})
		c.Next()
	})
	router.GET("/openai/v1/responses", h.ResponsesWebSocket)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server, usage.created, gateway.CloseOpenAIWSPool
}

func dialIsolationBridge(t *testing.T, server *httptest.Server) *coderws.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", &coderws.DialOptions{HTTPHeader: http.Header{"Session-Id": {"identical-root"}, "Thread-Id": {"identical-thread"}}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func writeIsolationFrame(t *testing.T, conn *coderws.Conn, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(payload)))
}

func readIsolationEvent(t *testing.T, conn *coderws.Conn) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, payload, err := conn.Read(ctx)
	require.NoError(t, err)
	return payload
}

func TestOpenAIWSHTTPBridgeIndependentSocketsWarmupAndCancellation(t *testing.T) {
	var calls atomic.Int32
	canceled := make(chan string, 4)
	releaseA := make(chan struct{})
	defer func() {
		select {
		case <-releaseA:
		default:
			close(releaseA)
		}
	}()
	server, usage, _ := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		for _, item := range gjson.GetBytes(body, "input").Array() {
			if !item.IsObject() {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":{"code":"invalid_type","message":"input array entries must be input items"}}`)
				return
			}
		}
		calls.Add(1)
		label := "B"
		if bytes.Contains(body, []byte("hold-A")) {
			label = "A"
		} else if bytes.Contains(body, []byte("hold-C")) {
			label = "C"
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_%s\",\"status\":\"in_progress\",\"model\":\"gpt-5.1\"}}\n\n", label)
		fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"started\"}\n\n")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("SSE test server must support flushing")
			return
		}
		flusher.Flush()
		if label == "A" || label == "C" {
			var proceed <-chan struct{}
			if label == "A" {
				proceed = releaseA
			}
			select {
			case <-proceed:
			case <-r.Context().Done():
				canceled <- label
				return
			}
		}
		fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_%s\",\"status\":\"completed\",\"model\":\"gpt-5.1\",\"output\":[{\"type\":\"message\",\"id\":\"msg_%s\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"%s-finished\"}]}],\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n", label, label, label)
	}))
	a := dialIsolationBridge(t, server)
	writeIsolationFrame(t, a, `{"type":"response.create","model":"gpt-5.1","store":false,"prompt_cache_key":"identical-cache","input":"hold-A"}`)
	require.Equal(t, "response.created", gjson.GetBytes(readIsolationEvent(t, a), "type").String())
	require.Equal(t, "response.output_text.delta", gjson.GetBytes(readIsolationEvent(t, a), "type").String())
	b := dialIsolationBridge(t, server)
	writeIsolationFrame(t, b, `{"type":"response.create","model":"gpt-5.1","store":false,"generate":false,"input":[]}`)
	require.Equal(t, "response.created", gjson.GetBytes(readIsolationEvent(t, b), "type").String())
	warmup := readIsolationEvent(t, b)
	require.Equal(t, "response.completed", gjson.GetBytes(warmup, "type").String())
	require.EqualValues(t, 1, calls.Load(), "prewarm must not invoke the model")
	select {
	case record := <-usage:
		t.Fatalf("prewarm emitted usage: %+v", record)
	default:
	}
	writeIsolationFrame(t, b, fmt.Sprintf(`{"type":"response.create","model":"gpt-5.1","store":false,"previous_response_id":%q,"prompt_cache_key":"identical-cache","input":"finish-B"}`, gjson.GetBytes(warmup, "response.id").String()))
	require.Equal(t, "response.created", gjson.GetBytes(readIsolationEvent(t, b), "type").String())
	require.Equal(t, "response.output_text.delta", gjson.GetBytes(readIsolationEvent(t, b), "type").String())
	require.Equal(t, "B-finished", gjson.GetBytes(readIsolationEvent(t, b), "response.output.0.content.0.text").String())
	require.NoError(t, b.Close(coderws.StatusNormalClosure, "B complete"))
	select {
	case label := <-canceled:
		t.Fatalf("closing B canceled independent upstream %s", label)
	default:
	}
	close(releaseA)
	require.Equal(t, "A-finished", gjson.GetBytes(readIsolationEvent(t, a), "response.output.0.content.0.text").String())
	require.NoError(t, a.Close(coderws.StatusNormalClosure, "A complete"))
	for range 2 {
		select {
		case record := <-usage:
			require.Equal(t, service.RequestTypeWSV2, record.EffectiveRequestType())
		case <-time.After(5 * time.Second):
			t.Fatal("completed bridge response lost its usage record")
		}
	}
	c := dialIsolationBridge(t, server)
	writeIsolationFrame(t, c, `{"type":"response.create","model":"gpt-5.1","store":false,"input":"hold-C"}`)
	require.Equal(t, "response.created", gjson.GetBytes(readIsolationEvent(t, c), "type").String())
	require.Equal(t, "response.output_text.delta", gjson.GetBytes(readIsolationEvent(t, c), "type").String())
	require.NoError(t, c.CloseNow())
	select {
	case label := <-canceled:
		require.Equal(t, "C", label, "disconnect cancels only the owning upstream")
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected execution left upstream inference running")
	}
}

func TestOpenAIWSHTTPBridgeShutdownClosesActiveConnection(t *testing.T) {
	canceled := make(chan struct{})
	server, _, shutdown := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_shutdown\",\"status\":\"in_progress\",\"model\":\"gpt-5.1\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"started\"}\n\n")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("SSE test server must support flushing")
			return
		}
		flusher.Flush()
		<-r.Context().Done()
		close(canceled)
	}))
	client := dialIsolationBridge(t, server)
	writeIsolationFrame(t, client, `{"type":"response.create","model":"gpt-5.1","store":false,"input":"wait for shutdown"}`)
	require.Equal(t, "response.created", gjson.GetBytes(readIsolationEvent(t, client), "type").String())
	require.Equal(t, "response.output_text.delta", gjson.GetBytes(readIsolationEvent(t, client), "type").String())
	shutdown()
	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, _, err := client.Read(readCtx)
	cancel()
	require.Error(t, err)
	require.Equal(t, coderws.StatusGoingAway, coderws.CloseStatus(err))
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown left the owning upstream request running")
	}
	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	next, response, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", nil)
	if next != nil {
		_ = next.CloseNow()
	}
	require.Error(t, err)
	require.NotNil(t, response)
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
}

func TestOpenAIWSHTTPResponseWriterPreservesSSEAndErrors(t *testing.T) {
	var events [][]byte
	writer := newOpenAIWSHTTPResponseWriter(context.Background(), func(event []byte) error { events = append(events, bytes.Clone(event)); return nil }, 4096)
	defer writer.stop()
	for _, chunk := range []string{": heartbeat\r\n\r\nevent: response.output_item.done\r\ndata: {\"item\":", "{\"type\":\"reasoning\",\"encrypted_content\":\"opaque\"}}\r\n\r\ndata: [DONE]\n\n"} {
		_, err := writer.WriteString(chunk)
		require.NoError(t, err)
	}
	require.NoError(t, writer.finish())
	require.Len(t, events, 1)
	require.JSONEq(t, `{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"opaque"}}`, string(events[0]))
	failure := newOpenAIWSHTTPResponseWriter(context.Background(), func(event []byte) error { events = append(events, bytes.Clone(event)); return nil }, 4096)
	defer failure.stop()
	failure.WriteHeader(http.StatusTooManyRequests)
	_, err := failure.WriteString(`{"message":"queue is full"}`)
	require.NoError(t, err)
	require.NoError(t, failure.finish())
	require.Equal(t, "error", gjson.GetBytes(events[1], "type").String())
	require.EqualValues(t, 429, gjson.GetBytes(events[1], "status").Int())
	require.Equal(t, "queue is full", gjson.GetBytes(events[1], "error.message").String())
}

func TestOpenAIWSHTTPBridgeAuditsEachGenerationButNotWarmup(t *testing.T) {
	engine := &turnCountingEngine{
		mode: securityaudit.ModeBlocking,
		decisions: []*securityaudit.PromptDecision{
			{Kind: securityaudit.DecisionAllow, AllowNextStage: true},
			{Kind: securityaudit.DecisionBlock, AllowNextStage: false},
		},
	}
	var upstreamCalls atomic.Int32
	server, _, _ := newIsolationBridgeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_audited\",\"status\":\"completed\",\"model\":\"gpt-5.1\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
	}), func(h *OpenAIGatewayHandler) {
		h.securityAuditCoordinator = securityaudit.NewCoordinator(nil, engine)
	})
	client := dialIsolationBridge(t, server)
	writeIsolationFrame(t, client, `{"type":"response.create","model":"gpt-5.1","store":false,"generate":false,"input":[]}`)
	require.Equal(t, "response.created", gjson.GetBytes(readIsolationEvent(t, client), "type").String())
	require.Equal(t, "response.completed", gjson.GetBytes(readIsolationEvent(t, client), "type").String())
	require.Zero(t, engine.evaluates.Load(), "warmup must not trigger moderation inference")
	require.Zero(t, upstreamCalls.Load())
	payload := `{"type":"response.create","model":"gpt-5.1","store":false,"input":"same prompt on distinct executions"}`
	writeIsolationFrame(t, client, payload)
	require.Equal(t, "response.completed", gjson.GetBytes(readIsolationEvent(t, client), "type").String())
	writeIsolationFrame(t, client, payload)
	blocked := readIsolationEvent(t, client)
	require.Equal(t, "error", gjson.GetBytes(blocked, "type").String())
	require.EqualValues(t, http.StatusForbidden, gjson.GetBytes(blocked, "status").Int())
	require.EqualValues(t, 2, engine.evaluates.Load(), "the second execution must not inherit the first execution's audit cache")
	require.EqualValues(t, 1, upstreamCalls.Load(), "blocked follow-up must not execute upstream")
	require.NoError(t, client.Close(coderws.StatusNormalClosure, "audit contract complete"))
}
