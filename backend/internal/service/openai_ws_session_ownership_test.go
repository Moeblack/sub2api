package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// This file pins the independent-socket ownership contracts of the native
// Responses WebSocket ingress: physical sockets that share user-supplied
// session/thread/cache hints must never observe or cancel each other's
// protocol state. Routing hints (session_id headers, prompt_cache_key) keep
// working for account affinity; protocol state (response->conn, session->conn,
// turn-state, invalid-encrypted lineage) is namespaced per connection.

func newOpenAIWSOwnershipConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.IngressPreviousResponseRecoveryEnabled = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 4
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 4
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	return cfg
}

func newOpenAIWSOwnershipOAuthAccount(id int64) *Account {
	return &Account{
		ID:          id,
		Name:        fmt.Sprintf("openai-ownership-oauth-%d", id),
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 4,
		Credentials: map[string]any{
			"access_token": "oauth-token",
		},
		Extra: map[string]any{
			"openai_oauth_responses_websockets_v2_enabled": true,
		},
	}
}

func newOpenAIWSOwnershipAPIKeyAccount(id int64) *Account {
	return &Account{
		ID:          id,
		Name:        fmt.Sprintf("openai-ownership-apikey-%d", id),
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 4,
		Credentials: map[string]any{
			"api_key": "sk-test",
		},
		Extra: map[string]any{
			"responses_websockets_v2_enabled": true,
		},
	}
}

// openAIWSOwnershipServer accepts client sockets and hands each to the native
// ingress as a direct service caller (no handler-installed connection scope),
// mirroring the production close-error mapping.
type openAIWSOwnershipServer struct {
	server      *httptest.Server
	errCh       chan error
	token       string
	apiKey      *APIKey
	registerCtx bool
}

func newOpenAIWSOwnershipServer(t *testing.T, svc *OpenAIGatewayService, account *Account, sessionIDHeader string) *openAIWSOwnershipServer {
	t.Helper()
	return newOpenAIWSOwnershipServerWithAuth(t, svc, account, sessionIDHeader, nil, false)
}

func newOpenAIWSOwnershipServerWithAuth(t *testing.T, svc *OpenAIGatewayService, account *Account, sessionIDHeader string, apiKey *APIKey, registerCtx bool) *openAIWSOwnershipServer {
	t.Helper()
	s := &openAIWSOwnershipServer{
		errCh:       make(chan error, 8),
		token:       "ownership-token",
		apiKey:      apiKey,
		registerCtx: registerCtx,
	}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{
			CompressionMode: coderws.CompressionContextTakeover,
		})
		if err != nil {
			s.errCh <- err
			return
		}
		defer func() {
			_ = conn.CloseNow()
		}()

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		req := r.Clone(r.Context())
		req.Header = req.Header.Clone()
		req.Header.Set("User-Agent", "unit-test-agent/1.0")
		if sessionIDHeader != "" {
			req.Header.Set("session_id", sessionIDHeader)
		}
		ginCtx.Request = req
		defer svc.CloseOpenAIWSClientConnection(ginCtx, conn)
		if s.apiKey != nil {
			ginCtx.Set("api_key", s.apiKey)
		}

		readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		msgType, firstMessage, readErr := conn.Read(readCtx)
		cancel()
		if readErr != nil {
			s.errCh <- readErr
			return
		}
		if msgType != coderws.MessageText && msgType != coderws.MessageBinary {
			s.errCh <- errors.New("unsupported websocket client message type")
			return
		}

		proxyCtx := r.Context()
		if s.registerCtx {
			registered, unregister := svc.RegisterOpenAIWSConnection(proxyCtx)
			defer unregister()
			proxyCtx = registered
		}
		proxyErr := svc.ProxyResponsesWebSocketFromClient(proxyCtx, ginCtx, conn, account, s.token, firstMessage, nil)
		// Mirror the production handler: surface OpenAIWSClientCloseError to
		// the client via a graceful close handshake.
		var closeErr *OpenAIWSClientCloseError
		if errors.As(proxyErr, &closeErr) {
			_ = conn.Close(closeErr.StatusCode(), closeErr.Reason())
		}
		s.errCh <- proxyErr
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *openAIWSOwnershipServer) dial(t *testing.T) *coderws.Conn {
	t.Helper()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(s.server.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	return clientConn
}

func (s *openAIWSOwnershipServer) nextServerErr(t *testing.T) error {
	t.Helper()
	select {
	case serverErr := <-s.errCh:
		return serverErr
	case <-time.After(5 * time.Second):
		t.Fatal("等待 ingress websocket 结束超时")
		return nil
	}
}

func openAIWSOwnershipWrite(t *testing.T, conn *coderws.Conn, payload string) {
	t.Helper()
	writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, conn.Write(writeCtx, coderws.MessageText, []byte(payload)))
}

func openAIWSOwnershipRead(t *testing.T, conn *coderws.Conn) []byte {
	t.Helper()
	readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	msgType, message, readErr := conn.Read(readCtx)
	require.NoError(t, readErr)
	require.Equal(t, coderws.MessageText, msgType)
	return message
}

func openAIWSConnWrites(conn *openAIWSCaptureConn) []map[string]any {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	return append([]map[string]any(nil), conn.writes...)
}

// TestOpenAIWSIngress_IndependentSocketsSharingHintsHoldIndependentConns pins
// the core isolation contract: two physical sockets with identical session
// hints (session_id header + prompt_cache_key, store=false) must hold
// independent upstream connections, and neither entry cancels the sibling.
func TestOpenAIWSIngress_IndependentSocketsSharingHintsHoldIndependentConns(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := newOpenAIWSOwnershipConfig()
	connA := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_own_A1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
			[]byte(`{"type":"response.completed","response":{"id":"resp_own_A2","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	connB := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_own_B1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{connA, connB}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := newOpenAIWSOwnershipOAuthAccount(701)
	server := newOpenAIWSOwnershipServer(t, svc, account, "shared-session-hint")

	// Socket A: store=false session with the shared hints.
	socketA := server.dial(t)
	defer func() { _ = socketA.CloseNow() }()
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":false,"prompt_cache_key":"shared-cache-hint","input":[{"type":"input_text","text":"a-first"}]}`)
	turnA1 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_own_A1", gjson.GetBytes(turnA1, "response.id").String())

	// Socket B with identical hints enters while A is mid-session. It must not
	// observe A's session->conn binding, so it dials its own upstream conn.
	socketB := server.dial(t)
	defer func() { _ = socketB.CloseNow() }()
	openAIWSOwnershipWrite(t, socketB, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":false,"prompt_cache_key":"shared-cache-hint","input":[{"type":"input_text","text":"b-first"}]}`)
	turnB1 := openAIWSOwnershipRead(t, socketB)
	require.Equal(t, "resp_own_B1", gjson.GetBytes(turnB1, "response.id").String())

	// A must still be alive: no cross-socket cancellation, and its chained
	// continuation keeps the same connection without a new dial.
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":false,"previous_response_id":"resp_own_A1","prompt_cache_key":"shared-cache-hint","input":[{"type":"input_text","text":"a-second"}]}`)
	turnA2 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_own_A2", gjson.GetBytes(turnA2, "response.id").String())

	require.NoError(t, socketA.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, socketB.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, server.nextServerErr(t))
	require.NoError(t, server.nextServerErr(t))

	writesA := openAIWSConnWrites(connA)
	require.Len(t, writesA, 2, "socket A 的两个 turn 都应落在 A 的连接上")
	require.Equal(t, "a-first", gjson.Get(requestToJSONString(writesA[0]), "input.0.text").String())
	require.Equal(t, "resp_own_A1", gjson.Get(requestToJSONString(writesA[1]), "previous_response_id").String())
	writesB := openAIWSConnWrites(connB)
	require.Len(t, writesB, 1, "socket B 的 turn 应落在 B 自己的连接上")
	require.Equal(t, "b-first", gjson.Get(requestToJSONString(writesB[0]), "input.0.text").String())
	require.False(t, gjson.Get(requestToJSONString(writesB[0]), "previous_response_id").Exists())
}

// TestOpenAIWSIngress_StoreEnabledCrossSocketForkKeepsOwnConn pins that a
// store-enabled socket forking from another socket's response keeps its own
// upstream connection: the parent reference is forwarded (upstream may honor
// it), but the sibling's pooled connection is never preferred or disturbed.
func TestOpenAIWSIngress_StoreEnabledCrossSocketForkKeepsOwnConn(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := newOpenAIWSOwnershipConfig()
	connA := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_fork_A1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
			[]byte(`{"type":"response.completed","response":{"id":"resp_fork_A2","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	connB := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_fork_B1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{connA, connB}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := newOpenAIWSOwnershipAPIKeyAccount(702)
	server := newOpenAIWSOwnershipServer(t, svc, account, "shared-session-hint")

	socketA := server.dial(t)
	defer func() { _ = socketA.CloseNow() }()
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"input":[{"type":"input_text","text":"a-first"}]}`)
	turnA1 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_fork_A1", gjson.GetBytes(turnA1, "response.id").String())

	// Socket B forks from A's response. With connection-local protocol state
	// the response->conn lookup misses, so B dials its own conn and forwards
	// the parent reference verbatim for upstream to honor.
	socketB := server.dial(t)
	defer func() { _ = socketB.CloseNow() }()
	openAIWSOwnershipWrite(t, socketB, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"previous_response_id":"resp_fork_A1","input":[{"type":"input_text","text":"b-fork"}]}`)
	turnB1 := openAIWSOwnershipRead(t, socketB)
	require.Equal(t, "resp_fork_B1", gjson.GetBytes(turnB1, "response.id").String())

	// A remains fully functional after B's fork.
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"previous_response_id":"resp_fork_A1","input":[{"type":"input_text","text":"a-second"}]}`)
	turnA2 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_fork_A2", gjson.GetBytes(turnA2, "response.id").String())

	require.NoError(t, socketA.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, socketB.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, server.nextServerErr(t))
	require.NoError(t, server.nextServerErr(t))

	writesB := openAIWSConnWrites(connB)
	require.Len(t, writesB, 1)
	require.Equal(t, "resp_fork_A1", gjson.Get(requestToJSONString(writesB[0]), "previous_response_id").String(),
		"fork 的父引用必须原样转发给上游裁决，不得在网关侧剥离")
	writesA := openAIWSConnWrites(connA)
	require.Len(t, writesA, 2, "socket A 不受 sibling fork 影响")
}

// TestOpenAIWSIngress_StoreDisabledCrossSocketForkRejected pins that a
// store=false (ZDR) socket cannot anchor on another socket's response: the
// unknown parent is rejected with a clean close instead of being silently
// stripped or leaking the sibling's connection.
func TestOpenAIWSIngress_StoreDisabledCrossSocketForkRejected(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := newOpenAIWSOwnershipConfig()
	connA := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_zdr_A1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
			[]byte(`{"type":"response.completed","response":{"id":"resp_zdr_A2","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{connA}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	// OAuth (Codex protocol) forces store-disabled semantics.
	account := newOpenAIWSOwnershipOAuthAccount(703)
	server := newOpenAIWSOwnershipServer(t, svc, account, "shared-session-hint")

	socketA := server.dial(t)
	defer func() { _ = socketA.CloseNow() }()
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":false,"input":[{"type":"input_text","text":"a-first"}]}`)
	turnA1 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_zdr_A1", gjson.GetBytes(turnA1, "response.id").String())

	socketB := server.dial(t)
	defer func() { _ = socketB.CloseNow() }()
	openAIWSOwnershipWrite(t, socketB, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":false,"previous_response_id":"resp_zdr_A1","input":[{"type":"input_text","text":"b-fork"}]}`)
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, _, readErr := socketB.Read(readCtx)
	cancelRead()
	var closeErr coderws.CloseError
	require.ErrorAs(t, readErr, &closeErr)
	require.Equal(t, coderws.StatusPolicyViolation, closeErr.Code)
	require.Equal(t, "upstream continuation connection is unavailable; please restart the conversation", closeErr.Reason)

	// The sibling is untouched: no dial was consumed for B and A continues.
	require.Equal(t, 1, dialer.DialCount(), "被拒绝的跨 socket 锚点不得消耗上游连接")
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":false,"previous_response_id":"resp_zdr_A1","input":[{"type":"input_text","text":"a-second"}]}`)
	turnA2 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_zdr_A2", gjson.GetBytes(turnA2, "response.id").String())

	require.NoError(t, socketA.Close(coderws.StatusNormalClosure, "done"))
	// B's rejected fork surfaces as an OpenAIWSClientCloseError first; A then
	// shuts down cleanly.
	var rejectedErr *OpenAIWSClientCloseError
	require.ErrorAs(t, server.nextServerErr(t), &rejectedErr)
	require.Equal(t, coderws.StatusPolicyViolation, rejectedErr.StatusCode())
	require.NoError(t, server.nextServerErr(t))

	writesA := openAIWSConnWrites(connA)
	require.Len(t, writesA, 2, "socket A 的两个 turn 都应完成，未被 sibling 取消")
}

// TestOpenAIWSIngress_UnknownFirstTurnParentSurfacesNotFound pins the strict
// unknown-parent contract: a first-turn previous_response_id that upstream
// rejects is surfaced to the client as previous_response_not_found and is
// never silently stripped and replayed as an incomplete delta.
func TestOpenAIWSIngress_UnknownFirstTurnParentSurfacesNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := newOpenAIWSOwnershipConfig()
	connA := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"error","error":{"type":"invalid_request_error","code":"previous_response_not_found","message":"Previous response with id 'resp_foreign' not found."}}`),
			[]byte(`{"type":"response.failed","response":{"id":"resp_foreign_failed","status":"failed","model":"gpt-5.1","usage":{"input_tokens":0,"output_tokens":0}}}`),
		},
	}
	dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{connA}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := newOpenAIWSOwnershipAPIKeyAccount(704)
	server := newOpenAIWSOwnershipServer(t, svc, account, "")

	socketA := server.dial(t)
	defer func() { _ = socketA.CloseNow() }()
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"previous_response_id":"resp_foreign","input":[{"type":"input_text","text":"delta-only"}]}`)

	errorEvent := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "error", gjson.GetBytes(errorEvent, "type").String())
	require.Equal(t, "previous_response_not_found", gjson.GetBytes(errorEvent, "error.code").String(),
		"未知父引用必须原样透传上游的 previous_response_not_found")
	failedEvent := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "response.failed", gjson.GetBytes(failedEvent, "type").String())

	require.NoError(t, socketA.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, server.nextServerErr(t))

	require.Equal(t, 1, dialer.DialCount(), "未知父引用不得触发换连重试")
	writesA := openAIWSConnWrites(connA)
	require.Len(t, writesA, 1, "不得剥离 previous_response_id 后重放残缺增量")
	require.Equal(t, "resp_foreign", gjson.Get(requestToJSONString(writesA[0]), "previous_response_id").String())
}

// TestOpenAIWSIngress_InvalidEncryptedLineageIsConnectionLocal pins that the
// invalid_encrypted_content lineage is connection-local: a digest marked on
// one socket strips later turns of that same socket but never strips another
// socket sharing the same session hints.
func TestOpenAIWSIngress_InvalidEncryptedLineageIsConnectionLocal(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := newOpenAIWSOwnershipConfig()
	connA := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"error","error":{"type":"invalid_request_error","code":"invalid_encrypted_content","message":"Could not decrypt encrypted_content."}}`),
			[]byte(`{"type":"response.failed","response":{"id":"resp_lineage_A1","status":"failed","model":"gpt-5.1","usage":{"input_tokens":0,"output_tokens":0}}}`),
			[]byte(`{"type":"response.completed","response":{"id":"resp_lineage_A2","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	connB := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_lineage_B1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{connA, connB}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := newOpenAIWSOwnershipAPIKeyAccount(705)
	server := newOpenAIWSOwnershipServer(t, svc, account, "shared-session-hint")

	reasoningItem := `{"type":"reasoning","id":"rs_shared","encrypted_content":"opaque-blob-1","summary":[]}`

	// Socket A marks the digest via an upstream invalid_encrypted_content error.
	socketA := server.dial(t)
	defer func() { _ = socketA.CloseNow() }()
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"prompt_cache_key":"shared-cache-hint","input":[`+reasoningItem+`,{"type":"input_text","text":"a-first"}]}`)
	errorEvent := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "error", gjson.GetBytes(errorEvent, "type").String())
	require.Equal(t, "invalid_encrypted_content", gjson.GetBytes(errorEvent, "error.code").String())
	failedEvent := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "response.failed", gjson.GetBytes(failedEvent, "type").String())

	// The same socket's next turn strips the known-invalid digest.
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"prompt_cache_key":"shared-cache-hint","input":[`+reasoningItem+`,{"type":"input_text","text":"a-second"}]}`)
	turnA2 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_lineage_A2", gjson.GetBytes(turnA2, "response.id").String())

	// Socket B with identical hints keeps its encrypted item: lineage is
	// connection-local protocol state, not a shared session property.
	socketB := server.dial(t)
	defer func() { _ = socketB.CloseNow() }()
	openAIWSOwnershipWrite(t, socketB, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"prompt_cache_key":"shared-cache-hint","input":[`+reasoningItem+`,{"type":"input_text","text":"b-first"}]}`)
	turnB1 := openAIWSOwnershipRead(t, socketB)
	require.Equal(t, "resp_lineage_B1", gjson.GetBytes(turnB1, "response.id").String())

	require.NoError(t, socketA.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, socketB.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, server.nextServerErr(t))
	require.NoError(t, server.nextServerErr(t))

	writesA := openAIWSConnWrites(connA)
	require.Len(t, writesA, 2)
	require.Contains(t, requestToJSONString(writesA[0]), "opaque-blob-1", "首轮写入尚未标记 lineage，密文原样上行")
	require.NotContains(t, requestToJSONString(writesA[1]), "opaque-blob-1", "同连接后续 turn 应剥离已知失效密文")
	writesB := openAIWSConnWrites(connB)
	require.Len(t, writesB, 1)
	require.Contains(t, requestToJSONString(writesB[0]), "opaque-blob-1", "lineage 不得跨连接泄露：socket B 保留密文")
}

// TestOpenAIWSIngress_ClientLifetimeExpiresWithGracefulClose pins the
// physical-socket lifetime contract: at the deadline the inter-turn read is
// interrupted with a graceful GoingAway close handshake carrying the lifetime
// reason — never a bare 1006 on the wire — and the server unwinds cleanly.
func TestOpenAIWSIngress_ClientLifetimeExpiresWithGracefulClose(t *testing.T) {
	gin.SetMode(gin.TestMode)

	prevLifetime := openAIWSIngressClientMaxLifetime
	openAIWSIngressClientMaxLifetime = 200 * time.Millisecond
	defer func() {
		openAIWSIngressClientMaxLifetime = prevLifetime
	}()

	cfg := newOpenAIWSOwnershipConfig()
	connA := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_lifetime_1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{connA}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := newOpenAIWSOwnershipAPIKeyAccount(706)
	server := newOpenAIWSOwnershipServer(t, svc, account, "")

	socketA := server.dial(t)
	defer func() { _ = socketA.CloseNow() }()
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"input":[{"type":"input_text","text":"hello"}]}`)
	turnA1 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_lifetime_1", gjson.GetBytes(turnA1, "response.id").String())

	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, _, readErr := socketA.Read(readCtx)
	cancelRead()
	var clientClose coderws.CloseError
	require.ErrorAs(t, readErr, &clientClose)
	require.Equal(t, coderws.StatusGoingAway, clientClose.Code, "lifetime 到期必须走优雅关闭握手，不得出现 1006")
	require.Equal(t, "maximum websocket connection lifetime reached; please reconnect", clientClose.Reason)

	var closeErr *OpenAIWSClientCloseError
	require.ErrorAs(t, server.nextServerErr(t), &closeErr)
	require.Equal(t, coderws.StatusGoingAway, closeErr.StatusCode())
	require.Equal(t, "maximum websocket connection lifetime reached; please reconnect", closeErr.Reason())
}

func TestOpenAIWSIngress_LifetimeStopsActiveGeneration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	previous := openAIWSIngressClientMaxLifetime
	openAIWSIngressClientMaxLifetime = 250 * time.Millisecond
	defer func() { openAIWSIngressClientMaxLifetime = previous }()
	upstream := newStagedPassthroughConn()
	svc := newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream)
	t.Cleanup(svc.CloseOpenAIWSPool)
	account := newOpenAIWSOwnershipAPIKeyAccount(710)
	account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough}
	server := newOpenAIWSOwnershipServer(t, svc, account, "")
	client := server.dial(t)
	defer func() { _ = client.CloseNow() }()
	openAIWSOwnershipWrite(t, client, `{"type":"response.create","model":"gpt-5.1","store":false,"input":"keep generating"}`)
	select {
	case <-upstream.writes:
	case <-time.After(3 * time.Second):
		t.Fatal("generation did not start")
	}
	upstream.Send(`{"type":"response.output_text.delta","delta":"active"}`)
	require.Equal(t, "active", gjson.GetBytes(openAIWSOwnershipRead(t, client), "delta").String())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := client.Read(ctx)
	require.Equal(t, coderws.StatusGoingAway, coderws.CloseStatus(err))
	select {
	case <-upstream.closed:
	case <-ctx.Done():
		t.Fatal("physical lifetime left active upstream work running")
	}
}

// TestOpenAIWSIngress_ShutdownCancelsSocketWithGracefulClose pins the bounded
// shutdown contract: cancelling registered connection contexts (what
// CloseOpenAIWSPool does at service teardown) reaches the client as a
// graceful close frame, not a torn TCP connection.
func TestOpenAIWSIngress_ShutdownCancelsSocketWithGracefulClose(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := newOpenAIWSOwnershipConfig()
	connA := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_shutdown_1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{connA}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := newOpenAIWSOwnershipAPIKeyAccount(707)
	server := newOpenAIWSOwnershipServerWithAuth(t, svc, account, "", nil, true)

	socketA := server.dial(t)
	defer func() { _ = socketA.CloseNow() }()
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"input":[{"type":"input_text","text":"hello"}]}`)
	turnA1 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_shutdown_1", gjson.GetBytes(turnA1, "response.id").String())

	// Service teardown while the client idles between turns.
	svc.CloseOpenAIWSPool()

	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, _, readErr := socketA.Read(readCtx)
	cancelRead()
	var clientClose coderws.CloseError
	require.ErrorAs(t, readErr, &clientClose)
	require.Equal(t, coderws.StatusGoingAway, clientClose.Code, "shutdown 必须下发 1001 关闭帧而非裸断开")

	if serverErr := server.nextServerErr(t); serverErr != nil {
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, serverErr, &closeErr)
		require.Equal(t, coderws.StatusGoingAway, closeErr.StatusCode())
	}
}

// TestOpenAIWSIngress_CrossTenantForkDeniedSameTenantAllowed pins the
// stored-response ownership contract for store=true cross-socket
// continuations: the producing tenant (same user, any of their keys) may fork,
// while another tenant is denied with a previous_response_not_found-shaped
// error event and a policy close before any upstream write.
func TestOpenAIWSIngress_CrossTenantForkDeniedSameTenantAllowed(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := newOpenAIWSOwnershipConfig()
	connA := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_tenant_A1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
			[]byte(`{"type":"response.completed","response":{"id":"resp_tenant_A2","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	connC := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.completed","response":{"id":"resp_tenant_C1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
		},
	}
	dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{connA, connC}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := newOpenAIWSOwnershipAPIKeyAccount(708)
	groupID := int64(0)
	tenantA := &APIKey{ID: 21, UserID: 11, Key: "sk-tenant-a"}
	tenantB := &APIKey{ID: 22, UserID: 12, Key: "sk-tenant-b"}
	tenantAOtherKey := &APIKey{ID: 29, UserID: 11, Key: "sk-tenant-a2"}
	serverA := newOpenAIWSOwnershipServerWithAuth(t, svc, account, "", tenantA, false)
	serverB := newOpenAIWSOwnershipServerWithAuth(t, svc, account, "", tenantB, false)
	serverC := newOpenAIWSOwnershipServerWithAuth(t, svc, account, "", tenantAOtherKey, false)

	// Tenant A produces the stored response; its owner is recorded.
	socketA := serverA.dial(t)
	defer func() { _ = socketA.CloseNow() }()
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"input":[{"type":"input_text","text":"a-first"}]}`)
	turnA1 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_tenant_A1", gjson.GetBytes(turnA1, "response.id").String())

	ownerUserID, ownerAPIKeyID, ownerFound, ownerErr := svc.getOpenAIWSStateStore().GetHTTPResponseOwner(context.Background(), groupID, "resp_tenant_A1")
	require.NoError(t, ownerErr)
	require.True(t, ownerFound, "完成轮必须为已认证租户记录 response owner")
	require.Equal(t, int64(11), ownerUserID)
	require.Equal(t, int64(21), ownerAPIKeyID)

	// Tenant B (different user) fork is denied before consuming an upstream conn.
	socketB := serverB.dial(t)
	defer func() { _ = socketB.CloseNow() }()
	openAIWSOwnershipWrite(t, socketB, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"previous_response_id":"resp_tenant_A1","input":[{"type":"input_text","text":"b-fork"}]}`)
	denyEvent := openAIWSOwnershipRead(t, socketB)
	require.Equal(t, "error", gjson.GetBytes(denyEvent, "type").String())
	require.Equal(t, "previous_response_not_found", gjson.GetBytes(denyEvent, "error.code").String(),
		"跨租户引用必须以 previous_response_not_found 形态下发")
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, _, readErr := socketB.Read(readCtx)
	cancelRead()
	var closeB coderws.CloseError
	require.ErrorAs(t, readErr, &closeB)
	require.Equal(t, coderws.StatusPolicyViolation, closeB.Code)
	require.Equal(t, "previous_response_id is not available on this connection", closeB.Reason)
	require.Equal(t, 1, dialer.DialCount(), "跨租户 fork 在上游写入前被拒绝，不得消耗连接")

	// Tenant A's other key (same user) forks successfully: same-user keys
	// remain interoperable, and the fork keeps its own physical conn.
	socketC := serverC.dial(t)
	defer func() { _ = socketC.CloseNow() }()
	openAIWSOwnershipWrite(t, socketC, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"previous_response_id":"resp_tenant_A1","input":[{"type":"input_text","text":"c-fork"}]}`)
	turnC1 := openAIWSOwnershipRead(t, socketC)
	require.Equal(t, "resp_tenant_C1", gjson.GetBytes(turnC1, "response.id").String())

	// The producer socket is unaffected by both attempts.
	openAIWSOwnershipWrite(t, socketA, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"previous_response_id":"resp_tenant_A1","input":[{"type":"input_text","text":"a-second"}]}`)
	turnA2 := openAIWSOwnershipRead(t, socketA)
	require.Equal(t, "resp_tenant_A2", gjson.GetBytes(turnA2, "response.id").String())

	require.NoError(t, socketA.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, socketC.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, serverA.nextServerErr(t))
	require.NoError(t, serverC.nextServerErr(t))
	var deniedErr *OpenAIWSClientCloseError
	require.ErrorAs(t, serverB.nextServerErr(t), &deniedErr)
	require.Equal(t, coderws.StatusPolicyViolation, deniedErr.StatusCode())

	writesC := openAIWSConnWrites(connC)
	require.Len(t, writesC, 1)
	require.Equal(t, "resp_tenant_A1", gjson.Get(requestToJSONString(writesC[0]), "previous_response_id").String(),
		"同租户 fork 的父引用原样转发上游")
}

// TestOpenAIWSIngress_PassthroughFailoverAttemptNeverClosesClient pins the
// failover lifecycle contract: when a passthrough attempt ends in a retryable
// pre-output rate limit, the attempt teardown must not send any close frame
// to the physical client — the handler retries on another account and the
// same socket carries the retried turn. This regresses the client-lifecycle
// close that fired on per-attempt context cancellation.
func TestOpenAIWSIngress_PassthroughFailoverAttemptNeverClosesClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := passthroughLifecycleConfig()
	upstreamFirst := newStagedPassthroughConn()
	upstreamRetry := newStagedPassthroughConn()
	dialer := &stagedPassthroughDialer{conns: []openAIWSClientConn{upstreamFirst, upstreamRetry}}
	svc := &OpenAIGatewayService{
		cfg:                       cfg,
		httpUpstream:              &httpUpstreamRecorder{},
		cache:                     &stubGatewayCache{},
		openaiWSResolver:          NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:             NewCodexToolCorrector(),
		openaiWSPassthroughDialer: dialer,
	}
	account := &Account{
		ID:          709,
		Name:        "openai-ownership-passthrough-failover",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 4,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra: map[string]any{
			"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough,
		},
	}

	serverErrCh := make(chan error, 2)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{
			CompressionMode: coderws.CompressionContextTakeover,
		})
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		req := r.Clone(r.Context())
		req.Header = req.Header.Clone()
		req.Header.Set("User-Agent", "unit-test-agent/1.0")
		ginCtx.Request = req

		readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		_, firstMessage, readErr := conn.Read(readCtx)
		cancel()
		if readErr != nil {
			serverErrCh <- readErr
			return
		}
		proxyErr := svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, nil)
		var failoverErr *UpstreamFailoverError
		if !errors.As(proxyErr, &failoverErr) {
			serverErrCh <- proxyErr
			return
		}
		// Handler-style same-socket retry with the exact first message.
		serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, nil)
	}))
	defer wsServer.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	openAIWSOwnershipWrite(t, clientConn, `{"type":"response.create","model":"gpt-5.1","stream":false,"store":true,"input":[{"type":"input_text","text":"hello"}]}`)

	// Attempt 1: upstream answers with a pre-output rate limit. The attempt
	// ends in a failover error and must leave the physical client untouched.
	upstreamFirst.Send(`{"type":"error","error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"usage limit reached"}}`)

	// The retry turn completes on the second upstream; between the attempts no
	// close frame (or any frame) may reach the client.
	select {
	case <-upstreamRetry.writes:
	case <-time.After(3 * time.Second):
		t.Fatal("retry did not reach the replacement upstream")
	}
	upstreamRetry.Send(`{"type":"response.completed","response":{"id":"resp_pt_retry_1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)

	retryTurn := openAIWSOwnershipRead(t, clientConn)
	require.Equal(t, "response.completed", gjson.GetBytes(retryTurn, "type").String(),
		"失败转移期间客户端不得收到 1001 等关闭帧；重试轮必须在同一 socket 上交付")
	require.Equal(t, "resp_pt_retry_1", gjson.GetBytes(retryTurn, "response.id").String())
	require.Equal(t, 2, dialer.CallCount(), "限流失败应触发一次换号重试")
	// A reader left behind by attempt 1 must not steal the next real turn.
	openAIWSOwnershipWrite(t, clientConn, `{"type":"response.create","model":"gpt-5.1","store":true,"previous_response_id":"resp_pt_retry_1","input":"after retry"}`)
	select {
	case next := <-upstreamRetry.writes:
		require.Contains(t, string(next), "after retry")
	case <-time.After(3 * time.Second):
		t.Fatal("a retired attempt stole the next client frame")
	}
	upstreamRetry.Send(`{"type":"response.completed","response":{"id":"resp_pt_retry_2","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	require.Equal(t, "resp_pt_retry_2", gjson.GetBytes(openAIWSOwnershipRead(t, clientConn), "response.id").String())

	require.NoError(t, clientConn.Close(coderws.StatusNormalClosure, "done"))
	require.NoError(t, <-serverErrCh)
}

func TestOpenAIWSConnectionRegistryLifecycle(t *testing.T) {
	registry := NewOpenAIWSConnectionRegistry()

	ctxA, unregisterA := registry.Register(context.Background())
	require.NoError(t, ctxA.Err())
	ctxB, unregisterB := registry.Register(context.Background())
	require.NoError(t, ctxB.Err())

	// Unregister is idempotent and does not cancel the child.
	unregisterA()
	unregisterA()
	require.NoError(t, ctxA.Err())

	registry.Close()
	require.NoError(t, ctxA.Err(), "已注销的连接不受 Close 影响")
	require.Error(t, ctxB.Err(), "Close 必须取消仍注册的连接上下文")
	// Unregister after Close is a safe no-op.
	unregisterB()

	// Register-after-Close yields a pre-cancelled context.
	ctxC, unregisterC := registry.Register(context.Background())
	require.Error(t, ctxC.Err())
	unregisterC()

	// Close is idempotent.
	registry.Close()
}

func TestOpenAIWSRegisteredConnectionsCancelOnPoolClose(t *testing.T) {
	svc := &OpenAIGatewayService{}

	ctx, unregister := svc.RegisterOpenAIWSConnection(context.Background())
	defer unregister()
	require.NoError(t, ctx.Err())

	// Pool is nil here: registered connections must still be cancelled.
	svc.CloseOpenAIWSPool()
	require.Error(t, ctx.Err(), "CloseOpenAIWSPool 必须取消注册的下游连接（即使 pool 为 nil）")

	lateCtx, lateUnregister := svc.RegisterOpenAIWSConnection(context.Background())
	defer lateUnregister()
	require.Error(t, lateCtx.Err(), "服务关闭后注册的连接应立即取消")

	require.NotPanics(t, func() { svc.CloseOpenAIWSPool() })
}

func TestShouldBridgeOpenAIWSIngressRequest(t *testing.T) {
	newCfg := func() *config.Config {
		cfg := &config.Config{}
		cfg.Gateway.OpenAIWS.Enabled = true
		cfg.Gateway.OpenAIWS.OAuthEnabled = true
		cfg.Gateway.OpenAIWS.APIKeyEnabled = true
		cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
		cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
		cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
		cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 64
		return cfg
	}
	accountWithMode := func(mode string) *Account {
		return &Account{
			Platform:    PlatformOpenAI,
			Type:        AccountTypeAPIKey,
			Concurrency: 1,
			Extra: map[string]any{
				"openai_apikey_responses_websockets_v2_mode": mode,
			},
		}
	}
	smallPayload := []byte(`{"type":"response.create","model":"gpt-5.1","input":[]}`)
	bigPayload := []byte(`{"type":"response.create","model":"gpt-5.1","input":[{"type":"input_text","text":"0123456789012345678901234567890123456789"}]}`)
	bigPayloadWithPrev := []byte(`{"type":"response.create","model":"gpt-5.1","previous_response_id":"resp_x","input":[{"type":"input_text","text":"0123456789012345678901234567890123456789"}]}`)
	require.GreaterOrEqual(t, len(bigPayload), 64)
	require.GreaterOrEqual(t, len(bigPayloadWithPrev), 64)

	t.Run("account_level_bridge_short_circuits", func(t *testing.T) {
		svc := &OpenAIGatewayService{cfg: newCfg()}
		require.True(t, svc.ShouldBridgeOpenAIWSIngressRequest(accountWithMode(OpenAIWSIngressModeHTTPBridge), smallPayload))
	})

	t.Run("grok_always_bridges", func(t *testing.T) {
		svc := &OpenAIGatewayService{cfg: newCfg()}
		grok := &Account{Platform: PlatformGrok, Type: AccountTypeOAuth, Concurrency: 1}
		require.True(t, svc.ShouldBridgeOpenAIWSIngressRequest(grok, smallPayload))
	})

	t.Run("account_force_http_bridges", func(t *testing.T) {
		svc := &OpenAIGatewayService{cfg: newCfg()}
		account := accountWithMode(OpenAIWSIngressModeCtxPool)
		account.Extra["openai_ws_force_http"] = true
		require.True(t, svc.ShouldBridgeOpenAIWSIngressRequest(account, smallPayload))
	})

	t.Run("explicit_native_small_payload_stays_native", func(t *testing.T) {
		svc := &OpenAIGatewayService{cfg: newCfg()}
		require.False(t, svc.ShouldBridgeOpenAIWSIngressRequest(accountWithMode(OpenAIWSIngressModeCtxPool), smallPayload))
	})

	t.Run("payload_fallback_requires_enablement", func(t *testing.T) {
		svc := &OpenAIGatewayService{cfg: newCfg()}
		require.False(t, svc.ShouldBridgeOpenAIWSIngressRequest(accountWithMode(OpenAIWSIngressModeCtxPool), bigPayload),
			"HTTPBridgeEnabled 关闭时超大首帧不得触发桥回退")
	})

	t.Run("payload_fallback_bridges_oversized_first_message", func(t *testing.T) {
		cfg := newCfg()
		cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
		svc := &OpenAIGatewayService{cfg: cfg}
		require.True(t, svc.ShouldBridgeOpenAIWSIngressRequest(accountWithMode(OpenAIWSIngressModeCtxPool), bigPayload))
	})

	t.Run("payload_fallback_skips_previous_response_id", func(t *testing.T) {
		cfg := newCfg()
		cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
		svc := &OpenAIGatewayService{cfg: cfg}
		require.False(t, svc.ShouldBridgeOpenAIWSIngressRequest(accountWithMode(OpenAIWSIngressModeCtxPool), bigPayloadWithPrev),
			"携带 previous_response_id 的首帧不走 payload 桥回退")
	})

	t.Run("payload_fallback_never_resurrects_mode_off", func(t *testing.T) {
		cfg := newCfg()
		cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
		svc := &OpenAIGatewayService{cfg: cfg}
		require.False(t, svc.ShouldBridgeOpenAIWSIngressRequest(accountWithMode(OpenAIWSIngressModeOff), bigPayload))
	})

	t.Run("passthrough_oversized_first_message_bridges", func(t *testing.T) {
		cfg := newCfg()
		cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
		svc := &OpenAIGatewayService{cfg: cfg}
		require.True(t, svc.ShouldBridgeOpenAIWSIngressRequest(accountWithMode(OpenAIWSIngressModePassthrough), bigPayload))
	})

	t.Run("passthrough_small_message_stays_native", func(t *testing.T) {
		cfg := newCfg()
		cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
		svc := &OpenAIGatewayService{cfg: cfg}
		require.False(t, svc.ShouldBridgeOpenAIWSIngressRequest(accountWithMode(OpenAIWSIngressModePassthrough), smallPayload))
	})

	t.Run("nil_account_never_bridges", func(t *testing.T) {
		svc := &OpenAIGatewayService{cfg: newCfg()}
		require.False(t, svc.ShouldBridgeOpenAIWSIngressRequest(nil, bigPayload))
	})
}

func TestIsOpenAIAccountTransportCompatible_HTTPBridgeIngress(t *testing.T) {
	accountWithMode := func(mode string) *Account {
		return &Account{
			ID:          8801,
			Platform:    PlatformOpenAI,
			Type:        AccountTypeAPIKey,
			Status:      StatusActive,
			Schedulable: true,
			Concurrency: 1,
			Extra: map[string]any{
				"openai_apikey_responses_websockets_v2_mode": mode,
			},
		}
	}
	newRouterCfg := func() *config.Config {
		cfg := newSchedulerTestOpenAIWSV2Config()
		cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
		cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
		return cfg
	}

	t.Run("nil_service_or_account_rejected", func(t *testing.T) {
		scheduler := &defaultOpenAIAccountScheduler{}
		require.False(t, scheduler.isAccountTransportCompatible(nil, OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
		scheduler.service = &OpenAIGatewayService{cfg: newRouterCfg()}
		require.False(t, scheduler.isAccountTransportCompatible(nil, OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})

	t.Run("explicit_native_rejected_without_bridge_route", func(t *testing.T) {
		scheduler := &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: newRouterCfg()}}
		require.False(t, scheduler.isAccountTransportCompatible(accountWithMode(OpenAIWSIngressModeCtxPool), OpenAIUpstreamTransportResponsesHTTPBridgeIngress),
			"显式 native 账号不得被静默改走 HTTP")
		require.False(t, scheduler.isAccountTransportCompatible(accountWithMode(OpenAIWSIngressModePassthrough), OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})

	t.Run("http_bridge_mode_accepted", func(t *testing.T) {
		scheduler := &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: newRouterCfg()}}
		require.True(t, scheduler.isAccountTransportCompatible(accountWithMode(OpenAIWSIngressModeHTTPBridge), OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})

	t.Run("mode_off_rejected_even_with_payload_fallback", func(t *testing.T) {
		cfg := newRouterCfg()
		cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
		scheduler := &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: cfg}}
		require.False(t, scheduler.isAccountTransportCompatible(accountWithMode(OpenAIWSIngressModeOff), OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})

	t.Run("payload_fallback_widens_candidate_pool", func(t *testing.T) {
		cfg := newRouterCfg()
		cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
		scheduler := &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: cfg}}
		require.True(t, scheduler.isAccountTransportCompatible(accountWithMode(OpenAIWSIngressModeCtxPool), OpenAIUpstreamTransportResponsesHTTPBridgeIngress),
			"启用 payload 桥回退后 ctx_pool 账号是合法桥候选")
		require.True(t, scheduler.isAccountTransportCompatible(accountWithMode(OpenAIWSIngressModePassthrough), OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})

	t.Run("account_force_http_accepted", func(t *testing.T) {
		scheduler := &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: newRouterCfg()}}
		account := accountWithMode(OpenAIWSIngressModeCtxPool)
		account.Extra["openai_ws_force_http"] = true
		require.True(t, scheduler.isAccountTransportCompatible(account, OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})

	t.Run("account_force_http_respects_mode_off_gate", func(t *testing.T) {
		scheduler := &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: newRouterCfg()}}
		account := accountWithMode(OpenAIWSIngressModeOff)
		account.Extra["openai_ws_force_http"] = true
		require.False(t, scheduler.isAccountTransportCompatible(account, OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})

	t.Run("global_force_http_accepted", func(t *testing.T) {
		cfg := newRouterCfg()
		cfg.Gateway.OpenAIWS.ForceHTTP = true
		scheduler := &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: cfg}}
		require.True(t, scheduler.isAccountTransportCompatible(accountWithMode(OpenAIWSIngressModeCtxPool), OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})

	t.Run("grok_accepted_without_ws_gate", func(t *testing.T) {
		scheduler := &defaultOpenAIAccountScheduler{service: &OpenAIGatewayService{cfg: newRouterCfg()}}
		grok := &Account{ID: 8802, Platform: PlatformGrok, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1}
		require.True(t, scheduler.isAccountTransportCompatible(grok, OpenAIUpstreamTransportResponsesHTTPBridgeIngress))
	})
}

type unavailableOpenAIWSOwnerStore struct{ OpenAIWSStateStore }

func (s *unavailableOpenAIWSOwnerStore) GetHTTPResponseOwner(context.Context, int64, string) (int64, int64, bool, error) {
	return 0, 0, false, errors.New("owner store unavailable")
}

func TestOpenAIWSIngress_OwnerStoreFailureNeverForwardsUnverifiedParent(t *testing.T) {
	for _, mode := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		t.Run(mode, func(t *testing.T) {
			cfg := newOpenAIWSOwnershipConfig()
			cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			dialer := &openAIWSQueueDialer{}
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			svc := &OpenAIGatewayService{
				cfg: cfg, cache: &stubGatewayCache{}, httpUpstream: &httpUpstreamRecorder{},
				openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(),
				openaiWSPool: pool, openaiWSPassthroughDialer: dialer,
			}
			t.Cleanup(svc.CloseOpenAIWSPool)
			svc.openaiWSStateStore = &unavailableOpenAIWSOwnerStore{svc.getOpenAIWSStateStore()}
			account := newOpenAIWSOwnershipAPIKeyAccount(711)
			account.Extra = map[string]any{"openai_apikey_responses_websockets_v2_mode": mode}
			server := newOpenAIWSOwnershipServerWithAuth(t, svc, account, "", &APIKey{ID: 51, UserID: 41}, false)
			client := server.dial(t)
			defer func() { _ = client.CloseNow() }()
			openAIWSOwnershipWrite(t, client, `{"type":"response.create","model":"gpt-5.1","store":true,"previous_response_id":"resp_unverified","input":"continue"}`)
			event := openAIWSOwnershipRead(t, client)
			require.Equal(t, "response_owner_unavailable", gjson.GetBytes(event, "error.code").String())
			require.EqualValues(t, 503, gjson.GetBytes(event, "status").Int())
			require.Zero(t, dialer.DialCount(), "unverified lineage must never reach an upstream account")
		})
	}
}
