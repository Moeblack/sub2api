package handler

import (
	"context"
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

func TestOpenAIWSNativeTurnsKeepConcurrentSlotsAndBillingSnapshots(t *testing.T) {
	var state openAIWSNativeTurns
	var first, second atomic.Int32
	firstAt, secondAt := time.Unix(100, 0), time.Unix(200, 0)
	state.adopt(1, func() { first.Add(1) }, func() { first.Add(1) })
	state.update(1, func(turn *openAIWSNativeTurn) {
		turn.pricingAt, turn.payloadHash = firstAt, "first-payload"
		turn.mapping, turn.hasMapping = service.ChannelMappingResult{MappedModel: "first-model"}, true
	})
	state.adopt(2, func() { second.Add(1) }, func() { second.Add(1) })
	state.update(2, func(turn *openAIWSNativeTurn) { turn.pricingAt = secondAt })
	state.release(2)
	require.EqualValues(t, 0, first.Load())
	require.EqualValues(t, 2, second.Load())
	require.True(t, state.holdsSlots(1))
	require.Equal(t, firstAt, state.snapshot(1).pricingAt)
	require.Equal(t, secondAt, state.snapshot(2).pricingAt)
	state.release(1)
	state.releaseAll()
	require.EqualValues(t, 2, first.Load())
	require.EqualValues(t, 2, second.Load())
}

func TestOpenAIWSNativeSteeringTransfersSlotsBeforeParentBilling(t *testing.T) {
	var state openAIWSNativeTurns
	var releases atomic.Int32
	parentAt, successorAt := time.Unix(100, 0), time.Unix(200, 0)
	state.adopt(1, func() { releases.Add(1) }, func() { releases.Add(1) })
	state.update(1, func(turn *openAIWSNativeTurn) {
		turn.pricingAt, turn.payloadHash = parentAt, "original-call-payload"
		turn.mapping, turn.hasMapping = service.ChannelMappingResult{MappedModel: "priced-alias"}, true
	})
	require.NoError(t, state.transfer(1, 3, successorAt))
	require.False(t, state.holdsSlots(1))
	require.True(t, state.holdsSlots(3))
	state.release(1)
	require.Zero(t, releases.Load(), "parent completion must not release successor capacity")
	require.Equal(t, parentAt, state.snapshot(1).pricingAt)
	require.Equal(t, successorAt, state.snapshot(3).pricingAt)
	require.Equal(t, "priced-alias", state.snapshot(3).mapping.MappedModel)
	require.Equal(t, "original-call-payload", state.snapshot(1).payloadHash)
	state.release(3)
	state.release(3)
	state.releaseAll()
	require.EqualValues(t, 2, releases.Load(), "failed or unused reservation releases exactly once")
}

func TestOpenAIWSNativeTurnContextsKeepCyberPolicyWithOwner(t *testing.T) {
	base, _ := gin.CreateTestContext(httptest.NewRecorder())
	base.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	var states openAIWSNativeTurns
	first, second := states.context(1, base), states.context(2, base)
	service.MarkOpsCyberPolicy(second, service.CyberPolicyMark{Message: "blocked second response", UpstreamInTok: 17})
	service.BeginOpsStreamTurn(second, 2)
	service.MarkOpsStreamFailure(second, "invalid_request_error", "cyber_policy", "blocked second response", http.StatusBadRequest)
	require.Nil(t, service.GetOpsCyberPolicy(first), "other lane must never inherit blocked usage")
	require.Empty(t, service.GetOpsStreamErrors(first))
	require.Equal(t, 17, service.GetOpsCyberPolicy(second).UpstreamInTok)
	require.Nil(t, service.GetOpsCyberPolicy(base), "connection context must remain independent")
	states.adopt(2, func() {}, func() {})
	require.NoError(t, states.transfer(2, 3, time.Now()))
	require.Nil(t, service.GetOpsCyberPolicy(states.context(3, base)), "successor has separate moderation state")
	states.releaseAll()
}

func TestOpenAIWSNativeSteeringTransfersAndReleasesImageSlot(t *testing.T) {
	var states openAIWSNativeTurns
	var released atomic.Int32
	states.adopt(1, nil, nil)
	states.update(1, func(turn *openAIWSNativeTurn) {
		turn.imageIntent, turn.imageAdmitted = true, true
		turn.imageRelease = func() { released.Add(1) }
	})
	require.NoError(t, states.transfer(1, 2, time.Now()))
	states.release(1)
	require.Zero(t, released.Load())
	require.True(t, states.snapshot(2).imageAdmitted)
	states.release(2)
	states.releaseAll()
	require.EqualValues(t, 1, released.Load())
}

func TestOpenAIWSNativeSnapshotsRetainLatestIdleLane(t *testing.T) {
	var states openAIWSNativeTurns
	states.update(1, func(turn *openAIWSNativeTurn) {
		turn.streamID, turn.hasMapping = "idle-lane", true
	})
	states.complete(1)
	for id := 2; id < 1100; id++ {
		states.update(id, func(turn *openAIWSNativeTurn) { turn.streamID = "busy-lane" })
		states.complete(id)
	}
	require.True(t, states.snapshot(1).hasMapping, "latest response on an idle lane must remain steerable")
	require.LessOrEqual(t, len(states.turns), 513, "historical snapshots remain bounded")
}

func TestOpenAIWSNativeAuditBodyBudgetRejectsBeforeCopyAndReleases(t *testing.T) {
	store := openAIWSNativeAuditBodies{limit: 12}
	first := []byte("12345678")
	require.Nil(t, store.put(1, first))
	first[0] = 'x'
	require.Equal(t, "12345678", string(store.bodies[1]), "accepted audit input is immutable")
	denied := store.put(2, []byte("abcde"))
	require.NotNil(t, denied)
	require.Equal(t, http.StatusTooManyRequests, denied.Status)
	require.Equal(t, "websocket_audit_buffer_limit_reached", denied.Code)
	require.EqualValues(t, 8, store.retained)
	require.NotContains(t, store.bodies, 2)
	require.NotNil(t, store.put(1, []byte("1234567890123")))
	require.Equal(t, "12345678", string(store.bodies[1]), "rejected replacement preserves original audit input")
	require.Nil(t, store.put(2, []byte("abcd")))
	require.EqualValues(t, 12, store.retained)
	require.Equal(t, "12345678", string(store.take(1)))
	require.EqualValues(t, 4, store.retained)
	require.Nil(t, store.take(1), "terminal/rejection cleanup is idempotent")
	require.EqualValues(t, 4, store.retained)
	require.Nil(t, store.put(2, []byte("12345678901")), "replacement charges only the new retained size")
	require.EqualValues(t, 11, store.retained)
	require.Equal(t, "12345678901", string(store.take(2)))
	require.Zero(t, store.retained)
	require.Nil(t, store.put(3, []byte("123456789012")), "released capacity is reusable")
}

func TestOpenAIWSNativeCompletedMetadataDropsBodyBearingContext(t *testing.T) {
	base, _ := gin.CreateTestContext(httptest.NewRecorder())
	base.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	base.Writer.Header().Set("X-Request-Id", "audit-id")
	var states openAIWSNativeTurns
	for _, turn := range []int{1, 2} {
		ctx := states.context(turn, base)
		ctx.Set(gin.BodyBytesKey, []byte("complete-original-audit-body"))
		service.MarkOpsCyberPolicy(ctx, service.CyberPolicyMark{Body: "upstream-evidence"})
		require.Equal(t, "audit-id", ctx.Writer.Header().Get("X-Request-Id"), "audit callbacks retain readable response headers")
		states.update(turn, func(state *openAIWSNativeTurn) { state.payloadHash = "retained-hash" })
		if turn == 1 {
			states.complete(turn)
		} else {
			states.release(turn)
		}
		require.Nil(t, states.snapshot(turn).requestContext, "completed or rejected metadata must not retain full bodies")
		require.Equal(t, "retained-hash", states.snapshot(turn).payloadHash)
		states.context(turn, base).Set(gin.BodyBytesKey, []byte("late-steering-audit-body"))
		require.Nil(t, states.snapshot(turn).requestContext, "late steering may use only a temporary completed-parent context")
	}
}

func TestOpenAIWSNativeModeRespectsAdminTransportRestrictions(t *testing.T) {
	for _, test := range []struct {
		name                    string
		mode                    string
		force, enabled, allowed bool
	}{
		{name: "passthrough", mode: service.OpenAIWSIngressModePassthrough, enabled: true, allowed: true},
		{name: "off", mode: service.OpenAIWSIngressModeOff, enabled: true},
		{name: "http bridge", mode: service.OpenAIWSIngressModeHTTPBridge, enabled: true},
		{name: "context pool", mode: service.OpenAIWSIngressModeCtxPool, enabled: true},
		{name: "global force HTTP", mode: service.OpenAIWSIngressModePassthrough, force: true, enabled: true},
		{name: "global off", mode: service.OpenAIWSIngressModePassthrough},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.Enabled = test.enabled
			cfg.Gateway.OpenAIWS.APIKeyEnabled = true
			cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
			cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			cfg.Gateway.OpenAIWS.ForceHTTP = test.force
			h := &OpenAIGatewayHandler{cfg: cfg}
			account := &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Concurrency: 2,
				Extra: map[string]any{"openai_apikey_responses_websockets_v2_mode": test.mode}}
			require.Equal(t, test.allowed, h.openAIWSNativeAccountAllowed(account))
		})
	}
}

func TestOpenAIResponsesAndWarmupEnforceAllowlistWithoutMiddleware(t *testing.T) {
	for _, warmup := range []bool{false, true} {
		t.Run(map[bool]string{false: "response", true: "warmup"}[warmup], func(t *testing.T) {
			body := []byte(`{"model":"gpt-5.4","Model":"forbidden-model","generate":false}`)
			writer := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(writer)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
			c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{Group: wsAllowlistGroup(true, "gpt-5.4")})
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1})
			h := &OpenAIGatewayHandler{gatewayService: &service.OpenAIGatewayService{}, billingCacheService: &service.BillingCacheService{}, apiKeyService: &service.APIKeyService{}, concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(nil), SSEPingFormatNone, time.Second)}
			if warmup {
				err := h.validateOpenAIWSWarmup(c, body)
				var denied *service.OpenAIWSRequestError
				require.ErrorAs(t, err, &denied)
				require.Equal(t, http.StatusNotFound, denied.Status)
				return
			}
			h.Responses(c)
			require.Equal(t, http.StatusNotFound, writer.Code)
			require.Contains(t, writer.Body.String(), "forbidden-model")
		})
	}
}

func TestOpenAIWSBridgeChecksRawAndEffectiveModelCandidates(t *testing.T) {
	for _, raw := range []string{
		`{"type":"response.create","model":"forbidden-model","input":[]}`,
		`{"type":"response.create","model":"forbidden-model","model":"gpt-5.4","input":[]}`,
		`{"type":"response.create","Model":"forbidden-model","model":"gpt-5.4","input":[]}`,
		`{"type":"response.create","model":"forbidden-model","model":"gpt-5.4","generate":false,"input":[]}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var calls atomic.Int32
			server, usage, _ := newIsolationBridgeServerWithGroup(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}), wsAllowlistGroup(true, "gpt-5.4"))
			conn := dialIsolationBridge(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(raw)))
			_, event, err := conn.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, "error", gjson.GetBytes(event, "type").String(), string(event))
			require.Contains(t, gjson.GetBytes(event, "error.message").String(), "forbidden-model")
			require.Zero(t, calls.Load())
			select {
			case <-usage:
				t.Fatal("rejected model must not create billed usage")
			default:
			}
		})
	}

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	require.NotNil(t, validateOpenAIWSModelAllowlist(c, wsAllowlistGroup(true, "gpt-5.4"), []byte(`{"type":"response.create"}`), []byte(`{"model":"forbidden-model"}`)))
}
