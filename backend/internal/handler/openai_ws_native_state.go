package handler

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

const openAIWSNativeModeHeader = "X-Sub2API-WebSocket-Mode"

const openAIWSNativeAuditBodyLimit = int64(256 << 20)

// Auditing requires the complete accepted input. Reserve capacity before
// copying; an over-budget request is rejected instead of losing audit context.
type openAIWSNativeAuditBodies struct {
	mu              sync.Mutex
	limit, retained int64
	bodies          map[int][]byte
}

func (s *openAIWSNativeAuditBodies) put(turn int, payload []byte) *service.OpenAIWSRequestError {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := s.limit
	if limit <= 0 {
		limit = openAIWSNativeAuditBodyLimit
	}
	previous := int64(len(s.bodies[turn]))
	if int64(len(payload)) > limit-(s.retained-previous) {
		return &service.OpenAIWSRequestError{Status: http.StatusTooManyRequests, Code: "websocket_audit_buffer_limit_reached", Message: "This connection's active request audit buffer is full; retry after an active response completes"}
	}
	if s.bodies == nil {
		s.bodies = make(map[int][]byte)
	}
	s.bodies[turn] = append([]byte(nil), payload...)
	s.retained += int64(len(payload)) - previous
	return nil
}

func (s *openAIWSNativeAuditBodies) take(turn int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	body := s.bodies[turn]
	delete(s.bodies, turn)
	s.retained -= int64(len(body))
	return body
}

func wantsOpenAIWSNativeResponses(c *gin.Context) bool {
	return c != nil && strings.EqualFold(strings.TrimSpace(c.GetHeader(openAIWSNativeModeHeader)), "native")
}

func (h *OpenAIGatewayHandler) openAIWSNativeAccountAllowed(account *service.Account) bool {
	if h == nil || h.cfg == nil || account == nil || !h.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled {
		return false
	}
	return account.ResolveOpenAIResponsesWebSocketV2Mode(h.cfg.Gateway.OpenAIWS.IngressModeDefault) == service.OpenAIWSIngressModePassthrough &&
		service.NewOpenAIWSProtocolResolver(h.cfg).Resolve(account).Transport == service.OpenAIUpstreamTransportResponsesWebsocketV2 &&
		!h.gatewayService.ShouldBridgeOpenAIWSIngress(account)
}

type openAIWSNativeTurn struct {
	requestModel                string
	streamID                    string
	userRelease, accountRelease func()
	imageRelease                func()
	pricingAt                   time.Time
	mapping                     service.ChannelMappingResult
	hasMapping                  bool
	payloadHash                 string
	completed                   bool
	admitted                    bool
	imageIntent, imageAdmitted  bool
	requestContext              *gin.Context
}

// Each response owns its admission slots and billing snapshot. Steering can
// transfer slots to a successor before the parent's terminal usage is recorded.
type openAIWSNativeTurns struct {
	latestCompleted map[string]int
	mu              sync.Mutex
	turns           map[int]openAIWSNativeTurn
}

func (s *openAIWSNativeTurns) update(turn int, fn func(*openAIWSNativeTurn)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turns == nil {
		s.turns = make(map[int]openAIWSNativeTurn)
	}
	state := s.turns[turn]
	fn(&state)
	s.turns[turn] = state
	// Only completed metadata is evicted. Active responses retain their slots.
	if len(s.turns) > 512 {
		for id, old := range s.turns {
			if old.completed && id < turn-256 && s.latestCompleted[old.streamID] != id {
				delete(s.turns, id)
			}
		}
	}
}

func (s *openAIWSNativeTurns) snapshot(turn int) openAIWSNativeTurn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turns[turn]
}

func (s *openAIWSNativeTurns) holdsSlots(turn int) bool {
	state := s.snapshot(turn)
	return state.admitted
}

func (s *openAIWSNativeTurns) context(turn int, base *gin.Context) *gin.Context {
	var ctx *gin.Context
	s.update(turn, func(state *openAIWSNativeTurn) {
		if state.requestContext == nil {
			state.requestContext = base.Copy()
			state.requestContext.Request = base.Request.Clone(base.Request.Context())
			// gin.Copy detaches the writer. Native callbacks only read response
			// headers for audit IDs and must retain that read-capable writer.
			state.requestContext.Writer = base.Writer
			service.BeginOpsStreamTurn(state.requestContext, turn)
			if state.requestModel != "" {
				setOpsRequestContext(state.requestContext, state.requestModel, true)
			}
		}
		ctx = state.requestContext
		if state.completed {
			// A steer against a completed parent may still need a temporary
			// admission context, but must not repopulate its retained metadata.
			state.requestContext = nil
		}
	})
	return ctx
}

func (s *openAIWSNativeTurns) adopt(turn int, user, account func()) {
	s.update(turn, func(state *openAIWSNativeTurn) {
		state.userRelease, state.accountRelease = user, account
		state.admitted = true
		state.completed = false
	})
}

func (s *openAIWSNativeTurns) transfer(previous, turn int, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent, ok := s.turns[previous]
	if !ok || !parent.admitted {
		return errors.New("steering parent has no active admission slots")
	}
	if next := s.turns[turn]; next.admitted {
		return errors.New("steering successor already owns admission slots")
	}
	next := parent
	next.pricingAt = at
	next.completed = false
	next.requestContext = nil
	parent.userRelease, parent.accountRelease = nil, nil
	parent.admitted = false
	parent.imageRelease, parent.imageAdmitted = nil, false
	s.turns[previous], s.turns[turn] = parent, next
	return nil
}

func (s *openAIWSNativeTurns) release(turn int) {
	s.mu.Lock()
	state := s.turns[turn]
	retained := state
	retained.userRelease, retained.accountRelease = nil, nil
	retained.imageRelease, retained.imageAdmitted = nil, false
	retained.completed = true
	retained.admitted = false
	// Completed responses retain only small steering/billing metadata, never
	// request contexts with moderation evidence or buffered body values.
	retained.requestContext = nil
	if s.turns != nil {
		s.turns[turn] = retained
	}
	s.mu.Unlock()
	if state.accountRelease != nil {
		state.accountRelease()
	}
	if state.userRelease != nil {
		state.userRelease()
	}
	if state.imageRelease != nil {
		state.imageRelease()
	}
}

func (s *openAIWSNativeTurns) complete(turn int) {
	s.mu.Lock()
	if s.latestCompleted == nil {
		s.latestCompleted = make(map[string]int)
	}
	state := s.turns[turn]
	s.latestCompleted[state.streamID] = turn
	s.mu.Unlock()
	s.release(turn)
}

func (s *openAIWSNativeTurns) releaseAll() {
	s.mu.Lock()
	states := s.turns
	s.turns = nil
	s.latestCompleted = nil
	s.mu.Unlock()
	for _, state := range states {
		if state.accountRelease != nil {
			state.accountRelease()
		}
		if state.userRelease != nil {
			state.userRelease()
		}
		if state.imageRelease != nil {
			state.imageRelease()
		}
	}
}
