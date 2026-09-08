package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func normalizeOpenAIResponsesWebSocketCompatibilityBodyForMode(body []byte, account *Account, lite, native bool) ([]byte, bool, error) {
	if native {
		return body, false, nil
	}
	return normalizeOpenAIResponsesWebSocketCompatibilityBody(body, account, lite)
}

func newOpenAIWSNativeTurn(number int, payload, original []byte, requestModel string, headers http.Header, reverse map[string]string) *openAIWSNativeTurn {
	upstreamModel := strings.TrimSpace(gjson.GetBytes(payload, "model").String())
	turn := &openAIWSNativeTurn{
		number: number, payload: payload, requestModel: requestModel, upstreamModel: upstreamModel, toolReverse: reverse,
		result: OpenAIForwardResult{
			Model: requestModel, UpstreamModel: openAIWSDifferentModel(requestModel, upstreamModel),
			ServiceTier:              extractOpenAIServiceTierFromBody(payload),
			ReasoningEffort:          extractOpenAIReasoningEffortFromBody(payload, upstreamModel, requestModel),
			RequestedReasoningEffort: CanonicalRequestedReasoningEffort(original, requestModel, upstreamModel),
			Stream:                   true, OpenAIWSMode: true, ResponseHeaders: cloneHeader(headers), UpstreamHeaders: cloneHeader(headers),
		},
	}
	if imageIntent := IsImageGenerationIntentForPlatform(openAIResponsesEndpoint, requestModel, payload, PlatformOpenAI); imageIntent {
		if imageConfig, err := resolveOpenAIResponsesImageBillingConfigDetailedFromBody(payload, requestModel); err == nil {
			turn.result.BillingModel = imageConfig.Model
			turn.result.ImageSize = imageConfig.SizeTier
			turn.result.ImageInputSize = imageConfig.InputSize
		}
	}
	return turn
}

func (s *OpenAIGatewayService) proxyOpenAIWSNative(ctx context.Context, c *gin.Context, clientConn *coderws.Conn, upstream openaiwsv2.FrameConn, account *Account, firstPayload []byte, requestModel string, originalFirst []byte, headers http.Header, hooks *OpenAIWSIngressHooks) error {
	first := newOpenAIWSNativeTurn(1, firstPayload, originalFirst, requestModel, headers, codexToolNameReverseFromContext(c))
	first.startedAt = hooks.InitialTurnStartedAt
	client := &openAIWSClientFrameConn{conn: clientConn, physicalClient: openAIWSPhysicalClientFromGin(c, clientConn), writeTimeout: s.openAIWSWriteTimeout()}
	options := openAIWSNativeOptions{
		writeTimeout: s.openAIWSWriteTimeout(), idleTimeout: s.openAIWSIngressInterTurnIdleTimeout(),
		activeReadTimeout: s.openAIWSPassthroughIdleTimeout(), lifetime: time.Hour,
		beforeTurn: hooks.BeforeTurn, continueTurn: hooks.ContinueTurn, releaseTurn: hooks.ReleaseTurn,
		firstOutputTimeout: func(turn *openAIWSNativeTurn) time.Duration {
			effort := ""
			if turn.result.ReasoningEffort != nil {
				effort = *turn.result.ReasoningEffort
			}
			return s.openAIWSPassthroughFirstOutputTimeout(effort)
		},
		validatePrevious: func(id string) error {
			key := getAPIKeyFromContext(c)
			if key == nil || key.ID <= 0 || key.UserID <= 0 {
				return newOpenAIWSRequestError(400, "previous_response_not_found", "Previous response is not available on this connection.", "previous_response_id")
			}
			owned, err := s.ValidateOpenAIHTTPResponseOwner(ctx, getOpenAIGroupIDFromContext(c), id, key.UserID, key.ID)
			if err != nil {
				return newOpenAIWSRequestError(503, "response_owner_unavailable", "Response ownership could not be verified.", "previous_response_id")
			}
			if !owned {
				return newOpenAIWSRequestError(400, "previous_response_not_found", "Previous response is not available on this connection.", "previous_response_id")
			}
			return nil
		},
		validateSteer: func(turn *openAIWSNativeTurn, payload []byte) error {
			if hooks.BeforeRequest == nil {
				return nil
			}
			// Control admission audits only the new input with the admitted
			// parent model, and must not replace that response's billing body.
			return hooks.BeforeRequest(turn.number, payload, turn.requestModel)
		},
		prepare: func(number int, payload []byte) (*openAIWSNativeTurn, error) {
			if hooks.ObserveClientResponseCreate != nil {
				hooks.ObserveClientResponseCreate(payload)
			}
			original := payload
			model := strings.TrimSpace(gjson.GetBytes(payload, "model").String())
			if model == "" {
				model = requestModel
			}
			if hooks.BeforeRequest != nil {
				if err := hooks.BeforeRequest(number, payload, model); err != nil {
					return nil, err
				}
			}
			var err error
			// The resolved client model scopes group effort mappings even when
			// a compatible client omitted it on this response.create frame.
			payload = s.ReplaceModelInBody(payload, model)
			payload, err = applyOpenAIWSReasoningEffortPolicy(payload, hooks)
			if err != nil {
				return nil, err
			}
			mapped := model
			if hooks.MapRequestModel != nil {
				mapped, err = hooks.MapRequestModel(number, model)
				if err != nil {
					return nil, err
				}
				if strings.TrimSpace(mapped) == "" {
					mapped = model
				}
			}
			payload = s.ReplaceModelInBody(payload, mapped)
			var reverse map[string]string
			if account.IsOpenAIOAuthLike() {
				payload, reverse, _, err = aliasOpenAIOAuthReservedToolNamesBody(payload)
				if err != nil {
					return nil, err
				}
			}
			payload, _, err = applyCodexAccountIdentityClientMetadataRaw(payload, codexAccountIdentitySource(c, account), getAPIKeyIDFromContext(c))
			if err != nil {
				return nil, err
			}
			var blocked *OpenAIFastBlockedError
			payload, blocked, err = s.applyOpenAIFastPolicyToWSResponseCreate(ctx, account, mapped, payload)
			if err != nil {
				return nil, err
			}
			if blocked != nil {
				turnContext := c
				if hooks.NativeTurnContext != nil {
					turnContext = hooks.NativeTurnContext(number)
				}
				MarkOpsClientBusinessLimited(turnContext, OpsClientBusinessLimitedReasonLocalPolicyDenied)
				return nil, blocked
			}
			return newOpenAIWSNativeTurn(number, payload, original, model, headers, reverse), nil
		},
		observeUpstream: func(turn *openAIWSNativeTurn, payload []byte) {
			turnContext := c
			if hooks.NativeTurnContext != nil {
				turnContext = hooks.NativeTurnContext(turn.number)
			}
			SetOpsUpstreamModel(turnContext, turn.upstreamModel)
			s.bindOpenAIWSResponseOwnerBeforeWrite(ctx, turnContext, account.ID, payload)
			kind := gjson.GetBytes(payload, "type").String()
			markOpenAIWSClientVisibleFailure(turnContext, kind, payload)
			if kind == "response.failed" || kind == "error" {
				if !markOpenAIWSV2PassthroughCyberPolicy(turnContext, payload) {
					model := gjson.GetBytes(payload, "response.model").String()
					if model == "" {
						model = turn.upstreamModel
					}
					s.handleOpenAIWSFailureAccountSideEffects(ctx, account, model, headers, payload)
				}
			}
		},
		afterTurn: func(number int, result *OpenAIForwardResult, err error) {
			if hooks.TurnStarted != nil && result != nil {
				hooks.TurnStarted(number, time.Now().Add(-result.Duration))
			}
			if hooks.AfterTurn != nil {
				hooks.AfterTurn(number, result, err)
			}
		},
	}
	err := runOpenAIWSNative(ctx, client, upstream, first, options)
	if err == nil {
		return nil
	}
	var closeErr *OpenAIWSClientCloseError
	if errors.As(err, &closeErr) {
		return closeErr
	}
	// Native writes and accepted steering have unknown outcomes on disconnect.
	// A typed client close prevents scheduler failover from replaying the input.
	return NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "native websocket disconnected; reconcile response state before retry", err)
}
