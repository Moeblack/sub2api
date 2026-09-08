package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requestmodel"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

// The bridge executes each response through the existing HTTP Responses path.
// base is captured before account selection or per-turn state is installed.
// Security, bounded admission, retries and billing remain execution-local.
func (h *OpenAIGatewayHandler) runOpenAIWSHTTPConnection(c, base *gin.Context, conn *coderws.Conn, first []byte) {
	connectionCtx, releaseClientContext := h.gatewayService.OpenAIWSClientConnectionContext(base.Request.Context(), c, conn)
	defer releaseClientContext()
	base = base.Copy()
	base.Request = base.Request.Clone(connectionCtx)
	apiKey, _ := middleware2.GetAPIKeyFromContext(base)
	subject, subjectOK := middleware2.GetAuthSubjectFromContext(base)
	reqLog := requestLogger(c, "handler.openai_gateway.responses_ws")
	var opsMu sync.Mutex
	recordErrors := func(execution *gin.Context, sequence int) {
		if h.opsService != nil {
			for _, streamError := range service.GetOpsStreamErrors(execution) {
				streamError.Turn = sequence
				logOpsStreamErrorValue(execution, h.opsService, http.StatusSwitchingProtocols, streamError)
			}
			return
		}
		// Outer middleware reads immutable snapshots after workers join.
		opsMu.Lock()
		defer opsMu.Unlock()
		errorsForRequest := service.GetOpsStreamErrors(c)
		for _, streamError := range service.GetOpsStreamErrors(execution) {
			streamError.Turn = sequence
			if len(errorsForRequest) < 64 {
				errorsForRequest = append(errorsForRequest, streamError)
			}
		}
		if len(errorsForRequest) > 0 {
			c.Set(service.OpsStreamErrorsKey, errorsForRequest)
		}
	}
	options := service.OpenAIWSConnectionOptions{
		FirstMessageTimeout: service.ResolveOpenAIWSClientFirstMessageTimeout(h.cfg),
		NormalizeRequest:    service.NormalizeCodexStandaloneInputs,
		ObserveRequestError: func(request service.OpenAIWSConnectionRequest, requestErr *service.OpenAIWSRequestError) {
			execution := base.Copy()
			ctx := context.WithValue(base.Request.Context(), ctxkey.RequestID, uuid.NewString())
			execution.Request = base.Request.Clone(ctx)
			setOpsRequestContext(execution, gjson.GetBytes(request.Payload, "model").String(), true)
			setOpsEndpointContext(execution, "", int16(service.RequestTypeWSV2))
			service.BeginOpsStreamTurn(execution, request.Sequence)
			errorType := "invalid_request_error"
			switch requestErr.Status {
			case http.StatusUnauthorized:
				errorType = "authentication_error"
			case http.StatusForbidden:
				errorType = "permission_error"
			case http.StatusTooManyRequests:
				errorType = "rate_limit_error"
			default:
				if requestErr.Status >= http.StatusInternalServerError {
					errorType = "server_error"
				}
			}
			service.MarkOpsStreamFailure(execution, errorType, requestErr.Code, requestErr.Message, requestErr.Status)
			reqLog.Info("openai.websocket_request_rejected",
				zap.String("connection_id", request.ConnectionID),
				zap.String("stream_id", request.StreamID),
				zap.Int("execution_sequence", request.Sequence),
				zap.Int("status", requestErr.Status),
				zap.String("error_code", requestErr.Code),
				zap.String("reason", requestErr.Message),
			)
			recordErrors(execution, request.Sequence)
		},
		ValidateWarmup: func(ctx context.Context, request service.OpenAIWSConnectionRequest) error {
			execution := base.Copy()
			execution.Request = base.Request.Clone(ctx)
			if apiKey != nil {
				if denied := validateOpenAIWSModelAllowlist(execution, apiKey.Group, request.RawPayload, request.Payload); denied != nil {
					return denied
				}
			}
			return h.validateOpenAIWSWarmup(execution, request.Payload)
		},
		ObserveClosed: func(outcome service.OpenAIWSConnectionOutcome) {
			reqLog.Info("openai.websocket_connection_closed",
				zap.String("connection_id", outcome.ConnectionID),
				zap.String("close_cause", outcome.Cause),
				zap.Int("close_code", outcome.CloseCode),
				zap.Int("request_count", outcome.Requests),
				zap.Error(outcome.CloseError),
			)
		},
		ResolvePersisted: func(ctx context.Context, responseID string) bool {
			if apiKey == nil || !subjectOK {
				return false
			}
			groupID := int64(0)
			if apiKey.GroupID != nil {
				groupID = *apiKey.GroupID
			}
			owned, err := h.gatewayService.ValidateOpenAIHTTPResponseOwner(ctx, groupID, responseID, subject.UserID, apiKey.ID)
			return err == nil && owned
		},
	}
	if h.cfg != nil {
		options.IdleTimeout = time.Duration(h.cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds) * time.Second
		options.WriteTimeout = time.Duration(h.cfg.Gateway.OpenAIWS.WriteTimeoutSeconds) * time.Second
	}
	err := service.RunOpenAIWSConnection(base.Request.Context(), conn, first, options,
		func(ctx context.Context, request service.OpenAIWSConnectionRequest, emit func([]byte) error) error {
			executionID := uuid.NewString()
			ctx = context.WithValue(ctx, ctxkey.RequestID, executionID)
			execution := base.Copy()
			execution.Request = base.Request.Clone(ctx)
			if apiKey != nil {
				if denied := validateOpenAIWSModelAllowlist(execution, apiKey.Group, request.RawPayload, request.Payload); denied != nil {
					return denied
				}
			}
			execution.Request.Method = http.MethodPost
			execution.Request.Body = io.NopCloser(bytes.NewReader(request.Payload))
			execution.Request.ContentLength = int64(len(request.Payload))
			execution.Request.Header.Del("Upgrade")
			execution.Request.Header.Del("Connection")
			execution.Request.Header.Del("Content-Length")
			execution.Request.Header.Set("Content-Type", "application/json")
			writer := newOpenAIWSHTTPResponseWriter(ctx, emit, service.ResolveOpenAIWSClientReadLimitBytes(h.cfg))
			execution.Writer = writer
			defer writer.stop()
			service.BeginOpsStreamTurn(execution, request.Sequence)
			reqLog.Info("openai.websocket_execution_started",
				zap.String("connection_id", request.ConnectionID),
				zap.String("execution_id", executionID),
				zap.String("stream_id", request.StreamID),
				zap.Int("execution_sequence", request.Sequence),
				zap.String("client_frame_sha256", service.HashUsageRequestPayload(request.RawPayload)),
			)
			h.Responses(execution)
			if status, errorType, code, message := writer.pendingHTTPError(); status >= http.StatusBadRequest && len(service.GetOpsStreamErrors(execution)) == 0 {
				service.MarkOpsStreamFailure(execution, errorType, code, message, status)
			}
			finishErr := writer.finish()
			recordErrors(execution, request.Sequence)
			return finishErr
		})
	if err != nil && base.Request.Context().Err() == nil {
		reqLog.Warn("openai.websocket_connection_ended", zap.Error(err))
	}
}

func (h *OpenAIGatewayHandler) recordOpenAIWSNativeTurnOps(connection, execution *gin.Context) {
	for _, streamError := range service.GetOpsStreamErrors(execution) {
		if h.opsService != nil {
			logOpsStreamErrorValue(execution, h.opsService, http.StatusSwitchingProtocols, streamError)
			continue
		}
		errorsForRequest := service.GetOpsStreamErrors(connection)
		if len(errorsForRequest) < 64 {
			errorsForRequest = append(errorsForRequest, streamError)
			connection.Set(service.OpsStreamErrorsKey, errorsForRequest)
			connection.Set(service.OpsStreamErrorKey, errorsForRequest[len(errorsForRequest)-1])
		}
	}
}

func (h *OpenAIGatewayHandler) useOpenAIWSHTTPConnectionRuntime() bool {
	return h != nil && h.cfg != nil && h.cfg.Gateway.OpenAIWS.ForceHTTP
}

// Raw frames can contain duplicate or case-variant model keys that are lost
// during bridge materialization. Effective payloads also carry inherited models.
func validateOpenAIWSModelAllowlist(c *gin.Context, group *service.Group, payloads ...[]byte) *service.OpenAIWSRequestError {
	for _, payload := range payloads {
		if blocked := blockedModelAllowlistCandidate(group, requestmodel.FromBodyCandidates("", "application/json", payload)); blocked != "" {
			service.MarkOpsClientBusinessLimited(c, service.OpsClientBusinessLimitedReasonLocalModelConfiguration)
			middleware2.MarkIngressRejected(c, middleware2.IngressRejectModelNotAllowed)
			return &service.OpenAIWSRequestError{Status: http.StatusNotFound, Code: "model_not_found", Message: fmt.Sprintf("Model %q is not available for this group", blocked), Param: "model"}
		}
	}
	return nil
}

// Warmup performs admission checks, including the existing request-rate gate,
// but never selects an account, invokes moderation inference, or records usage.
func (h *OpenAIGatewayHandler) validateOpenAIWSWarmup(c *gin.Context, payload []byte) error {
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		return &service.OpenAIWSRequestError{Status: 401, Code: "authentication_error", Message: "Invalid API key"}
	}
	if _, ok := middleware2.GetAuthSubjectFromContext(c); !ok {
		return &service.OpenAIWSRequestError{Status: 401, Code: "authentication_error", Message: "User context not found"}
	}
	if denied := validateOpenAIWSModelAllowlist(c, apiKey.Group, payload); denied != nil {
		return denied
	}
	model := gjson.GetBytes(payload, "model").String()
	ensureCompositeTargetPlatform(c, apiKey, model)
	if !openAICompatibleTextTargetAllowed(c, apiKey, model) {
		return &service.OpenAIWSRequestError{Status: 400, Code: "model_not_supported", Message: "Model is not supported by this endpoint", Param: "model"}
	}
	if _, _, err := applyOpenAIReasoningEffortPolicyForRequest(c, apiKey, payload); err != nil {
		return &service.OpenAIWSRequestError{Status: 403, Code: "permission_error", Message: err.Error(), Param: "reasoning"}
	}
	if _, err := service.ValidateOpenAIServiceTierField(payload); err != nil {
		return &service.OpenAIWSRequestError{Status: 400, Code: "invalid_service_tier", Message: err.Error(), Param: "service_tier"}
	}
	if service.IsExplicitImageGenerationIntent("/v1/responses", model, payload) && !service.GroupAllowsImageGeneration(apiKey.Group) {
		return &service.OpenAIWSRequestError{Status: 403, Code: "permission_error", Message: service.ImageGenerationPermissionMessage()}
	}
	if key := findBlockedCyberSessionKey(c.Request.Context(), h.gatewayService, apiKey.ID, c, payload); key != "" {
		h.enqueueCyberSessionBlockedOpsEntry(c, apiKey, model, key)
		return &service.OpenAIWSRequestError{Status: 403, Code: "session_blocked_by_cyber_policy", Message: cyberSessionBlockedClientMsg}
	}
	if err := h.gatewayService.ValidateOpenAIWSWarmupModel(c.Request.Context(), apiKey.GroupID, model); err != nil {
		return err
	}
	subscription, _ := middleware2.GetSubscriptionFromContext(c)
	if err := h.billingCacheService.CheckBillingEligibility(c.Request.Context(), apiKey.User, apiKey, apiKey.Group, subscription, service.QuotaPlatform(c.Request.Context(), apiKey)); err != nil {
		status, code, message, _ := billingErrorDetails(err)
		return &service.OpenAIWSRequestError{Status: status, Code: code, Message: message}
	}
	return nil
}

// This writer has no reference to the hijacked connection's HTTP writer.
// Header belongs to the HTTP execution; the socket sees only Responses events.
type openAIWSHTTPResponseWriter struct {
	mu        sync.Mutex
	header    http.Header
	status    int
	size      int
	buffer    []byte
	limit     int64
	streaming bool
	emit      func([]byte) error
	err       error
	terminal  string
	closed    chan bool
	stop      func() bool
}

var _ gin.ResponseWriter = (*openAIWSHTTPResponseWriter)(nil)

func newOpenAIWSHTTPResponseWriter(ctx context.Context, emit func([]byte) error, limit int64) *openAIWSHTTPResponseWriter {
	writer := &openAIWSHTTPResponseWriter{header: make(http.Header), status: http.StatusOK, size: -1, limit: limit, emit: emit, closed: make(chan bool, 1)}
	writer.stop = context.AfterFunc(ctx, func() { writer.closed <- true })
	return writer
}

func (w *openAIWSHTTPResponseWriter) Header() http.Header { return w.header }
func (w *openAIWSHTTPResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.size < 0 {
		w.status = status
	}
}
func (w *openAIWSHTTPResponseWriter) WriteHeaderNow() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.size < 0 {
		w.size = 0
	}
}
func (w *openAIWSHTTPResponseWriter) Status() int              { w.mu.Lock(); defer w.mu.Unlock(); return w.status }
func (w *openAIWSHTTPResponseWriter) Size() int                { w.mu.Lock(); defer w.mu.Unlock(); return w.size }
func (w *openAIWSHTTPResponseWriter) Written() bool            { return w.Size() >= 0 }
func (w *openAIWSHTTPResponseWriter) Flush()                   { w.WriteHeaderNow() }
func (w *openAIWSHTTPResponseWriter) CloseNotify() <-chan bool { return w.closed }
func (w *openAIWSHTTPResponseWriter) Pusher() http.Pusher      { return nil }
func (w *openAIWSHTTPResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errors.New("cannot hijack a WebSocket response execution")
}
func (w *openAIWSHTTPResponseWriter) WriteString(value string) (int, error) {
	return w.Write([]byte(value))
}
func (w *openAIWSHTTPResponseWriter) terminalEvent() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.terminal
}
func (w *openAIWSHTTPResponseWriter) pendingHTTPError() (int, string, string, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	errorType := gjson.GetBytes(w.buffer, "error.type").String()
	code := gjson.GetBytes(w.buffer, "error.code").String()
	message := gjson.GetBytes(w.buffer, "error.message").String()
	if errorType == "" {
		errorType = "invalid_request_error"
	}
	if code == "" {
		code = gjson.GetBytes(w.buffer, "code").String()
	}
	if message == "" {
		message = gjson.GetBytes(w.buffer, "message").String()
	}
	if message == "" {
		message = http.StatusText(w.status)
	}
	return w.status, errorType, code, message
}

// The caller holds mu. Preserve upstream JSON verbatim unless its SSE event
// name supplies the type; never turn comments or [DONE] into protocol events.
func (w *openAIWSHTTPResponseWriter) emitEvent(payload []byte, eventName string) error {
	eventType := gjson.GetBytes(payload, "type").String()
	if eventType == "" && eventName != "" {
		eventType = eventName
		var err error
		payload, err = sjson.SetBytes(payload, "type", eventType)
		if err != nil {
			return err
		}
	}
	switch eventType {
	case "response.completed", "response.done", "response.failed", "response.incomplete", "error":
		w.terminal = eventType
	}
	return w.emit(payload)
}

func (w *openAIWSHTTPResponseWriter) Write(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	if w.size < 0 {
		w.size = 0
	}
	w.size += len(payload)
	if int64(len(w.buffer))+int64(len(payload)) > w.limit {
		w.err = errors.New("upstream response exceeds the WebSocket bridge buffer limit")
		return 0, w.err
	}
	w.buffer = append(w.buffer, payload...)
	if !w.streaming {
		prefix := bytes.TrimSpace(w.buffer)
		w.streaming = bytes.HasPrefix(prefix, []byte("data:")) || bytes.HasPrefix(prefix, []byte("event:")) || bytes.HasPrefix(prefix, []byte(":"))
	}
	if w.streaming {
		w.err = w.drainSSE()
	}
	return len(payload), w.err
}

func (w *openAIWSHTTPResponseWriter) drainSSE() error {
	for {
		end := bytes.Index(w.buffer, []byte("\n\n"))
		separator := 2
		if crlf := bytes.Index(w.buffer, []byte("\r\n\r\n")); crlf >= 0 && (end < 0 || crlf < end) {
			end = crlf
			separator = 4
		}
		if end < 0 {
			return nil
		}
		event := w.buffer[:end]
		w.buffer = w.buffer[end+separator:]
		var data []byte
		var eventName string
		for _, line := range bytes.Split(event, []byte("\n")) {
			line = bytes.TrimSuffix(line, []byte("\r"))
			if bytes.HasPrefix(line, []byte("data:")) {
				if len(data) > 0 {
					data = append(data, '\n')
				}
				data = append(data, bytes.TrimPrefix(line[5:], []byte(" "))...)
			} else if bytes.HasPrefix(line, []byte("event:")) {
				eventName = strings.TrimSpace(string(line[6:]))
			}
		}
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		if err := w.emitEvent(data, eventName); err != nil {
			return err
		}
	}
}

func (w *openAIWSHTTPResponseWriter) finish() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if len(bytes.TrimSpace(w.buffer)) == 0 {
		return nil
	}
	if w.streaming {
		w.buffer = append(w.buffer, '\n', '\n')
		return w.drainSSE()
	}
	var response map[string]json.RawMessage
	if err := json.Unmarshal(w.buffer, &response); err != nil {
		return err
	}
	if w.status >= http.StatusBadRequest {
		if _, ok := response["error"]; !ok {
			var code, message string
			_ = json.Unmarshal(response["code"], &code)
			_ = json.Unmarshal(response["message"], &message)
			if message == "" {
				message = http.StatusText(w.status)
			}
			errorType := "invalid_request_error"
			if w.status == http.StatusTooManyRequests {
				errorType = "rate_limit_error"
			} else if w.status >= http.StatusInternalServerError {
				errorType = "server_error"
			}
			detail, _ := json.Marshal(map[string]string{"type": errorType, "code": code, "message": message})
			response = map[string]json.RawMessage{"error": detail}
		}
		response["type"] = json.RawMessage(`"error"`)
		response["status"], _ = json.Marshal(w.status)
		payload, err := json.Marshal(response)
		if err != nil {
			return err
		}
		return w.emitEvent(payload, "")
	}
	var object, status string
	_ = json.Unmarshal(response["object"], &object)
	_ = json.Unmarshal(response["status"], &status)
	if object != "response" || (status != "completed" && status != "failed" && status != "incomplete") {
		return errors.New("upstream HTTP response was not a complete Responses object")
	}
	payload, err := json.Marshal(map[string]any{"type": "response." + status, "response": json.RawMessage(w.buffer)})
	if err != nil {
		return err
	}
	return w.emitEvent(payload, "")
}
