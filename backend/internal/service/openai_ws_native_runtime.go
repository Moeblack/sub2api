package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

// Native connections belong to one downstream socket and one upstream account.
// The event loop is the sole owner of queues, response metadata and callbacks.
// In particular, it never reconnects or replays a write with an unknown outcome.
type openAIWSNativeTurn struct {
	number        int
	streamID      string
	payload       []byte
	requestModel  string
	upstreamModel string
	toolReverse   map[string]string
	imageCounter  *openAIImageOutputCounter
	startedAt     time.Time
	lastActivity  time.Time
	responseID    string
	completed     bool
	result        OpenAIForwardResult
	steerUnacked  int
	steers        map[string]struct{}
	failedSteers  map[string]struct{}
	steerPending  bool
	requiresInput bool
	warmup        bool
	sawOutput     bool
}

type openAIWSNativeOptions struct {
	prepare            func(turn int, payload []byte) (*openAIWSNativeTurn, error)
	beforeTurn         func(turn int) error
	afterTurn          func(turn int, result *OpenAIForwardResult, err error)
	continueTurn       func(parent, successor int) error
	releaseTurn        func(turn int)
	validatePrevious   func(responseID string) error
	validateSteer      func(turn *openAIWSNativeTurn, payload []byte) error
	observeUpstream    func(turn *openAIWSNativeTurn, payload []byte)
	writeTimeout       time.Duration
	idleTimeout        time.Duration
	activeReadTimeout  time.Duration
	firstOutputTimeout func(turn *openAIWSNativeTurn) time.Duration
	lifetime           time.Duration
}

type openAIWSNativeLane struct {
	active      *openAIWSNativeTurn
	reservation *openAIWSNativeTurn
	latest      *openAIWSNativeTurn
	queue       [][]byte
}

type openAIWSNativeFrame struct {
	messageType coderws.MessageType
	payload     []byte
	err         error
}

type openAIWSNativeRuntime struct {
	ctx         context.Context
	client      openaiwsv2.FrameConn
	upstream    openaiwsv2.FrameConn
	options     openAIWSNativeOptions
	lanes       map[string]*openAIWSNativeLane
	laneOrder   []string
	responses   map[string]*openAIWSNativeTurn
	nextTurn    int
	active      int
	queued      int
	queuedBytes int
	idleSince   time.Time
	clientGone  bool
	drainUntil  time.Time
	drainErr    error
}

func runOpenAIWSNative(ctx context.Context, client, upstream openaiwsv2.FrameConn, first *openAIWSNativeTurn, options openAIWSNativeOptions) (runErr error) {
	if ctx == nil || client == nil || upstream == nil || first == nil {
		return errors.New("invalid native websocket connection")
	}
	if options.writeTimeout <= 0 {
		options.writeTimeout = 30 * time.Second
	}
	if options.lifetime <= 0 || options.lifetime > time.Hour {
		options.lifetime = time.Hour
	}
	ctx, cancel := context.WithTimeout(ctx, options.lifetime)
	defer cancel()
	r := &openAIWSNativeRuntime{ctx: ctx, client: client, upstream: upstream, options: options,
		lanes: make(map[string]*openAIWSNativeLane), responses: make(map[string]*openAIWSNativeTurn), nextTurn: 1}
	first.number = 1
	if err := validateOpenAIWSNativeEnvelope(first.payload); err != nil {
		_ = r.writeError("", err)
		return err
	}
	if gjson.GetBytes(first.payload, "type").String() != "response.create" {
		err := newOpenAIWSRequestError(400, "invalid_request_error", "The first native WebSocket event must be response.create.", "type")
		_ = r.writeError("", err)
		return err
	}
	laneID, err := openAIWSStreamID(first.payload)
	if err != nil {
		_ = r.writeError("", err)
		return err
	}
	first.streamID = laneID
	first.warmup = openAIWSNativeIsWarmup(first.payload)
	r.lanes[laneID] = &openAIWSNativeLane{active: first}
	r.laneOrder = append(r.laneOrder, laneID)
	r.active = 1
	if first.startedAt.IsZero() {
		first.startedAt = time.Now()
	}
	first.lastActivity = first.startedAt
	first.imageCounter = newOpenAIImageOutputCounter()
	defer func() {
		// Unknown writes are never reissued. Release every surviving execution
		// and every unused steering reservation exactly once on connection loss.
		cleanupErr := runErr
		if cleanupErr == nil {
			cleanupErr = io.ErrUnexpectedEOF
		}
		for _, lane := range r.lanes {
			if lane.active != nil && !lane.active.completed && options.afterTurn != nil {
				turn := lane.active
				r.finishResult(turn)
				if turn.result.ImageCount > 0 || openAIUsageHasTokens(&turn.result.Usage) {
					options.afterTurn(turn.number, &turn.result, cleanupErr)
				} else {
					options.afterTurn(turn.number, nil, cleanupErr)
				}
			}
			if lane.reservation != nil && options.releaseTurn != nil {
				options.releaseTurn(lane.reservation.number)
			}
		}
	}()
	if err := r.write(upstream, coderws.MessageText, first.payload); err != nil {
		return fmt.Errorf("native websocket first write (outcome unknown): %w", err)
	}
	first.payload = nil // Native state lives upstream; never retain replay input.
	readFrames := func(conn openaiwsv2.FrameConn) <-chan openAIWSNativeFrame {
		frames := make(chan openAIWSNativeFrame, 1)
		go func() {
			for {
				kind, payload, err := conn.ReadFrame(ctx)
				select {
				case frames <- openAIWSNativeFrame{kind, payload, err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}()
		return frames
	}
	clientFrames, upstreamFrames := readFrames(client), readFrames(upstream)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if r.clientGone {
			clientFrames = nil
		}
		select {
		case frame := <-clientFrames:
			if frame.err != nil {
				if r.active == 0 {
					return nil
				}
				disconnectErr := frame.err
				if coderws.CloseStatus(frame.err) == coderws.StatusNormalClosure || errors.Is(frame.err, io.EOF) {
					disconnectErr = nil
				}
				r.beginDrain(disconnectErr)
				clientFrames = nil
				continue
			}
			if err := r.handleClient(frame); err != nil {
				return err
			}
		case frame := <-upstreamFrames:
			if frame.err != nil {
				if r.active == 0 && (errors.Is(frame.err, io.EOF) || coderws.CloseStatus(frame.err) == coderws.StatusNormalClosure) {
					return r.drainErr
				}
				return fmt.Errorf("native upstream disconnected; execution outcomes must be reconciled before retry: %w", frame.err)
			}
			if err := r.handleUpstream(frame); err != nil {
				return err
			}
		case now := <-ticker.C:
			if r.clientGone && (r.active == 0 || !now.Before(r.drainUntil)) {
				return r.drainErr
			}
			if err := r.checkTimeout(now); err != nil {
				return err
			}
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		if r.clientGone {
			if r.active == 0 {
				return r.drainErr
			}
			continue
		}
		if err := r.dispatch(); err != nil {
			return err
		}
	}
}

func (r *openAIWSNativeRuntime) write(conn openaiwsv2.FrameConn, kind coderws.MessageType, payload []byte) error {
	if conn == r.client && r.clientGone {
		return nil
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.options.writeTimeout)
	defer cancel()
	return conn.WriteFrame(ctx, kind, payload)
}

func (r *openAIWSNativeRuntime) beginDrain(err error) {
	if r.clientGone {
		return
	}
	r.clientGone = true
	if err != nil {
		// Keep the original transport cause while identifying this as a
		// downstream cancellation, never an account scheduling failure.
		r.drainErr = fmt.Errorf("downstream websocket closed: %w", errors.Join(context.Canceled, err))
	}
	r.drainUntil = time.Now().Add(1200 * time.Millisecond)
	for _, lane := range r.lanes {
		lane.queue = nil
		if lane.active != nil {
			lane.active.result.ClientDisconnect = true
		}
		if lane.reservation != nil {
			lane.reservation.result.ClientDisconnect = true
		}
	}
	r.queued, r.queuedBytes = 0, 0
}

func (r *openAIWSNativeRuntime) writeError(laneID string, err error) error {
	var requestErr *OpenAIWSRequestError
	if !errors.As(err, &requestErr) {
		requestErr = newOpenAIWSRequestError(400, "invalid_request_error", err.Error(), "")
	}
	return r.write(r.client, coderws.MessageText, openAIWSRequestErrorEvent(requestErr, laneID))
}

func (r *openAIWSNativeRuntime) handleClient(frame openAIWSNativeFrame) error {
	if (frame.messageType != coderws.MessageText && frame.messageType != coderws.MessageBinary) || !gjson.ValidBytes(frame.payload) || !gjson.ParseBytes(frame.payload).IsObject() {
		return r.writeError("", errors.New("native Responses WebSocket requires a JSON object"))
	}
	if err := validateOpenAIWSNativeEnvelope(frame.payload); err != nil {
		return r.writeError("", err)
	}
	switch gjson.GetBytes(frame.payload, "type").String() {
	case "response.create":
		laneID, err := openAIWSStreamID(frame.payload)
		if err != nil {
			return r.writeError("", err)
		}
		lane := r.lanes[laneID]
		if lane == nil {
			named := len(r.lanes)
			if _, exists := r.lanes[""]; exists {
				named--
			}
			if laneID != "" && named >= openAIWSMaxNamedStreams {
				return r.writeError(laneID, newOpenAIWSRequestError(400, "websocket_stream_limit_reached", "This connection has reached 32 named streams.", "stream_id"))
			}
			lane = &openAIWSNativeLane{}
			r.lanes[laneID] = lane
			r.laneOrder = append(r.laneOrder, laneID)
		}
		if r.queued >= openAIWSMaxQueuedRequests || r.queuedBytes+len(frame.payload) > openAIWSMaxQueuedBytes {
			return r.writeError(laneID, newOpenAIWSRequestError(429, "websocket_queue_limit_reached", "This connection's request queue is full; wait for an active response to finish.", ""))
		}
		lane.queue = append(lane.queue, append([]byte(nil), frame.payload...))
		r.queued++
		r.queuedBytes += len(frame.payload)
		return nil
	case "response.steer", "response.inject":
		return r.handleControl(frame.payload)
	default:
		return r.writeError("", newOpenAIWSRequestError(400, "unsupported_event", "Unsupported Responses WebSocket client event.", "type"))
	}
}

// Reject ambiguous envelope keys before resolving models, lanes or response
// owners. Nested input values remain raw: tools may legitimately carry JSON
// strings and opaque context whose representation must not be rewritten.
func validateOpenAIWSNativeEnvelope(payload []byte) error {
	if !gjson.ValidBytes(payload) || !gjson.ParseBytes(payload).IsObject() {
		return newOpenAIWSRequestError(400, "invalid_request_error", "Native Responses WebSocket requires a JSON object.", "")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if _, err := decoder.Token(); err != nil {
		return err
	}
	seen := make(map[string]bool)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return newOpenAIWSRequestError(400, "invalid_request_error", "Duplicate WebSocket event fields are not allowed.", key)
		}
		seen[key] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
	}
	return nil
}

func (r *openAIWSNativeRuntime) handleControl(payload []byte) error {
	kind := gjson.GetBytes(payload, "type").String()
	field := "previous_response_id"
	if kind == "response.inject" {
		invalid := false
		gjson.ParseBytes(payload).ForEach(func(key, value gjson.Result) bool {
			invalid = key.String() != "type" && key.String() != "response_id" && key.String() != "input"
			return !invalid
		})
		if invalid || !gjson.GetBytes(payload, "input").IsArray() {
			return r.controlFailure(payload, "invalid_input", "Injection accepts only type, response_id, and an input array.")
		}
		field = "response_id"
	}
	id := strings.TrimSpace(gjson.GetBytes(payload, field).String())
	turn := r.responses[id]
	if turn == nil {
		return r.controlFailure(payload, "response_not_found", "The target response is not available on this WebSocket connection.")
	}
	if gjson.GetBytes(payload, "stream_id").Exists() {
		return r.controlFailure(payload, "invalid_input", "Do not send stream_id; the target response determines the lane.")
	}
	if kind == "response.inject" {
		if turn.completed {
			return r.controlFailure(payload, "response_not_found", "Tool results can only be injected into an active response.")
		}
		if r.options.validateSteer != nil {
			if err := r.options.validateSteer(turn, payload); err != nil {
				return r.controlFailure(payload, "invalid_input", err.Error())
			}
		}
		return r.write(r.upstream, coderws.MessageText, payload)
	}
	invalidField := false
	gjson.ParseBytes(payload).ForEach(func(key, value gjson.Result) bool {
		invalidField = key.String() != "type" && key.String() != "previous_response_id" && key.String() != "input"
		return !invalidField
	})
	input := gjson.GetBytes(payload, "input")
	if invalidField || (input.Type != gjson.String && (!input.IsArray() || len(input.Array()) == 0)) {
		return r.controlFailure(payload, "invalid_input", "Steering accepts only type, previous_response_id, and nonempty user input.")
	}
	if turn.steerUnacked+len(turn.steers) >= 64 {
		return r.controlFailure(payload, "too_many_pending_steers", "Wait for queued steering to be committed before submitting more input.")
	}
	if r.options.validateSteer != nil {
		if err := r.options.validateSteer(turn, payload); err != nil {
			return r.controlFailure(payload, "invalid_input", err.Error())
		}
	}
	// A completed response can still accept steering. Reserve capacity before
	// sending it, since the server may create a successor immediately.
	if turn.completed && !turn.requiresInput && !turn.steerPending {
		lane := r.lanes[turn.streamID]
		if lane.active != nil {
			return r.controlFailure(payload, "response_not_found", "Use the current response ID to steer this lane.")
		}
		if lane.reservation == nil {
			if r.active >= openAIWSMaxActiveResponses {
				return r.controlFailure(payload, "too_many_pending_steers", "The connection is at its active response limit.")
			}
			successor := r.successor(turn)
			if r.options.continueTurn != nil {
				if err := r.options.continueTurn(turn.number, successor.number); err != nil {
					if r.options.releaseTurn != nil {
						r.options.releaseTurn(successor.number)
					}
					return r.controlFailure(payload, "successor_creation_failed", err.Error())
				}
			} else if r.options.beforeTurn != nil {
				if err := r.options.beforeTurn(successor.number); err != nil {
					if r.options.releaseTurn != nil {
						r.options.releaseTurn(successor.number)
					}
					return r.controlFailure(payload, "successor_creation_failed", err.Error())
				}
			}
			lane.reservation = successor
			r.active++
		}
	}
	turn.steerUnacked++
	return r.write(r.upstream, coderws.MessageText, payload)
}

func (r *openAIWSNativeRuntime) controlFailure(payload []byte, code, message string) error {
	kind := gjson.GetBytes(payload, "type").String()
	event := map[string]any{"type": kind + ".failed", "error": map[string]any{"type": "invalid_request_error", "code": code, "message": message}}
	var input any
	_ = json.Unmarshal([]byte(gjson.GetBytes(payload, "input").Raw), &input)
	if kind == "response.steer" {
		event["steer"] = map[string]any{"previous_response_id": gjson.GetBytes(payload, "previous_response_id").String(), "input": input}
	} else {
		event["response_id"] = gjson.GetBytes(payload, "response_id").String()
		event["input"] = input
	}
	targetField := "previous_response_id"
	if kind == "response.inject" {
		targetField = "response_id"
	}
	if turn := r.responses[gjson.GetBytes(payload, targetField).String()]; turn != nil && turn.streamID != "" {
		event["stream_id"] = turn.streamID
	}
	data, _ := json.Marshal(event)
	return r.write(r.client, coderws.MessageText, data)
}

func (r *openAIWSNativeRuntime) dispatch() error {
	for _, laneID := range r.laneOrder {
		lane := r.lanes[laneID]
		if lane.active != nil || len(lane.queue) == 0 {
			continue
		}
		payload := lane.queue[0]
		parentID := gjson.GetBytes(payload, "previous_response_id").String()
		// An accepted steer owns the next continuation. Only an explicit
		// same-parent tool-result request may consume that reservation.
		if lane.reservation != nil && (lane.latest == nil || parentID != lane.latest.responseID) {
			continue
		}
		if lane.reservation != nil && !lane.latest.requiresInput && !lane.latest.steerPending {
			continue
		}
		if lane.reservation == nil && r.active >= openAIWSMaxActiveResponses {
			continue
		}
		lane.queue = lane.queue[1:]
		r.queued--
		r.queuedBytes -= len(payload)
		if parentID != "" && r.responses[parentID] == nil {
			ownerErr := error(newOpenAIWSRequestError(400, "previous_response_not_found", "Previous response is not available on this connection.", "previous_response_id"))
			if r.options.validatePrevious != nil {
				ownerErr = r.options.validatePrevious(parentID)
			}
			if ownerErr != nil {
				if err := r.writeError(laneID, ownerErr); err != nil {
					return err
				}
				continue
			}
		}
		r.nextTurn++
		turnNo := r.nextTurn
		if lane.reservation != nil {
			turnNo = lane.reservation.number
		}
		turn, err := r.options.prepare(turnNo, payload)
		if err != nil {
			if r.options.releaseTurn != nil {
				r.options.releaseTurn(turnNo)
			}
			if lane.reservation != nil {
				lane.reservation = nil
				r.active--
			}
			if err := r.writeError(laneID, err); err != nil {
				return err
			}
			continue
		}
		if r.options.beforeTurn != nil {
			if err := r.options.beforeTurn(turnNo); err != nil {
				if r.options.releaseTurn != nil {
					r.options.releaseTurn(turnNo)
				}
				if lane.reservation != nil {
					lane.reservation = nil
					r.active--
				}
				if err := r.writeError(laneID, err); err != nil {
					return err
				}
				continue
			}
		}
		turn.number, turn.streamID = turnNo, laneID
		turn.warmup = openAIWSNativeIsWarmup(turn.payload)
		turn.startedAt, turn.lastActivity = time.Now(), time.Now()
		turn.imageCounter = newOpenAIImageOutputCounter()
		if lane.reservation == nil {
			r.active++
		}
		lane.reservation = nil
		lane.active = turn
		if err := r.write(r.upstream, coderws.MessageText, turn.payload); err != nil {
			return fmt.Errorf("native websocket write (outcome unknown): %w", err)
		}
		turn.payload = nil
	}
	return nil
}

func (r *openAIWSNativeRuntime) successor(parent *openAIWSNativeTurn) *openAIWSNativeTurn {
	r.nextTurn++
	result := OpenAIForwardResult{Model: parent.requestModel, UpstreamModel: openAIWSDifferentModel(parent.requestModel, parent.upstreamModel),
		Stream: true, OpenAIWSMode: true, ServiceTier: parent.result.ServiceTier, ReasoningEffort: parent.result.ReasoningEffort,
		RequestedReasoningEffort: parent.result.RequestedReasoningEffort, ResponseHeaders: parent.result.ResponseHeaders,
		UpstreamHeaders: parent.result.UpstreamHeaders, BillingModel: parent.result.BillingModel,
		ImageSize: parent.result.ImageSize, ImageInputSize: parent.result.ImageInputSize, ClientDisconnect: r.clientGone}
	return &openAIWSNativeTurn{number: r.nextTurn, streamID: parent.streamID, requestModel: parent.requestModel,
		upstreamModel: parent.upstreamModel, toolReverse: parent.toolReverse, imageCounter: newOpenAIImageOutputCounter(),
		startedAt: time.Now(), lastActivity: time.Now(), result: result, warmup: parent.warmup}
}

func (r *openAIWSNativeRuntime) handleUpstream(frame openAIWSNativeFrame) error {
	if frame.messageType != coderws.MessageText || !gjson.ValidBytes(frame.payload) {
		return r.write(r.client, frame.messageType, frame.payload)
	}
	payload := frame.payload
	kind := gjson.GetBytes(payload, "type").String()
	if strings.HasPrefix(kind, "response.steer.") {
		r.observeSteer(payload)
		return r.write(r.client, frame.messageType, payload)
	}
	id := gjson.GetBytes(payload, "response.id").String()
	if id == "" {
		id = gjson.GetBytes(payload, "response_id").String()
	}
	if id == "" && openAIWSPassthroughIsTerminalOutput(payload) {
		return errors.New("upstream native terminal is missing its response ID")
	}
	laneID := gjson.GetBytes(payload, "stream_id").String()
	lane := r.lanes[laneID]
	turn := r.responses[id]
	if turn != nil {
		if laneID != turn.streamID {
			return errors.New("upstream native response lane does not match its owner")
		}
		lane = r.lanes[turn.streamID]
	}
	if kind == "response.created" && turn == nil {
		if lane == nil {
			return errors.New("upstream native response has no admitted lane")
		}
		if lane.active == nil && lane.reservation != nil {
			lane.active, lane.reservation = lane.reservation, nil
		}
		turn = lane.active
		if turn == nil || turn.responseID != "" || id == "" {
			return errors.New("upstream native response has no admitted execution")
		}
		turn.responseID = id
		r.responses[id] = turn
		if lane.latest != nil && lane.latest != turn {
			delete(r.responses, lane.latest.responseID)
		}
		lane.latest = turn
	}
	// Unknown/evicted response IDs must not settle a different current
	// execution; a delayed duplicate parent terminal is a common example.
	if turn == nil && lane != nil && id == "" {
		turn = lane.active
	}
	if turn != nil && !turn.completed && r.options.observeUpstream != nil {
		r.options.observeUpstream(turn, frame.payload)
	}
	if turn != nil && !turn.completed {
		turn.lastActivity = time.Now()
		if turn.imageCounter == nil {
			turn.imageCounter = newOpenAIImageOutputCounter()
		}
		turn.imageCounter.AddSSEData(payload)
		if openAIWSPassthroughStartsSemanticOutput(payload) && !openAIWSPassthroughIsTerminalOutput(payload) {
			turn.sawOutput = true
		}
		if len(gjson.GetBytes(payload, "response.output").Array()) > 0 {
			turn.sawOutput = true
		}
		(*OpenAIGatewayService)(nil).parseSSEUsageBytesWithType(payload, kind, &turn.result.Usage)
		if model := gjson.GetBytes(payload, "response.model").String(); model != "" {
			if turn.result.UpstreamResponseModel != "" && turn.result.UpstreamResponseModel != model {
				turn.result.UpstreamResponseModelConflict = true
			}
			turn.result.UpstreamResponseModel = model
		}
		if turn.result.FirstTokenMs == nil && openAIWSPassthroughStartsSemanticOutput(payload) && !openAIWSPassthroughIsTerminalOutput(payload) {
			elapsed := int(time.Since(turn.startedAt).Milliseconds())
			turn.result.FirstTokenMs = &elapsed
		}
	}
	if turn != nil {
		payload = replaceOpenAIWSMessageModel(payload, turn.upstreamModel, turn.requestModel)
		payload = restoreCodexToolNamesInJSON(payload, turn.toolReverse)
	}
	if normalized, changed := normalizeCompletedImageGenerationStatus(payload); changed {
		payload = normalized
	}
	writeErr := r.write(r.client, frame.messageType, payload)
	terminal := openAIWSPassthroughIsTerminalOutput(frame.payload)
	// A lane-scoped error before response.created rejects that admitted create.
	// Errors belonging to steer/inject are auxiliary and must never bill/finish
	// the active response. Once an ID exists, wait for its actual terminal event.
	if kind == "error" && turn != nil && turn.responseID == "" {
		terminal = true
	}
	if terminal && turn != nil && !turn.completed {
		turn.completed = true
		turn.result.ClientDisconnect = r.clientGone || writeErr != nil
		// A required client tool result may arrive before steer.pending. Only
		// that explicit continuation can replace an automatic reservation;
		// ordinary queued user creates must wait for the actual successor.
		for _, item := range gjson.GetBytes(frame.payload, "response.output").Array() {
			switch item.Get("type").String() {
			case "function_call", "custom_tool_call", "computer_call", "local_shell_call", "mcp_approval_request":
				if !item.Get("async").Bool() {
					turn.requiresInput = true
				}
			}
		}
		r.finishResult(turn)
		turn.result.UpstreamTerminalEvent = normalizeOpenAIWSTerminalEvent(kind)
		turn.result.UpstreamTerminalReason = gjson.GetBytes(frame.payload, "response.incomplete_details.reason").String()
		turn.result.UpstreamResponseServiceTier = normalizeObservedOpenAIServiceTier(gjson.GetBytes(frame.payload, "response.service_tier").String())
		lane.active = nil
		if turn.responseID != "" {
			lane.latest = turn
		}
		r.active--
		var transferErr error
		if turn.steerUnacked+len(turn.steers) > 0 || gjson.GetBytes(frame.payload, "response.incomplete_details.reason").String() == "steered" {
			successor := r.successor(turn)
			if r.options.continueTurn != nil {
				transferErr = r.options.continueTurn(turn.number, successor.number)
			} else if r.options.beforeTurn != nil {
				transferErr = r.options.beforeTurn(successor.number)
			}
			if transferErr == nil {
				lane.reservation = successor
				r.active++
			}
		}
		if r.options.afterTurn != nil {
			// The upstream terminal is authoritative even if the client write
			// failed: those tokens were consumed and must be recorded once.
			if turn.warmup && !turn.sawOutput && turn.result.ImageCount == 0 && !openAIUsageHasTokens(&turn.result.Usage) {
				// Warmup prepares server state without inference. A zero-token
				// usage record would still charge per-request channels, so it
				// only releases admission and does not create a bill.
				r.options.afterTurn(turn.number, nil, nil)
			} else {
				r.options.afterTurn(turn.number, &turn.result, nil)
			}
		}
		if transferErr != nil {
			return transferErr
		}
	}
	if writeErr != nil {
		r.beginDrain(writeErr)
	}
	return nil
}

func openAIWSNativeIsWarmup(payload []byte) bool {
	generate := gjson.GetBytes(payload, "generate")
	return generate.Exists() && generate.Type == gjson.False
}

func (r *openAIWSNativeRuntime) observeSteer(payload []byte) {
	parent := r.responses[gjson.GetBytes(payload, "steer.previous_response_id").String()]
	if parent == nil {
		return
	}
	id := gjson.GetBytes(payload, "steer.id").String()
	switch gjson.GetBytes(payload, "type").String() {
	case "response.steer.accepted":
		if parent.steers == nil {
			parent.steers = make(map[string]struct{})
		}
		if _, exists := parent.steers[id]; !exists {
			if parent.steerUnacked > 0 {
				parent.steerUnacked--
			}
			parent.steers[id] = struct{}{}
		}
	case "response.steer.failed":
		if id != "" {
			if _, duplicate := parent.failedSteers[id]; duplicate {
				return
			}
			if parent.failedSteers == nil || len(parent.failedSteers) >= 256 {
				parent.failedSteers = make(map[string]struct{})
			}
			parent.failedSteers[id] = struct{}{}
		}
		if _, accepted := parent.steers[id]; id != "" && accepted {
			delete(parent.steers, id)
		} else if parent.steerUnacked > 0 {
			parent.steerUnacked--
		}
	case "response.steer.pending":
		parent.steerPending = true
	}
	if parent.completed && (parent.steerPending || parent.steerUnacked+len(parent.steers) == 0) {
		lane := r.lanes[parent.streamID]
		if lane.reservation != nil {
			if r.options.releaseTurn != nil {
				r.options.releaseTurn(lane.reservation.number)
			}
			lane.reservation = nil
			r.active--
		}
	}
}

func (r *openAIWSNativeRuntime) finishResult(turn *openAIWSNativeTurn) {
	turn.result.RequestID = turn.responseID
	turn.result.ResponseID = turn.responseID
	turn.result.Duration = time.Since(turn.startedAt)
	turn.result.ImageCount = turn.imageCounter.Count()
	turn.result.ImageOutputSizes = turn.imageCounter.Sizes()
	ApplyOpenAIImageBillingResolution(&turn.result)
}

func (r *openAIWSNativeRuntime) checkTimeout(now time.Time) error {
	if r.active == 0 {
		if r.idleSince.IsZero() {
			r.idleSince = now
		}
		if r.options.idleTimeout > 0 && now.Sub(r.idleSince) >= r.options.idleTimeout {
			return NewOpenAIWSClientCloseError(coderws.StatusNormalClosure, "websocket idle timeout", nil)
		}
		return nil
	}
	r.idleSince = time.Time{}
	for _, lane := range r.lanes {
		turn := lane.active
		if turn == nil {
			turn = lane.reservation
		}
		if turn == nil {
			continue
		}
		if turn.result.FirstTokenMs == nil && r.options.firstOutputTimeout != nil {
			if timeout := r.options.firstOutputTimeout(turn); timeout > 0 && now.Sub(turn.startedAt) >= timeout {
				return NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "native upstream produced no output; reconcile responses before retry", errOpenAIWSPassthroughFirstOutputTimeout)
			}
		} else if r.options.activeReadTimeout > 0 && now.Sub(turn.lastActivity) >= r.options.activeReadTimeout {
			return NewOpenAIWSClientCloseError(coderws.StatusGoingAway, "native upstream response timed out; reconcile responses before retry", errOpenAIWSPassthroughActiveTurnTimeout)
		}
	}
	return nil
}
