package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// shouldBridgeOpenAIOAuthCompactV2 reports whether a documented downstream
// /responses/compact request must use ChatGPT's current streaming internal
// remote_compaction_v2 wire. Public API-key upstreams keep their native
// /responses/compact endpoint and payload unchanged.
func shouldBridgeOpenAIOAuthCompactV2(c *gin.Context, account *Account) bool {
	return account != nil && account.IsOpenAIOAuth() && isOpenAIResponsesCompactPath(c)
}

func recordOpenAIResponsesUpstreamEndpoint(c *gin.Context, remoteCompactV2 bool) {
	endpoint := openAIResponsesEndpoint
	if !remoteCompactV2 && isOpenAIResponsesCompactPath(c) {
		endpoint = openAIResponsesCompactEndpoint
	}
	SetActualOpenAIUpstreamEndpoint(c, endpoint)
}

// prepareOpenAIRemoteCompactV2Request converts the downstream compact request
// into the request shape emitted by current Codex clients. The caller keeps
// the original downstream stream intent: stream=true here describes only the
// ChatGPT upstream wire and is bridged back to the legacy response shape.
func prepareOpenAIRemoteCompactV2Request(reqBody map[string]any) (bool, error) {
	if reqBody == nil {
		return false, errors.New("remote compact v2 request body is nil")
	}

	changed := false
	model, _ := reqBody["model"].(string)
	model = strings.TrimSpace(model)

	instructions, instructionsIsString := reqBody["instructions"].(string)
	if raw, exists := reqBody["instructions"]; exists && raw != nil && !instructionsIsString {
		return false, errors.New("remote compact v2 instructions must be a string")
	}
	if strings.TrimSpace(instructions) == "" {
		reqBody["instructions"] = defaultCodexSynthInstructions(model)
		changed = true
	}

	input, inputChanged, err := normalizeOpenAIRemoteCompactV2Input(reqBody["input"])
	if err != nil {
		return false, err
	}
	if inputChanged {
		changed = true
	}
	reqBody["input"] = input

	if tools, exists := reqBody["tools"]; !exists || tools == nil {
		reqBody["tools"] = []any{}
		changed = true
	}
	if choice, ok := reqBody["tool_choice"].(string); !ok || strings.TrimSpace(choice) != "auto" {
		reqBody["tool_choice"] = "auto"
		changed = true
	}
	if _, ok := reqBody["parallel_tool_calls"].(bool); !ok {
		reqBody["parallel_tool_calls"] = false
		changed = true
	}
	if reasoning, exists := reqBody["reasoning"]; !exists || reasoning == nil {
		reqBody["reasoning"] = map[string]any{
			"effort":  "medium",
			"summary": "auto",
		}
		changed = true
	}
	if ensureOpenAIRequestStringListMember(reqBody, "include", "reasoning.encrypted_content") {
		changed = true
	}
	if store, ok := reqBody["store"].(bool); !ok || store {
		reqBody["store"] = false
		changed = true
	}
	if stream, ok := reqBody["stream"].(bool); !ok || !stream {
		reqBody["stream"] = true
		changed = true
	}

	return changed, nil
}

func normalizeOpenAIRemoteCompactV2Input(raw any) ([]any, bool, error) {
	var input []any
	changed := false
	switch value := raw.(type) {
	case nil:
		input = []any{}
		changed = true
	case []any:
		input = value
	case map[string]any:
		input = []any{value}
		changed = true
	case string:
		if strings.TrimSpace(value) == "" {
			input = []any{}
		} else {
			input = []any{map[string]any{
				"type":    "message",
				"role":    "user",
				"content": value,
			}}
		}
		changed = true
	default:
		return nil, false, fmt.Errorf("remote compact v2 input must be an array, object, or string")
	}

	// Current Codex appends exactly one trigger as the final prompt item. Make
	// the conversion idempotent and repair clients that supplied duplicates or
	// placed the trigger before additional history.
	normalized := make([]any, 0, len(input)+1)
	triggerCount := 0
	triggerWasLast := false
	for index, rawItem := range input {
		item, ok := rawItem.(map[string]any)
		if ok && strings.TrimSpace(firstNonEmptyString(item["type"])) == "compaction_trigger" {
			triggerCount++
			triggerWasLast = index == len(input)-1
			continue
		}
		normalized = append(normalized, rawItem)
	}
	normalized = append(normalized, map[string]any{"type": "compaction_trigger"})
	if triggerCount != 1 || !triggerWasLast {
		changed = true
	}
	return normalized, changed, nil
}

func ensureOpenAIRequestStringListMember(reqBody map[string]any, field, member string) bool {
	member = strings.TrimSpace(member)
	if reqBody == nil || member == "" {
		return false
	}
	switch existing := reqBody[field].(type) {
	case []any:
		for _, raw := range existing {
			if value, ok := raw.(string); ok && strings.EqualFold(strings.TrimSpace(value), member) {
				return false
			}
		}
		reqBody[field] = append(existing, member)
		return true
	case []string:
		for _, value := range existing {
			if strings.EqualFold(strings.TrimSpace(value), member) {
				return false
			}
		}
		reqBody[field] = append(existing, member)
		return true
	case nil:
		reqBody[field] = []any{member}
		return true
	default:
		// Legacy compact normalization never carries include. If an internal
		// caller supplied a malformed value, replace it with the required wire
		// shape instead of emitting a request that ChatGPT will reject.
		reqBody[field] = []any{member}
		return true
	}
}

func prepareOpenAIRemoteCompactV2Body(body []byte) ([]byte, bool, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return body, false, errors.New("remote compact v2 request body is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var reqBody map[string]any
	if err := decoder.Decode(&reqBody); err != nil {
		return body, false, fmt.Errorf("parse remote compact v2 request: %w", err)
	}
	changed, err := prepareOpenAIRemoteCompactV2Request(reqBody)
	if err != nil {
		return body, false, err
	}
	if !changed {
		return body, false, nil
	}
	prepared, err := marshalOpenAIUpstreamJSON(reqBody)
	if err != nil {
		return body, false, fmt.Errorf("serialize remote compact v2 request: %w", err)
	}
	return prepared, true, nil
}

func ensureOpenAICommaSeparatedHeaderToken(headers http.Header, name, token string) {
	if headers == nil {
		return
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}

	values := make([]string, 0, 4)
	seen := make(map[string]struct{})
	for key, headerValues := range headers {
		if !strings.EqualFold(strings.TrimSpace(key), name) {
			continue
		}
		delete(headers, key)
		for _, headerValue := range headerValues {
			for _, part := range strings.Split(headerValue, ",") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				key := strings.ToLower(part)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				values = append(values, part)
			}
		}
	}
	if _, exists := seen[strings.ToLower(token)]; !exists {
		values = append(values, token)
	}
	headers.Set(name, strings.Join(values, ", "))
}

func applyOpenAIRemoteCompactV2Headers(headers http.Header) {
	if headers == nil {
		return
	}
	stripOpenAILegacyResponsesBeta(headers)
	headers.Set("Accept", "text/event-stream")
	ensureOpenAICommaSeparatedHeaderToken(headers, "x-codex-beta-features", openAIRemoteCompactionV2Feature)
}

// validateOpenAIRemoteCompactV2Response verifies the protocol invariants used
// by current Codex: a completed stream and exactly one compaction output item.
// HTTP 200 alone is not enough to mark an account Compact-capable because
// protocol failures can also be delivered inside the SSE stream.
func validateOpenAIRemoteCompactV2Response(body []byte) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return errors.New("remote compact v2 returned an empty response")
	}

	completed := false
	compactionCount := 0
	outputItemCount := 0
	var streamFailure string
	forEachOpenAISSEDataPayload(string(body), func(data []byte) {
		eventType := strings.TrimSpace(gjson.GetBytes(data, "type").String())
		switch eventType {
		case "response.output_item.done":
			outputItemCount++
			if isResponsesCompactionItemType(gjson.GetBytes(data, "item.type").String()) {
				compactionCount++
			}
		case "response.completed", "response.done":
			completed = true
		case "response.failed":
			streamFailure = strings.TrimSpace(extractOpenAISSEErrorMessage(data))
			if streamFailure == "" {
				streamFailure = "remote compact v2 stream failed"
			}
		}
	})
	if streamFailure != "" {
		return errors.New(streamFailure)
	}
	if !completed {
		return errors.New("remote compact v2 stream closed before response.completed")
	}
	if compactionCount != 1 {
		return fmt.Errorf("remote compact v2 expected exactly one compaction output item, got %d from %d output items", compactionCount, outputItemCount)
	}
	return nil
}
