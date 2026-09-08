package apicompat

import (
	"encoding/json"
	"fmt"
	"strings"
)

// validateResponsesNativeFeatures prevents conversion from silently changing
// native Responses execution semantics. Chat Completions and Anthropic cannot
// represent an outstanding asynchronous call or an in-history effort update.
func validateResponsesNativeFeatures(req *ResponsesRequest, target string) error {
	if req == nil {
		return fmt.Errorf("responses request is nil")
	}
	unsupported := func(feature string) error {
		return fmt.Errorf("%s requires a native OpenAI Responses upstream; cannot convert it to %s", feature, target)
	}
	if req.Reasoning != nil {
		if mode := strings.TrimSpace(req.Reasoning.Mode); mode != "" && mode != "standard" {
			return unsupported("reasoning.mode=" + mode)
		}
		if context := strings.TrimSpace(req.Reasoning.Context); context != "" && context != "auto" {
			return unsupported("reasoning.context=" + context)
		}
	}
	tools, err := EffectiveResponsesTools(req)
	if err != nil {
		return err
	}
	var validateTools func([]ResponsesTool) error
	validateTools = func(tools []ResponsesTool) error {
		for _, tool := range tools {
			if tool.Async != nil && *tool.Async {
				return unsupported("async tools")
			}
			if err := validateTools(tool.Tools); err != nil {
				return err
			}
			if err := validateTools(tool.Children); err != nil {
				return err
			}
		}
		return nil
	}
	if err := validateTools(tools); err != nil {
		return err
	}
	var input []struct {
		Type  string `json:"type"`
		Async bool   `json:"async"`
	}
	// The input may also be a string. For arrays, malformed async metadata must
	// not bypass the configuration/history checks for the entire conversation.
	if strings.HasPrefix(strings.TrimSpace(string(req.Input)), "[") {
		if err := json.Unmarshal(req.Input, &input); err != nil {
			return fmt.Errorf("parse responses execution metadata: %w", err)
		}
		for _, item := range input {
			if item.Type == "configuration_update" {
				return unsupported("configuration_update")
			}
			if item.Async && (item.Type == "function_call" || item.Type == "custom_tool_call") {
				return unsupported("async tool call history")
			}
		}
	}
	return nil
}
