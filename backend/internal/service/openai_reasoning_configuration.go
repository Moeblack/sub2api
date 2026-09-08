package service

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

// Only explicit, visible updates are known. A response's reasoning.effort
// still echoes its top-level setting, and does not expose inherited updates.
// No update means callers retain their existing request-level estimate.
func openAIReasoningConfigurationEffort(body []byte) (string, bool) {
	var effort string
	found := false
	for _, item := range openAIReasoningInputItems(gjson.GetBytes(body, "input")) {
		if item.Get("type").String() == "configuration_update" {
			value := item.Get("reasoning.effort")
			if value.Type == gjson.String && strings.TrimSpace(value.String()) != "" {
				effort, found = strings.TrimSpace(value.String()), true
			}
		}
	}
	return effort, found
}

func openAIReasoningConfigurationEffortFromMap(body map[string]any) (string, bool) {
	input, _ := body["input"].([]any)
	if item, ok := body["input"].(map[string]any); ok {
		input = []any{item}
	}
	var effort string
	found := false
	for _, raw := range input {
		item, ok := raw.(map[string]any)
		if !ok || item["type"] != "configuration_update" {
			continue
		}
		reasoning, _ := item["reasoning"].(map[string]any)
		if value, ok := reasoning["effort"].(string); ok && strings.TrimSpace(value) != "" {
			effort, found = strings.TrimSpace(value), true
		}
	}
	return effort, found
}

// A policy may validate the full history, but must not rewrite it: doing so can
// alter already-generated reasoning and invalidate the original cached prefix.
// Reject conflicts even for a downgrade policy; ordinary top-level requests
// retain their established mapping/downgrade behavior.
func validateOpenAIReasoningConfigurationPolicy(body []byte, resolve func(string) (string, error)) error {
	root := gjson.ParseBytes(body)
	if err := rejectDuplicateReasoningPolicyMembers(root, "request", "model", "input", "reasoning", "reasoning_effort", "output_config"); err != nil {
		return err
	}
	for _, path := range []string{"reasoning", "output_config"} {
		if err := rejectDuplicateReasoningPolicyMembers(root.Get(path), path, "effort"); err != nil {
			return err
		}
	}
	var validationErr error
	for index, item := range openAIReasoningInputItems(root.Get("input")) {
		isUpdate := false
		item.ForEach(func(key, value gjson.Result) bool {
			if key.String() == "type" && value.String() == "configuration_update" {
				isUpdate = true
			}
			return true
		})
		if !isUpdate {
			continue
		}
		path := fmt.Sprintf("input.%d", index)
		if err := rejectDuplicateReasoningPolicyMembers(item, path, "type", "reasoning"); err != nil {
			validationErr = err
			break
		}
		if err := rejectDuplicateReasoningPolicyMembers(item.Get("reasoning"), path+".reasoning", "effort"); err != nil {
			validationErr = err
			break
		}
		field := item.Get("reasoning.effort")
		if !field.Exists() {
			continue
		}
		original := strings.TrimSpace(field.String())
		if field.Type != gjson.String || normalizeReasoningEffortMappingSource(original) == "" {
			validationErr = &ReasoningConfigurationPolicyError{Param: path + ".reasoning.effort", Message: "configuration_update effort cannot be validated against this group's policy"}
			break
		}
		effective, err := resolve(original)
		if err != nil {
			validationErr = err
			break
		}
		if normalizeReasoningEffortMappingSource(effective) != normalizeReasoningEffortMappingSource(original) {
			validationErr = &ReasoningConfigurationPolicyError{Param: path + ".reasoning.effort", Message: "configuration_update conflicts with this group's reasoning policy; historical updates cannot be rewritten"}
			break
		}
	}
	return validationErr
}

func openAIReasoningInputItems(input gjson.Result) []gjson.Result {
	if input.IsObject() {
		return []gjson.Result{input}
	}
	return input.Array()
}

func rejectDuplicateReasoningPolicyMembers(object gjson.Result, path string, names ...string) error {
	if !object.IsObject() {
		return nil
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		seen[name] = false
	}
	var err error
	object.ForEach(func(key, _ gjson.Result) bool {
		name := key.String()
		if duplicate, tracked := seen[name]; tracked {
			if duplicate {
				err = &ReasoningConfigurationPolicyError{Param: path + "." + name, Message: "duplicate JSON member prevents unambiguous reasoning policy validation"}
				return false
			}
			seen[name] = true
		}
		return true
	})
	return err
}
