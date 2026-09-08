package service

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func FuzzOpenAIStandaloneInputPreservation(f *testing.F) {
	f.Add([]byte(`{"input":[{"type":"web_search_call","id":"ws_1"},{"type":"function_call_output","namespace":"codex_app","name":"send_message_to_thread","output":"` + semanticDelegation + `"}]}`))
	f.Add([]byte(`{"input":[{"type":"function_call_output","namespace":"codex_app","name":"automation_update","output":"<heartbeat><automation_id>example</automation_id></heartbeat>"}],"metadata":{"n":9007199254740993}}`))
	f.Add([]byte(`{"input":[{"type":"function_call_output","name":"external","output":[{"type":"input_text","text":"native"}]}]}`))
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 65536 {
			t.Skip()
		}
		got, changed := NormalizeCodexStandaloneInputs(body)
		if !changed {
			if !bytes.Equal(body, got) {
				t.Fatal("unchanged result must retain original bytes")
			}
			return
		}
		decode := func(data []byte) map[string]any {
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			var result map[string]any
			if err := decoder.Decode(&result); err != nil {
				t.Fatal(err)
			}
			return result
		}
		before, after := decode(body), decode(got)
		left, ok := before["input"].([]any)
		if !ok {
			t.Fatalf("original input must be an array, got %T", before["input"])
		}
		right, ok := after["input"].([]any)
		if !ok {
			t.Fatalf("normalized input must be an array, got %T", after["input"])
		}
		if len(left) != len(right) {
			t.Fatal("normalization changed history length")
		}
		for index, original := range left {
			if reflect.DeepEqual(original, right[index]) {
				continue
			}
			item, ok := original.(map[string]any)
			if !ok || !isCodexDelegationCandidate(item) && !isCodexAutomationCandidate(item) {
				t.Fatal("unrelated history item changed")
			}
			if value, exists := item["call_id"]; exists {
				id, valid := value.(string)
				if !valid || len(bytes.TrimSpace([]byte(id))) != 0 {
					t.Fatal("paired output changed")
				}
			}
			expected := map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": item["output"]}}}
			if !reflect.DeepEqual(expected, right[index]) {
				t.Fatal("notification contents or authority changed")
			}
		}
		delete(before, "input")
		delete(after, "input")
		if !reflect.DeepEqual(before, after) {
			t.Fatal("request controls changed")
		}
		again, changed := NormalizeCodexStandaloneInputs(got)
		if changed || !bytes.Equal(got, again) {
			t.Fatal("normalization is not idempotent")
		}
	})
}
